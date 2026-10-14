package agentgrid.control;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * End-to-end verifier for Exp 9: MPI-style Broadcast, Scatter and Gather between the node processes.
 * Drives the control plane's HTTP API only (127.0.0.1:8080, a fresh control plane with the default 5
 * members). Prints raw numbers for every step and a PASS/FAIL table; exits non-zero if a check fails.
 * C7 kills and restarts a node; C8 adds a node and removes another, so it runs last and leaves a
 * 5-member cluster whose ids are not 1-5 (like MembershipVerifier).
 *
 * Usage (from backend/): java -cp build/classes agentgrid.control.CollectiveVerifier
 */
public class CollectiveVerifier {

    private static final String BASE = System.getProperty("agentgrid.verifier.base", "http://127.0.0.1:8080");
    private static final long STABLE_TIMEOUT_MS = 30000;
    private static final int MAX_INTS = 1000;
    private static final int MAX_MESSAGE_CHARS = 1024;

    private record Resp(int status, String body, long ms) {
        Map<String, Object> json() {
            return JsonUtil.parseObject(body);
        }
    }

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final Map<String, Boolean> passed = new LinkedHashMap<>();
    private final Map<String, String> results = new LinkedHashMap<>();
    private final Random random = new Random();
    private Map<String, Object> linear;
    private Map<String, Object> tree;

    public static void main(String[] args) throws Exception {
        boolean ok = new CollectiveVerifier().run();
        System.exit(ok ? 0 : 1);
    }

    private boolean run() throws Exception {
        line("=====================================================================");
        line(" AgentGrid-Lite Exp 9 collective verifier (HTTP API only) -> " + BASE);
        line("=====================================================================");
        ensureCluster();

        runCheck("C1", this::c1);
        runCheck("C2", this::c2);
        runCheck("C3", this::c3);
        runCheck("C4", this::c4);
        runCheck("C5", this::c5);
        runCheck("C6", this::c6);
        runCheck("C7", this::c7);
        runCheck("C9", this::c9);   // before C8, which changes the membership
        runCheck("C8", this::c8);

        line("");
        line("=====================================================================");
        line(" RESULT");
        line("=====================================================================");
        boolean all = true;
        for (Map.Entry<String, Boolean> e : passed.entrySet()) {
            System.out.printf("%-4s  %-44s %s%n", e.getValue() ? "PASS" : "FAIL", e.getKey(), results.get(e.getKey()));
            all &= e.getValue();
        }
        line("OVERALL: " + (all ? "PASS" : "FAIL"));
        return all;
    }

    private interface Check {
        void run() throws Exception;
    }

    /** A check that throws is a FAIL with the exception, and the next check still runs. */
    private void runCheck(String id, Check check) {
        try {
            check.run();
        } catch (Exception e) {
            check(id + " threw", false, e.toString());
        }
    }

    // =========================================================================
    // C1: broadcast, identical bytes everywhere
    // =========================================================================

    private void c1() throws Exception {
        header("C1 broadcast: every rank holds identical bytes (SHA-256), LINEAR and TREE");
        // Multi-byte UTF-8 on purpose: the bytes, not just the characters, must survive every hop.
        String message = "MPI_Bcast check " + System.nanoTime() + ": héllo wörld ✓ 漢字";
        String sha = sha256(message);
        int n = view().get("size") instanceof Number s ? s.intValue() : -1;
        line("message " + message.getBytes(StandardCharsets.UTF_8).length + " bytes (" + message.length()
                + " chars), sha256 " + sha);
        long seqBefore = lastSeq();
        for (String algorithm : List.of("LINEAR", "TREE")) {
            Map<String, Object> r = post("/api/collective/broadcast", Map.of("message", message, "algorithm", algorithm)).json();
            if (algorithm.equals("LINEAR")) {
                linear = r;
            } else {
                tree = r;
            }
            int same = 0;
            List<Map<String, Object>> ranks = maps(r.get("ranks"));
            for (Map<String, Object> x : ranks) {
                boolean ok = sha.equals(x.get("sha256")) && message.equals(x.get("received"));
                same += ok ? 1 : 0;
                line(String.format("  %-6s rank %s node %s: %s bytes, sha256 %s %s", algorithm, x.get("rank"), x.get("node"),
                        x.get("bytes"), x.get("sha256"), ok ? "identical" : "DIFFERENT"));
            }
            check("C1 broadcast " + algorithm + " identical bytes", same == n && ranks.size() == n,
                    same + "/" + n + " ranks hold the root's exact bytes (SHA-256 " + sha.substring(0, 16) + "...), "
                            + r.get("messages") + " messages, " + r.get("timeMs") + " ms");
        }
        // The operation is also an SSE event, stamped with the control plane's Lamport clock.
        Map<String, Object> event = null;
        for (Map<String, Object> e : listOf(get("/api/events?since=" + seqBefore))) {
            Object f = e.get("fields");
            if ("COLLECTIVE_BCAST".equals(e.get("type")) && f instanceof Map<?, ?> m && tree.get("opId").equals(m.get("opId"))) {
                event = e;
            }
        }
        check("C1 COLLECTIVE_BCAST event with Lamport time", event != null && num(event.get("lamport")) == num(tree.get("lamport")),
                event == null ? "no COLLECTIVE_BCAST event for op " + tree.get("opId")
                        : "seq " + event.get("seq") + ", Lamport " + event.get("lamport") + ": " + event.get("details"));
    }

    // =========================================================================
    // C2: TREE rounds vs LINEAR rounds
    // =========================================================================

    private void c2() throws Exception {
        header("C2 TREE takes ceil(log2 n) rounds, LINEAR n - 1, with the same result");
        int n = num(tree.get("size"));
        int log2 = 32 - Integer.numberOfLeadingZeros(n - 1);
        printTrace("LINEAR", linear);
        printTrace("TREE", tree);
        long forwardedTree = 0;
        long forwardedLinear = 0;
        for (Map<String, Object> t : maps(tree.get("trace"))) {
            forwardedTree += num(t.get("from")) != num(tree.get("root")) ? 1 : 0;
        }
        for (Map<String, Object> t : maps(linear.get("trace"))) {
            forwardedLinear += num(t.get("from")) != num(linear.get("root")) ? 1 : 0;
        }
        line("LINEAR: " + linear.get("messages") + " messages, " + linear.get("rounds") + " rounds, " + linear.get("timeMs")
                + " ms, " + forwardedLinear + " sent by non-root ranks");
        line("TREE:   " + tree.get("messages") + " messages, " + tree.get("rounds") + " rounds, " + tree.get("timeMs")
                + " ms, " + forwardedTree + " forwarded by non-root ranks");
        boolean ok = num(linear.get("rounds")) == n - 1 && num(tree.get("rounds")) == log2
                && (n < 4 || num(tree.get("rounds")) < num(linear.get("rounds")))
                && num(linear.get("messages")) == n - 1 && num(tree.get("messages")) == n - 1
                && forwardedLinear == 0 && forwardedTree > 0
                && tree.get("sha256").equals(linear.get("sha256"))
                && Boolean.TRUE.equals(tree.get("identical")) && Boolean.TRUE.equals(linear.get("identical"));
        check("C2 TREE fewer rounds, same result", ok, "n=" + n + ": TREE " + tree.get("rounds") + " rounds (ceil(log2 n) = " + log2
                + "), LINEAR " + linear.get("rounds") + " (n - 1 = " + (n - 1) + "); both " + (n - 1)
                + " messages, " + forwardedTree + " TREE messages forwarded by non-root ranks; same SHA-256 at every rank");
    }

    // =========================================================================
    // C3: scatter
    // =========================================================================

    private List<Integer> scattered;

    private void c3() throws Exception {
        header("C3 scatter: rank i holds exactly chunk i; chunks are disjoint and cover the array");
        int n = num(view().get("size"));
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 4 * n; i++) {
            data.add(1000 + i * 7);   // distinct values, so overlapping chunks would show
        }
        Collections.shuffle(data, random);
        Map<String, Object> r = post("/api/collective/scatter", Map.of("data", data)).json();
        int c = num(r.get("chunkSize"));
        boolean exact = true;
        List<Integer> union = new ArrayList<>();
        for (Map<String, Object> x : maps(r.get("ranks"))) {
            int rank = num(x.get("rank"));
            List<Integer> got = ints(x.get("chunk"));
            boolean ok = got.equals(data.subList(rank * c, (rank + 1) * c));
            exact &= ok;
            union.addAll(got);
            line("  rank " + rank + " node " + x.get("node") + ": " + got + (ok ? "" : "  expected " + data.subList(rank * c, (rank + 1) * c)));
        }
        List<Integer> sortedUnion = new ArrayList<>(union);
        List<Integer> sortedData = new ArrayList<>(data);
        Collections.sort(sortedUnion);
        Collections.sort(sortedData);
        boolean disjointCover = union.size() == data.size() && sortedUnion.equals(sortedData);
        check("C3 scatter chunk i at rank i", exact && union.equals(data), n + " ranks x " + c + " ints; every chunk equals data[i*"
                + c + ", (i+1)*" + c + "); " + r.get("messages") + " messages, " + r.get("rounds") + " rounds");
        check("C3 chunks disjoint and cover", disjointCover, union.size() + " ints held in total = " + data.size()
                + " scattered, each value exactly once");
        scattered = data;
    }

    // =========================================================================
    // C4: gather in rank order despite random delays
    // =========================================================================

    private void c4() throws Exception {
        header("C4 gather: the root's buffer is in rank order even with random delays (5 runs)");
        int outOfOrder = 0;
        int correct = 0;
        for (int run = 1; run <= 5; run++) {
            Map<String, Object> r = post("/api/collective/gather", Map.of("randomDelay", true)).json();
            List<Integer> delays = new ArrayList<>();
            for (Map<String, Object> x : maps(r.get("ranks"))) {
                delays.add(num(x.get("delayMs")));
            }
            boolean inOrder = ints(r.get("result")).equals(scattered);
            correct += inOrder ? 1 : 0;
            outOfOrder += Boolean.TRUE.equals(r.get("outOfOrder")) ? 1 : 0;
            line("  run " + run + ": delays by rank " + delays + " ms, arrival order " + r.get("arrivalOrder")
                    + ", buffer order " + r.get("finalOrder") + ", buffer == scattered data: " + inOrder);
        }
        check("C4 gather in rank order", correct == 5, correct + "/5 runs put chunk i at position i (buffer == the C3 data)");
        check("C4 arrival order differed", outOfOrder >= 1, "chunks arrived out of rank order in " + outOfOrder + "/5 runs");
    }

    // =========================================================================
    // C5: gather(scatter(x)) == x
    // =========================================================================

    private void c5() throws Exception {
        header("C5 gather(scatter(x)) == x for several sizes");
        int n = num(view().get("size"));
        List<String> sizes = new ArrayList<>();
        boolean all = true;
        for (int k : new int[] {1, 2, 7, MAX_INTS / n}) {
            List<Integer> x = new ArrayList<>();
            for (int i = 0; i < k * n; i++) {
                x.add(random.nextInt());
            }
            x.set(0, Integer.MIN_VALUE);   // the extremes must survive the int32 encoding
            x.set(x.size() - 1, Integer.MAX_VALUE);
            post("/api/collective/scatter", Map.of("data", x));
            Map<String, Object> g = post("/api/collective/gather", Map.of("randomDelay", k == 7)).json();
            boolean same = ints(g.get("result")).equals(x);
            all &= same;
            sizes.add(x.size() + (same ? " ok" : " DIFFERENT"));
            line("  " + x.size() + " ints (" + k + " per rank" + (k == 7 ? ", random delays" : "") + "): gather(scatter(x)) == x: "
                    + same + "; arrival order " + g.get("arrivalOrder"));
        }
        check("C5 gather(scatter(x)) == x", all, "sizes " + sizes);
    }

    // =========================================================================
    // C6: refusals
    // =========================================================================

    private void c6() throws Exception {
        header("C6 refused: a non-divisible scatter (400, no padding) and oversized input (400)");
        int n = num(view().get("size"));
        List<Integer> before = ints(post("/api/collective/gather", Map.of()).json().get("result"));

        Resp odd = post("/api/collective/scatter", Map.of("data", range(4 * n + 3)));
        line("  scatter of " + (4 * n + 3) + " ints over " + n + " ranks -> " + odd.status() + " " + odd.body());
        List<Integer> after = ints(post("/api/collective/gather", Map.of()).json().get("result"));
        line("  gather after the refusal returns the previous data unchanged: " + after.equals(before));
        check("C6 non-divisible scatter refused", odd.status() == 400 && odd.body().contains("remainder of " + ((4 * n + 3) % n))
                && after.equals(before), "HTTP " + odd.status() + " naming the remainder " + ((4 * n + 3) % n)
                + "; the ranks still hold the previous chunks (nothing scattered, nothing padded)");

        int over = (MAX_INTS / n + 1) * n;   // divisible, so only the size limit applies
        Resp bigData = post("/api/collective/scatter", Map.of("data", range(over)));
        Resp maxData = post("/api/collective/scatter", Map.of("data", range(MAX_INTS / n * n)));
        Resp bigMsg = post("/api/collective/broadcast", Map.of("message", "x".repeat(MAX_MESSAGE_CHARS + 1)));
        Resp maxMsg = post("/api/collective/broadcast", Map.of("message", "x".repeat(MAX_MESSAGE_CHARS)));
        Resp empty = post("/api/collective/broadcast", Map.of("message", ""));
        Resp badRoot = post("/api/collective/broadcast", Map.of("message", "x", "root", n));
        line("  scatter of " + over + " ints -> " + bigData.status() + " " + bigData.body());
        line("  scatter of " + (MAX_INTS / n * n) + " ints -> " + maxData.status());
        line("  broadcast of " + (MAX_MESSAGE_CHARS + 1) + " chars -> " + bigMsg.status() + " " + bigMsg.body());
        line("  broadcast of " + MAX_MESSAGE_CHARS + " chars -> " + maxMsg.status());
        line("  broadcast of 0 chars -> " + empty.status() + "; root " + n + " -> " + badRoot.status() + " " + badRoot.body());
        check("C6 oversized input refused", bigData.status() == 400 && bigMsg.status() == 400 && empty.status() == 400
                && badRoot.status() == 400 && maxData.status() == 200 && maxMsg.status() == 200,
                over + " ints -> " + bigData.status() + ", " + (MAX_MESSAGE_CHARS + 1) + " chars -> " + bigMsg.status()
                        + " (limits " + MAX_INTS + " ints / " + MAX_MESSAGE_CHARS + " chars still accepted: "
                        + maxData.status() + " / " + maxMsg.status() + "); empty message and out-of-range root -> 400");
    }

    // =========================================================================
    // C7: a dead rank
    // =========================================================================

    private void c7() throws Exception {
        header("C7 a killed node fails the collective within the timeout, naming its rank; after a restart it works");
        Map<String, Object> v = view();
        long timeoutMs = num(v.get("timeoutMs"));
        Map<String, Object> victim = null;
        for (Map<String, Object> r : maps(v.get("ranks"))) {
            if (victim == null && !r.get("node").equals(v.get("leader"))) {
                victim = r;
            }
        }
        String who = "rank " + victim.get("rank") + " (node " + victim.get("node") + ")";
        line("victim: " + who + ", not the leader; timeout " + timeoutMs + " ms");
        post("/api/nodes/" + victim.get("node") + "/kill", Map.of());
        List<String> failures = new ArrayList<>();
        boolean allFailed = true;
        for (String[] op : new String[][] {{"broadcast", "{\"message\":\"anyone there?\",\"algorithm\":\"TREE\"}"},
                {"scatter", JsonUtil.toJson(Map.of("data", range(4 * num(v.get("size")))))}, {"gather", "{}"}}) {
            Resp r = postRaw("/api/collective/" + op[0], op[1]);
            line("  " + op[0] + " -> HTTP " + r.status() + " after " + r.ms() + " ms: " + r.body());
            allFailed &= r.status() == 503 && r.body().contains(who + " did not respond") && r.ms() < timeoutMs;
            failures.add(op[0] + " " + r.status() + " in " + r.ms() + " ms");
        }
        check("C7 dead rank named within the timeout", allFailed, failures + ", each naming " + who + " (timeout " + timeoutMs + " ms)");

        post("/api/nodes/" + victim.get("node") + "/restart", Map.of());
        boolean up = waitUp();
        Resp b = postRaw("/api/collective/broadcast", "{\"message\":\"back again\",\"algorithm\":\"LINEAR\"}");
        Resp g409 = postRaw("/api/collective/gather", "{}");   // the restarted process lost its chunk
        Resp s = postRaw("/api/collective/scatter", JsonUtil.toJson(Map.of("data", range(4 * num(v.get("size"))))));
        Resp g = postRaw("/api/collective/gather", "{}");
        line("  after restart (all up: " + up + "): broadcast -> " + b.status() + ", gather before scatter -> " + g409.status()
                + " " + g409.body() + ", scatter -> " + s.status() + ", gather -> " + g.status());
        boolean works = up && b.status() == 200 && Boolean.TRUE.equals(b.json().get("identical")) && s.status() == 200
                && g.status() == 200 && ints(g.json().get("result")).equals(range(4 * num(v.get("size"))))
                && g409.status() == 409 && g409.body().contains(who);
        check("C7 works again after the restart", works, "broadcast " + b.status() + ", scatter " + s.status() + ", gather "
                + g.status() + "; before the new scatter, gather is refused (409) because " + who + " lost its chunk");
    }

    // =========================================================================
    // C9: a root that is not the leader
    // =========================================================================

    private void c9() throws Exception {
        header("C9 a root other than the leader's rank");
        Map<String, Object> v = view();
        int n = num(v.get("size"));
        int root = (num(v.get("root")) + 2) % n;
        line("leader's rank " + v.get("root") + " (node " + v.get("leader") + "); chosen root rank " + root);
        boolean ok = true;
        List<String> what = new ArrayList<>();
        for (String algorithm : List.of("LINEAR", "TREE")) {
            Map<String, Object> b = post("/api/collective/broadcast", Map.of("message", "from rank " + root, "algorithm", algorithm, "root", root)).json();
            boolean firstFromRoot = !maps(b.get("trace")).isEmpty() && num(maps(b.get("trace")).get(0).get("from")) == root;
            ok &= num(b.get("root")) == root && Boolean.TRUE.equals(b.get("identical")) && firstFromRoot;
            what.add(algorithm + " " + (Boolean.TRUE.equals(b.get("identical")) ? "identical" : "DIFFERENT"));
            line("  broadcast " + algorithm + " root " + b.get("root") + ": identical " + b.get("identical") + ", first message from rank "
                    + maps(b.get("trace")).get(0).get("from"));
        }
        List<Integer> data = range(4 * n);
        Map<String, Object> s = post("/api/collective/scatter", Map.of("data", data, "root", root)).json();
        boolean allFromRoot = true;
        for (Map<String, Object> t : maps(s.get("trace"))) {
            allFromRoot &= num(t.get("from")) == root;
        }
        Map<String, Object> g = post("/api/collective/gather", Map.of("randomDelay", true, "root", root)).json();
        boolean allToRoot = true;
        for (Map<String, Object> t : maps(g.get("trace"))) {
            allToRoot &= num(t.get("to")) == root;
        }
        Map<String, Object> d = post("/api/collective/demo", Map.of("root", root)).json();
        line("  scatter root " + s.get("root") + ": correct " + s.get("correct") + ", every message from rank " + root + ": " + allFromRoot);
        line("  gather root " + g.get("root") + ": buffer == data " + ints(g.get("result")).equals(data) + ", every message to rank "
                + root + ": " + allToRoot + ", arrival " + g.get("arrivalOrder"));
        line("  demo root " + d.get("root") + ": sum of squares " + d.get("sumOfSquares") + ", expected " + d.get("expected")
                + ", verified " + d.get("verified"));
        ok &= Boolean.TRUE.equals(s.get("correct")) && allFromRoot && ints(g.get("result")).equals(data) && allToRoot
                && Boolean.TRUE.equals(d.get("verified")) && num(d.get("root")) == root;
        check("C9 non-leader root", ok, "root rank " + root + " (leader's rank " + v.get("root") + "): broadcast " + what
                + ", scatter from it, gather to it in rank order, demo verified");
    }

    // =========================================================================
    // C8: elastic membership
    // =========================================================================

    private void c8() throws Exception {
        header("C8 add a node, then remove one: the communicator follows the membership and the operations stay correct");
        Map<String, Object> v0 = view();
        line("before: epoch " + v0.get("epoch") + ", nodes " + nodes(v0));
        Map<String, Object> added = post("/api/cluster/nodes", Map.of()).json();
        int newId = num(added.get("node"));
        waitUp();
        Map<String, Object> v1 = view();
        line("added node " + newId + ": epoch " + v1.get("epoch") + ", nodes " + nodes(v1) + ", root " + v1.get("root"));
        Resp stale = postRaw("/api/collective/gather", "{}");   // the chunks held belong to the old epoch
        line("  gather of the old epoch's chunks -> " + stale.status() + " " + stale.body());
        String afterAdd = exercise(v1);

        int removed = -1;
        for (int id : nodes(v1)) {
            if (removed < 0 && id != newId && !Integer.valueOf(id).equals(v1.get("leader"))) {
                removed = id;
            }
        }
        post("/api/cluster/nodes/" + removed + "/remove", Map.of());
        waitUp();
        Map<String, Object> v2 = view();
        line("removed node " + removed + ": epoch " + v2.get("epoch") + ", nodes " + nodes(v2) + ", root " + v2.get("root"));
        String afterRemove = exercise(v2);

        List<Integer> expected1 = new ArrayList<>(nodes(v0));
        expected1.add(newId);
        List<Integer> expected2 = new ArrayList<>(expected1);
        expected2.remove(Integer.valueOf(removed));
        boolean ranksOk = nodes(v1).equals(expected1) && nodes(v2).equals(expected2)
                && num(v1.get("epoch")) == num(v0.get("epoch")) + 1 && num(v2.get("epoch")) == num(v0.get("epoch")) + 2
                && num(v1.get("size")) == expected1.size() && num(v2.get("size")) == expected2.size();
        check("C8 communicator follows membership", ranksOk && stale.status() == 409,
                "size " + v0.get("size") + " -> " + v1.get("size") + " -> " + v2.get("size") + ", epoch " + v0.get("epoch") + " -> "
                        + v1.get("epoch") + " -> " + v2.get("epoch") + ", rank order = node id order " + nodes(v2)
                        + "; old-epoch chunks refused (" + stale.status() + ")");
        check("C8 operations correct after add and remove", afterAdd.isEmpty() && afterRemove.isEmpty(),
                afterAdd.isEmpty() && afterRemove.isEmpty() ? "broadcast (LINEAR, TREE), scatter + gather and the demo correct at sizes "
                        + v1.get("size") + " and " + v2.get("size") : afterAdd + afterRemove);
    }

    /** Broadcast both ways, scatter + gather and the demo on the current communicator; "" if all were correct. */
    private String exercise(Map<String, Object> v) throws Exception {
        int n = num(v.get("size"));
        StringBuilder bad = new StringBuilder();
        for (String algorithm : List.of("LINEAR", "TREE")) {
            Map<String, Object> b = post("/api/collective/broadcast", Map.of("message", "size " + n, "algorithm", algorithm)).json();
            int expectRounds = algorithm.equals("TREE") ? 32 - Integer.numberOfLeadingZeros(n - 1) : n - 1;
            line("  " + n + " ranks, broadcast " + algorithm + ": identical " + b.get("identical") + ", " + b.get("messages")
                    + " messages, " + b.get("rounds") + " rounds");
            if (!Boolean.TRUE.equals(b.get("identical")) || maps(b.get("ranks")).size() != n || num(b.get("rounds")) != expectRounds) {
                bad.append(algorithm).append(" broadcast wrong at size ").append(n).append("; ");
            }
        }
        List<Integer> data = range(3 * n);
        Map<String, Object> s = post("/api/collective/scatter", Map.of("data", data)).json();
        Map<String, Object> g = post("/api/collective/gather", Map.of("randomDelay", true)).json();
        Map<String, Object> d = post("/api/collective/demo", Map.of()).json();
        line("  " + n + " ranks, scatter " + data.size() + " ints: correct " + s.get("correct") + "; gather == data "
                + ints(g.get("result")).equals(data) + " (arrival " + g.get("arrivalOrder") + "); demo verified " + d.get("verified"));
        if (!Boolean.TRUE.equals(s.get("correct")) || !ints(g.get("result")).equals(data) || !Boolean.TRUE.equals(d.get("verified"))) {
            bad.append("scatter/gather/demo wrong at size ").append(n).append("; ");
        }
        return bad.toString();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Every member up and an agreed leader; restarts any member that is down. */
    private void ensureCluster() throws Exception {
        for (Map<String, Object> n : listOf(get("/api/cluster"))) {
            if (!Boolean.TRUE.equals(n.get("up"))) {
                post("/api/nodes/" + num(n.get("id")) + "/restart", Map.of());
            }
        }
        boolean up = waitUp();
        Map<String, Object> v = view();
        line("cluster: " + (up ? "all " + v.get("size") + " members up, leader node " + v.get("leader")
                : "NOT stable within " + STABLE_TIMEOUT_MS + " ms") + "; communicator epoch " + v.get("epoch") + ", nodes " + nodes(v));
    }

    /** Waits until every member is up and the leader is agreed. */
    private boolean waitUp() throws Exception {
        long deadline = System.currentTimeMillis() + STABLE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            boolean allUp = true;
            for (Map<String, Object> n : listOf(get("/api/cluster"))) {
                allUp &= Boolean.TRUE.equals(n.get("up"));
            }
            Map<String, Object> el = map(get("/api/election"));
            if (allUp && Boolean.TRUE.equals(el.get("agreed")) && el.get("leaderId") != null) {
                return true;
            }
            Thread.sleep(200);
        }
        return false;
    }

    private Map<String, Object> view() throws Exception {
        return map(get("/api/collective"));
    }

    private static List<Integer> nodes(Map<String, Object> view) {
        List<Integer> out = new ArrayList<>();
        for (Map<String, Object> r : maps(view.get("ranks"))) {
            out.add(num(r.get("node")));
        }
        return out;
    }

    private void printTrace(String label, Map<String, Object> r) {
        for (Map<String, Object> t : maps(r.get("trace"))) {
            line(String.format("  %-6s round %s: rank %s (node %s) -> rank %s (node %s), %s bytes, sent %s ms, received %s ms", label,
                    t.get("round"), t.get("from"), t.get("fromNode"), t.get("to"), t.get("toNode"), t.get("bytes"),
                    t.get("sentAtMs"), t.get("receivedAtMs")));
        }
    }

    private long lastSeq() throws Exception {
        long max = 0;
        for (Map<String, Object> e : listOf(get("/api/events"))) {
            max = Math.max(max, num(e.get("seq")));
        }
        return max;
    }

    private static List<Integer> range(int n) {
        List<Integer> out = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            out.add(i);
        }
        return out;
    }

    private static String sha256(String s) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
    }

    private Object get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path)).timeout(Duration.ofSeconds(10)).GET().build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) {
            throw new IllegalStateException("GET " + path + " -> " + res.statusCode() + " " + res.body());
        }
        return JsonUtil.parse(res.body());
    }

    /** POST that expects 200 for the add / kill / restart calls and every operation that must succeed. */
    private Resp post(String path, Map<String, Object> body) throws Exception {
        Resp r = postRaw(path, JsonUtil.toJson(body));
        boolean refusalExpected = path.startsWith("/api/collective/");
        if (r.status() != 200 && !refusalExpected) {
            throw new IllegalStateException("POST " + path + " -> " + r.status() + " " + r.body());
        }
        return r;
    }

    private Resp postRaw(String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        long t0 = System.nanoTime();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        return new Resp(res.statusCode(), res.body(), (System.nanoTime() - t0) / 1_000_000);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object o) {
        return (Map<String, Object>) o;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> listOf(Object o) {
        return o == null ? Collections.emptyList() : (List<Map<String, Object>>) o;
    }

    private static List<Map<String, Object>> maps(Object o) {
        return listOf(o);
    }

    private static List<Integer> ints(Object list) {
        List<Integer> out = new ArrayList<>();
        if (list != null) {
            for (Object o : (List<?>) list) {
                out.add(((Number) o).intValue());
            }
        }
        return out;
    }

    private static int num(Object o) {
        return o == null ? -1 : ((Number) o).intValue();
    }

    private void check(String name, boolean ok, String detail) {
        passed.put(name, ok);
        results.put(name, detail);
        line((ok ? "PASS " : "FAIL ") + name + ": " + detail);
    }

    private static void header(String title) {
        line("");
        line("---------------------------------------------------------------------");
        line(title);
        line("---------------------------------------------------------------------");
    }

    private static void line(String s) {
        System.out.println(s);
    }
}
