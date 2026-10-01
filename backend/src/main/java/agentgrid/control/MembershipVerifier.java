package agentgrid.control;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

/**
 * End-to-end verifier for elastic cluster membership (add / remove nodes). Drives the
 * control plane's HTTP API only (127.0.0.1:8080, a FRESH control plane with the 5 nodes of
 * cluster.properties). Each check runs in its own try/catch; between checks recover() brings
 * every member back up, ready, on the current epoch and agreed on a leader. Prints the raw
 * numbers of every step, a PASS/FAIL table and an OVERALL line; exits non-zero on a FAIL.
 *
 * Execution order differs from the numbering in two places, both forced by the rules being
 * tested: M9 (add failure) needs fewer than 9 members, so it runs after M10 has removed one;
 * M10's "the next add does not reuse an id" part runs after M9. M12 removes nodes down to 3
 * members to reach the minimum-size rule, so the cleanup (M13) re-adds fresh ids.
 *
 * Usage (from backend/): java -cp build/classes agentgrid.control.MembershipVerifier
 */
public class MembershipVerifier {

    private static final String BASE = System.getProperty("agentgrid.verifier.base", "http://127.0.0.1:8080");
    private static final String QUERY = "How do leader election and failure detectors handle a crashed node?";
    private static final long READY_TIMEOUT_MS = 40000;
    private static final long JOB_TIMEOUT_MS = 30000;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final Map<String, Boolean> passed = new LinkedHashMap<>();
    private final Map<String, String> results = new LinkedHashMap<>();
    private final String run = "m" + (System.currentTimeMillis() % 1_000_000);

    // State carried between checks.
    private long e0 = -1;
    private int added6 = -1;
    private String preAddKey;
    private String preAddValue;
    private Map<String, Object> addResponse;
    private long membershipChangedSeq = -1;
    private final List<Integer> removedIds = new ArrayList<>();
    private final List<Integer> allocatedIds = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        System.exit(new MembershipVerifier().runAll() ? 0 : 1);
    }

    private interface Check {
        void run() throws Exception;
    }

    private boolean runAll() throws Exception {
        line("=====================================================================");
        line(" AgentGrid-Lite membership verifier (HTTP API only) -> " + BASE);
        line("=====================================================================");
        line("run id " + run);
        try {
            recover("start");
            step("M1", this::m1);
            step("M2", this::m2);
            step("M3", this::m3);
            step("M4", this::m4);
            step("M5", this::m5);
            step("M6", this::m6);
            step("M7", this::m7);
            step("M8", this::m8);
            step("M10", this::m10Remove);
            step("M9", this::m9);
            step("M10b", this::m10NextAdd);
            step("M11", this::m11);
            step("M12", this::m12);
        } finally {
            step("M13", this::m13);
        }

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

    private void step(String id, Check body) {
        try {
            body.run();
        } catch (Exception e) {
            line("EXCEPTION in " + id + ": " + e);
            check(id + " aborted by exception", false, e.toString());
        }
        recover("after " + id);
    }

    // =========================================================================
    // M1 .. M3: start, add node 6, Bully leader
    // =========================================================================

    private void m1() throws Exception {
        header("M1 start: 5 members, epoch e0, all nodes report the same epoch");
        Map<String, Object> v = view();
        e0 = num(v.get("epoch"));
        List<Integer> ids = memberIds(v);
        Map<Integer, Long> epochs = waitEpochs(e0, 10000);
        line("membership: epoch " + e0 + ", members " + ids + ", quorum " + v.get("quorum") + ", nextId " + v.get("nextId"));
        line("epoch reported by each node: " + epochs);
        boolean ok = ids.equals(List.of(1, 2, 3, 4, 5)) && num(v.get("quorum")) == 3 && epochs.size() == 5
                && epochs.values().stream().allMatch(e -> e == e0);
        check("M1 5 members, all on epoch e0", ok, "epoch e0=" + e0 + ", members " + ids + ", node epochs " + epochs);
    }

    private void m2() throws Exception {
        header("M2 add a node: 6/6 online, epoch e0+1 on ALL nodes, quorum 4");
        preAddKey = "mem/" + run + "/written-before-add";
        preAddValue = "before-add-" + System.nanoTime();
        Map<String, Object> w = write(1, preAddKey, preAddValue, "STRONG");
        line("before the add: STRONG write " + preAddKey + " via node 1: " + w.get("status") + " acked=" + w.get("acked"));
        long seqBefore = lastSeq();
        long t0 = System.currentTimeMillis();
        HttpResponse<String> res = postRaw("/api/cluster/nodes", "{}");
        long addMs = System.currentTimeMillis() - t0;
        line("POST /api/cluster/nodes {} -> HTTP " + res.statusCode() + " in " + addMs + " ms");
        if (res.statusCode() != 200) {
            check("M2 add node", false, "HTTP " + res.statusCode() + " " + res.body());
            return;
        }
        addResponse = map(JsonUtil.parse(res.body()));
        added6 = (int) num(addResponse.get("node"));
        allocatedIds.add(added6);
        line("  node " + added6 + " port " + addResponse.get("port") + " pool " + addResponse.get("poolSize") + " weight "
                + addResponse.get("weight") + "; ready after " + addResponse.get("readyMs") + " ms, snapshot from "
                + addResponse.get("snapshotSource"));
        line("  pushed " + addResponse.get("pushed"));
        line("  membership " + summary(map(addResponse.get("membership"))));
        Map<String, Object> ev = firstEvent(seqBefore, "MEMBERSHIP_CHANGED");
        membershipChangedSeq = ev == null ? -1 : num(ev.get("seq"));
        line("  MEMBERSHIP_CHANGED event: " + (ev == null ? "MISSING" : ev.get("details") + " fields=" + ev.get("fields")));
        Map<String, Object> added = firstEvent(seqBefore, "NODE_ADDED");
        line("  NODE_ADDED event: " + (added == null ? "MISSING" : added.get("details")));

        Map<Integer, Long> epochs = waitEpochs(e0 + 1, 15000);
        Map<String, Object> v = view();
        int online = 0;
        for (Map<String, Object> n : listOf(v.get("nodes"))) {
            if (Boolean.TRUE.equals(n.get("up"))) online++;
        }
        line("after the add: membership " + summary(v) + "; online " + online + "/" + memberIds(v).size()
                + "; epochs " + epochs);
        boolean ok = added6 == 6 && memberIds(v).size() == 6 && online == 6 && num(v.get("epoch")) == e0 + 1
                && num(v.get("quorum")) == 4 && epochs.size() == 6 && epochs.values().stream().allMatch(e -> e == e0 + 1)
                && ev != null && added != null;
        check("M2 add node 6: 6/6 online, e0+1 everywhere, quorum 4", ok, "node " + added6 + " added in " + addMs
                + " ms; online " + online + "/6; epoch " + v.get("epoch") + " on " + epochs + "; quorum " + v.get("quorum"));
    }

    private void m3() throws Exception {
        header("M3 all 6 agree on a leader, and it is the new node (Bully, highest id)");
        Map<String, Object> el = waitLeader(added6, memberIds(view()), 15000);
        // Convergence from the events: MEMBERSHIP_CHANGED (the moment node 6 became a member)
        // to the last member's first LEADER_ACCEPTED of node 6 after it.
        long convergence = -1;
        Map<String, Object> mc = eventBySeq(membershipChangedSeq);
        if (mc != null) {
            long startMs = num(mc.get("trueMs"));
            Map<Integer, Long> firstAccept = new TreeMap<>();
            for (Map<String, Object> e : events(membershipChangedSeq)) {
                Map<String, Object> f = map(e.get("fields"));
                if ("LEADER_ACCEPTED".equals(e.get("type")) && f != null && num(f.get("leader")) == added6) {
                    firstAccept.putIfAbsent((int) num(e.get("node")), num(e.get("trueMs")));
                }
            }
            line("first LEADER_ACCEPTED of node " + added6 + " per node, ms after MEMBERSHIP_CHANGED: " + offsets(firstAccept, startMs));
            if (firstAccept.size() == 6) {
                convergence = Collections.max(firstAccept.values()) - startMs;
            }
        }
        line("election: agreed=" + (el != null) + " leader=" + (el == null ? null : el.get("leaderId")) + " views="
                + (el == null ? map(get("/api/election")).get("views") : el.get("views")));
        check("M3 all 6 agree on leader " + added6, el != null && num(el.get("leaderId")) == added6 && convergence >= 0,
                "leader " + (el == null ? "not agreed" : el.get("leaderId")) + "; all 6 accepted it " + convergence
                        + " ms after MEMBERSHIP_CHANGED");
    }

    // =========================================================================
    // M4 .. M7: work on the new node, snapshot, clocks, quorum 4
    // =========================================================================

    private void m4() throws Exception {
        header("M4 a WEIGHTED job completes and at least one subtask runs on node " + added6);
        Map<String, Object> job = runJob("WEIGHTED", "EVENTUAL");
        Map<Integer, Integer> perNode = new TreeMap<>();
        for (Map<String, Object> stage : listOf(job.get("stages"))) {
            for (Map<String, Object> s : listOf(stage.get("subtasks"))) {
                if (s.get("node") != null) perNode.merge((int) num(s.get("node")), 1, Integer::sum);
            }
        }
        line("job " + job.get("jobId") + " " + job.get("status") + " makespan " + job.get("makespanMs") + " ms; subtasks per node "
                + perNode + "; leader " + job.get("leaderNode"));
        int onNew = perNode.getOrDefault(added6, 0);
        check("M4 WEIGHTED job uses node " + added6, "COMPLETE".equals(job.get("status")) && onNew > 0,
                job.get("status") + ", " + onNew + " of " + perNode.values().stream().mapToInt(i -> i).sum()
                        + " subtasks on node " + added6 + " " + perNode);
    }

    private void m5() throws Exception {
        header("M5 node " + added6 + " holds a key written before it was added; a STRONG write reaches all 6");
        Map<String, Object> r = read(added6, preAddKey);
        boolean snapshotOk = Boolean.TRUE.equals(r.get("found")) && preAddValue.equals(r.get("value"));
        line("read " + preAddKey + " on node " + added6 + ": found=" + r.get("found") + " value matches=" + preAddValue.equals(r.get("value"))
                + " verdict=" + r.get("verdict"));
        String key = "mem/" + run + "/after-add";
        Map<String, Object> w = write(2, key, "six", "STRONG");
        List<String> reads = new ArrayList<>();
        int fresh = 0;
        for (int n : memberIds(view())) {
            String v = String.valueOf(read(n, key).get("verdict"));
            reads.add("N" + n + "=" + v);
            if ("FRESH".equals(v)) fresh++;
        }
        line("STRONG write " + key + " via node 2: " + w.get("status") + " liveAtCheck=" + w.get("liveAtCheck") + " acked="
                + w.get("acked") + " in " + w.get("latencyMs") + " ms; reads " + reads);
        boolean ok = snapshotOk && "STORED".equals(w.get("status")) && listOf(w.get("acked")).size() == 6 && fresh == 6;
        check("M5 snapshot on join + STRONG on 6", ok, "pre-add key on node " + added6 + ": " + snapshotOk + "; STRONG "
                + w.get("status") + " acked by " + listOf(w.get("acked")).size() + ", FRESH on " + fresh + "/6");
    }

    private void m6() throws Exception {
        header("M6 the Berkeley round after the add covers 6 nodes, spread <= 100 ms");
        Map<String, Object> round = map(addResponse == null ? null : addResponse.get("clockRound"));
        line("round run by the add: " + round);
        Map<String, Object> manual = map(post("/api/clock/sync", "{}"));
        line("manual round now: nodeCount=" + manual.get("nodeCount") + " spreadBefore=" + manual.get("spreadBefore")
                + " spreadAfter=" + manual.get("spreadAfter") + " coordinator=" + manual.get("coordinator"));
        Map<Integer, Long> offsets = new TreeMap<>();
        for (Map<String, Object> m : listOf(get("/api/clock/drift"))) {
            if (m.get("offsetMs") != null) offsets.put((int) num(m.get("node")), num(m.get("offsetMs")));
        }
        long spread = offsets.isEmpty() ? -1 : Collections.max(offsets.values()) - Collections.min(offsets.values());
        line("offsets vs control plane now: " + offsets + " (spread " + spread + " ms)");
        boolean roundOk = round != null && round.get("error") == null && num(round.get("nodeCount")) == 6
                && num(round.get("spreadAfter")) <= 100;
        boolean ok = roundOk && offsets.size() == 6 && spread <= 100 && num(manual.get("nodeCount")) == 6;
        check("M6 Berkeley round over 6, spread <= 100 ms", ok, "join round nodeCount=" + (round == null ? null : round.get("nodeCount"))
                + " spreadAfter=" + (round == null ? null : round.get("spreadAfter")) + " ms; offsets spread now " + spread + " ms over "
                + offsets.size() + " nodes");
    }

    private void m7() throws Exception {
        header("M7 kill 3 of the 6: STRONG is REFUSED (3 live < quorum 4); restart them and recover");
        List<Integer> victims = List.of(1, 2, 3);
        for (int v : victims) {
            post("/api/nodes/" + v + "/kill", "{}");
        }
        List<Integer> up = waitUpSet(List.of(4, 5, added6), 15000);
        line("killed " + victims + "; UP now " + up);
        String key = "mem/" + run + "/three-of-six";
        Map<String, Object> w = write(4, key, "must be refused", "STRONG");
        line("STRONG write via node 4: " + w.get("status") + " liveAtCheck=" + w.get("liveAtCheck") + " message=\"" + w.get("message") + "\"");
        boolean refused = "REFUSED".equals(w.get("status")) && listOf(w.get("liveAtCheck")).size() == 3
                && String.valueOf(w.get("message")).contains("needs 4");
        List<String> reads = new ArrayList<>();
        for (int n : List.of(4, 5, added6)) {
            Map<String, Object> r = read(n, key);
            reads.add("N" + n + " found=" + r.get("found"));
            refused &= Boolean.FALSE.equals(r.get("found"));
        }
        line("  reads on the live nodes: " + reads);
        restartParallel(victims);
        boolean ready = waitReady(READY_TIMEOUT_MS) != null;
        String key2 = "mem/" + run + "/recovered";
        Map<String, Object> w2 = write(1, key2, "back to six", "STRONG");
        line("restarted " + victims + ": all ready=" + ready + "; STRONG write via node 1: " + w2.get("status") + " acked=" + w2.get("acked"));
        boolean recovered = ready && "STORED".equals(w2.get("status")) && listOf(w2.get("acked")).size() == 6;
        check("M7 3 live < quorum 4 refused, then recovery", refused && recovered,
                "refused=" + refused + " (" + w.get("message") + "); after restart STORED on " + listOf(w2.get("acked")).size());
    }

    // =========================================================================
    // M8 .. M11: fill to 9, failure path, remove
    // =========================================================================

    private void m8() throws Exception {
        header("M8 add nodes up to 9 members; the 10th add returns 409");
        List<String> adds = new ArrayList<>();
        boolean ok = true;
        while (memberIds(view()).size() < 9) {
            long t0 = System.currentTimeMillis();
            HttpResponse<String> res = postRaw("/api/cluster/nodes", "{}");
            if (res.statusCode() != 200) {
                adds.add("HTTP " + res.statusCode() + " " + res.body());
                ok = false;
                break;
            }
            Map<String, Object> r = map(JsonUtil.parse(res.body()));
            allocatedIds.add((int) num(r.get("node")));
            adds.add("node " + r.get("node") + " in " + (System.currentTimeMillis() - t0) + " ms (epoch "
                    + map(r.get("membership")).get("epoch") + ")");
        }
        line("adds: " + adds);
        Map<String, Object> v = view();
        Map<Integer, Long> epochs = waitEpochs(num(v.get("epoch")), 20000);
        HttpResponse<String> tenth = postRaw("/api/cluster/nodes", "{}");
        line("membership " + summary(v) + "; epochs " + epochs);
        line("10th add -> HTTP " + tenth.statusCode() + " " + tenth.body());
        ok &= memberIds(v).size() == 9 && num(v.get("quorum")) == 5 && epochs.size() == 9
                && epochs.values().stream().allMatch(e -> e == num(v.get("epoch")))
                && tenth.statusCode() == 409 && memberIds(view()).size() == 9;
        check("M8 up to 9 members, 10th add 409", ok, memberIds(v) + " quorum " + v.get("quorum") + "; 10th add HTTP " + tenth.statusCode());
    }

    private void m10Remove() throws Exception {
        Map<String, Object> el = map(get("/api/election"));
        int leader = (int) num(el.get("leaderId"));
        int victim = -1;
        for (int id : allocatedIds) {
            if (id != leader && memberIds(view()).contains(id) && id != added6) {
                victim = id;
                break;
            }
        }
        header("M10 remove a live non-leader (node " + victim + "; leader is " + leader + "): epoch bump everywhere, restart of it is 404");
        Map<String, Object> before = view();
        long epochBefore = num(before.get("epoch"));
        HttpResponse<String> res = postRaw("/api/cluster/nodes/" + victim + "/remove", "{}");
        line("POST /api/cluster/nodes/" + victim + "/remove -> HTTP " + res.statusCode() + " " + res.body());
        removedIds.add(victim);
        Map<Integer, Long> epochs = waitEpochs(epochBefore + 1, 15000);
        HttpResponse<String> restart = postRaw("/api/nodes/" + victim + "/restart", "{}");
        HttpResponse<String> kill = postRaw("/api/nodes/" + victim + "/kill", "{}");
        Map<String, Object> after = view();
        line("after: membership " + summary(after) + "; epochs " + epochs);
        line("restart node " + victim + " -> HTTP " + restart.statusCode() + " " + restart.body());
        line("kill node " + victim + " -> HTTP " + kill.statusCode());
        line("tracked node processes: " + after.get("processes"));
        boolean ok = res.statusCode() == 200 && !memberIds(after).contains(victim) && memberIds(after).size() == 8
                && num(after.get("epoch")) == epochBefore + 1 && epochs.size() == 8
                && epochs.values().stream().allMatch(e -> e == epochBefore + 1)
                && restart.statusCode() == 404 && !map(after.get("processes")).containsKey(String.valueOf(victim));
        check("M10 remove live non-leader " + victim, ok, "epoch " + epochBefore + " -> " + after.get("epoch") + " on all "
                + epochs.size() + " remaining; restart " + victim + " HTTP " + restart.statusCode() + "; its process gone");
    }

    private void m9() throws Exception {
        Map<String, Object> before = view();
        int nextId = (int) num(before.get("nextId"));
        int port = 1600 + nextId;
        header("M9 add failure: port " + port + " (next id " + nextId + ") is occupied -> NODE_ADD_FAILED, membership unchanged");
        long seq = lastSeq();
        HttpResponse<String> res;
        long t0 = System.currentTimeMillis();
        try (ServerSocket squatter = new ServerSocket()) {
            squatter.bind(new InetSocketAddress(port));
            line("verifier holds port " + port + " with a ServerSocket");
            res = postRaw("/api/cluster/nodes", "{}");
        }
        long ms = System.currentTimeMillis() - t0;
        allocatedIds.add(nextId);
        Map<String, Object> after = view();
        Map<String, Object> failedEv = firstEvent(seq, "NODE_ADD_FAILED");
        boolean portFree = portFree(port);
        line("POST /api/cluster/nodes -> HTTP " + res.statusCode() + " after " + ms + " ms: " + res.body());
        line("NODE_ADD_FAILED event: " + (failedEv == null ? "MISSING" : failedEv.get("details")));
        line("before: " + summary(before) + "; after: " + summary(after) + "; next id now " + after.get("nextId"));
        line("tracked node processes after: " + after.get("processes") + "; port " + port + " free after the verifier released it: " + portFree);
        boolean ok = res.statusCode() == 500 && failedEv != null && num(after.get("epoch")) == num(before.get("epoch"))
                && memberIds(after).equals(memberIds(before))
                && !map(after.get("processes")).containsKey(String.valueOf(nextId)) && portFree;
        check("M9 add failure leaves nothing behind", ok, "HTTP " + res.statusCode() + ", NODE_ADD_FAILED "
                + (failedEv != null) + ", epoch " + before.get("epoch") + " -> " + after.get("epoch") + ", members unchanged "
                + memberIds(after).equals(memberIds(before)) + ", no process for id " + nextId);
    }

    private void m10NextAdd() throws Exception {
        header("M10 (continued) the next add does not reuse an id (removed " + removedIds + ", allocated so far " + allocatedIds + ")");
        HttpResponse<String> res = postRaw("/api/cluster/nodes", "{}");
        line("POST /api/cluster/nodes -> HTTP " + res.statusCode() + " " + (res.statusCode() == 200 ? "" : res.body()));
        int id = res.statusCode() == 200 ? (int) num(map(JsonUtil.parse(res.body())).get("node")) : -1;
        boolean ok = id > 0 && !removedIds.contains(id) && !allocatedIds.contains(id)
                && id > Collections.max(allocatedIds);
        if (id > 0) allocatedIds.add(id);
        line("new node id " + id + "; membership " + summary(view()));
        check("M10 ids are not reused", ok, "next add got id " + id + " (removed " + removedIds + ", earlier ids up to "
                + (allocatedIds.size() > 1 ? allocatedIds.get(allocatedIds.size() - 2) : -1) + ")");
    }

    private void m11() throws Exception {
        Map<String, Object> el = map(get("/api/election"));
        int leader = (int) num(el.get("leaderId"));
        header("M11 remove the leader (node " + leader + "): the remaining members agree on a new one");
        long t0 = System.currentTimeMillis();
        HttpResponse<String> res = postRaw("/api/cluster/nodes/" + leader + "/remove", "{}");
        line("POST /api/cluster/nodes/" + leader + "/remove -> HTTP " + res.statusCode() + " " + res.body());
        removedIds.add(leader);
        List<Integer> remaining = memberIds(view());
        Map<String, Object> agreed = null;
        long agreedMs = -1;
        long deadline = t0 + 20000;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> e = map(get("/api/election"));
            if (Boolean.TRUE.equals(e.get("agreed")) && e.get("leaderId") != null && num(e.get("leaderId")) != leader
                    && allViews(e, remaining, num(e.get("leaderId")))) {
                agreed = e;
                agreedMs = System.currentTimeMillis() - t0;
                break;
            }
            Thread.sleep(100);
        }
        int expected = Collections.max(remaining);
        line("remaining members " + remaining + "; agreed leader " + (agreed == null ? "NONE" : agreed.get("leaderId"))
                + " after " + agreedMs + " ms; views " + (agreed == null ? map(get("/api/election")).get("views") : agreed.get("views")));
        boolean ok = res.statusCode() == 200 && agreed != null && num(agreed.get("leaderId")) == expected;
        check("M11 remove the leader " + leader, ok, "new leader " + (agreed == null ? "none" : agreed.get("leaderId"))
                + " (expected " + expected + ") agreed by all " + remaining.size() + " after " + agreedMs + " ms");
    }

    // =========================================================================
    // M12: guards
    // =========================================================================

    private void m12() throws Exception {
        header("M12 remove guards: 409 while a job runs, 409 below quorum (live node), dead node allowed, 409 below 3 members");
        // Back to the five original members first.
        for (int id : new ArrayList<>(memberIds(view()))) {
            if (id > 5) {
                HttpResponse<String> r = postRaw("/api/cluster/nodes/" + id + "/remove", "{}");
                line("setup: remove node " + id + " -> HTTP " + r.statusCode());
                removedIds.add(id);
            }
        }
        waitReady(READY_TIMEOUT_MS);
        line("setup done: membership " + summary(view()));
        List<String> problems = new ArrayList<>();

        // (b) a job is running
        Map<String, Object> submit = map(post("/api/jobs", "{\"query\":\"" + QUERY + "\",\"policy\":\"ROUND_ROBIN\",\"consistency\":\"EVENTUAL\"}"));
        HttpResponse<String> whileJob = postRaw("/api/cluster/nodes/1/remove", "{}");
        line("(b) job " + submit.get("jobId") + " submitted; remove node 1 -> HTTP " + whileJob.statusCode() + " " + whileJob.body());
        if (whileJob.statusCode() != 409 || !whileJob.body().contains("running")) problems.add("no 409 while a job runs");
        waitJob((String) submit.get("jobId"));

        // (c) kill 4 and 5: live 3 = quorum 3 of 5
        post("/api/nodes/4/kill", "{}");
        post("/api/nodes/5/kill", "{}");
        List<Integer> up = waitUpSet(List.of(1, 2, 3), 15000);
        line("(c) killed 4 and 5; UP " + up);
        HttpResponse<String> liveBelow = postRaw("/api/cluster/nodes/1/remove", "{}");
        line("(c) remove LIVE node 1 (would leave 2 live < quorum 3 of 4) -> HTTP " + liveBelow.statusCode() + " " + liveBelow.body());
        if (liveBelow.statusCode() != 409) problems.add("removing a live node below quorum was not 409");
        HttpResponse<String> dead5 = postRaw("/api/cluster/nodes/5/remove", "{}");
        line("(c) remove DEAD node 5 (live 3 == quorum 3 of the new 4 members) -> HTTP " + dead5.statusCode() + " " + dead5.body());
        if (dead5.statusCode() != 200) problems.add("removing dead node 5 was not allowed");
        else removedIds.add(5);
        HttpResponse<String> dead4 = postRaw("/api/cluster/nodes/4/remove", "{}");
        line("(c) remove DEAD node 4 (3 members left) -> HTTP " + dead4.statusCode() + " " + dead4.body());
        if (dead4.statusCode() != 200) problems.add("removing dead node 4 was not allowed");
        else removedIds.add(4);

        // (a) minimum size
        Map<String, Object> v = view();
        HttpResponse<String> below3 = postRaw("/api/cluster/nodes/3/remove", "{}");
        line("(a) membership " + summary(v) + "; remove node 3 -> HTTP " + below3.statusCode() + " " + below3.body());
        if (below3.statusCode() != 409 || memberIds(view()).size() != 3) problems.add("going below 3 members was not 409");
        HttpResponse<String> unknown = postRaw("/api/cluster/nodes/99/remove", "{}");
        line("remove unknown node 99 -> HTTP " + unknown.statusCode() + " " + unknown.body());
        if (unknown.statusCode() != 404) problems.add("unknown id was not 404");
        check("M12 remove guards", problems.isEmpty(), "job 409=" + whileJob.statusCode() + ", live below quorum " + liveBelow.statusCode()
                + ", dead node " + dead5.statusCode() + "/" + dead4.statusCode() + ", below 3 " + below3.statusCode()
                + ", unknown " + unknown.statusCode() + (problems.isEmpty() ? "" : "; " + problems));
    }

    // =========================================================================
    // M13: cleanup (always runs)
    // =========================================================================

    private void m13() throws Exception {
        header("M13 cleanup: back to a normal 5-member cluster (all up, ready, agreed leader)");
        recover("cleanup");
        List<Integer> ids = memberIds(view());
        while (ids.size() > 5) {
            int id = Collections.max(ids);
            HttpResponse<String> r = postRaw("/api/cluster/nodes/" + id + "/remove", "{}");
            line("remove node " + id + " -> HTTP " + r.statusCode());
            if (r.statusCode() != 200) break;
            ids = memberIds(view());
        }
        while (ids.size() < 5) {
            HttpResponse<String> r = postRaw("/api/cluster/nodes", "{}");
            line("add -> HTTP " + r.statusCode() + (r.statusCode() == 200 ? " node " + map(JsonUtil.parse(r.body())).get("node") : " " + r.body()));
            if (r.statusCode() != 200) break;
            ids = memberIds(view());
        }
        Map<String, Object> st = waitReady(READY_TIMEOUT_MS);
        Map<String, Object> v = view();
        Map<String, Object> el = map(get("/api/election"));
        line("final membership " + summary(v) + "; all ready=" + (st != null) + "; leader " + el.get("leaderId")
                + "; tracked processes " + v.get("processes"));
        boolean ok = memberIds(v).size() == 5 && st != null && map(v.get("processes")).size() == 5;
        check("M13 cleanup to 5 members", ok, "members " + memberIds(v) + " (ids are never reused, so the removed "
                + "originals come back as new ids), leader " + el.get("leaderId"));
    }

    // =========================================================================
    // Recovery between checks
    // =========================================================================

    private void recover(String when) {
        try {
            if (waitReady(8000) != null) return;
            for (int attempt = 1; attempt <= 3; attempt++) {
                List<Integer> down = new ArrayList<>();
                for (Map<String, Object> n : listOf(view().get("nodes"))) {
                    if (!Boolean.TRUE.equals(n.get("up"))) down.add((int) num(n.get("id")));
                }
                line("recovery " + when + ", attempt " + attempt + ": down " + down + ", restarting them");
                restartParallel(down);
                if (waitReady(READY_TIMEOUT_MS) != null) {
                    line("recovery " + when + ": all members up, ready, on epoch " + view().get("epoch") + ", leader "
                            + map(get("/api/election")).get("leaderId"));
                    return;
                }
            }
            check("recovery " + when, false, "members not all up/ready/agreed after 3 attempts: " + summary(view()));
        } catch (Exception e) {
            check("recovery " + when, false, "threw " + e);
        }
    }

    /** Every member up with a ready replica on the control plane's epoch, and an agreed member leader; else null. */
    private Map<String, Object> waitReady(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> v = view();
            List<Integer> ids = memberIds(v);
            long epoch = num(v.get("epoch"));
            boolean epochsOk = v.get("changeInProgress") == null;
            int up = 0;
            for (Map<String, Object> n : listOf(v.get("nodes"))) {
                if (Boolean.TRUE.equals(n.get("up"))) up++;
                if (!Boolean.TRUE.equals(n.get("up")) || n.get("epoch") == null || num(n.get("epoch")) != epoch) epochsOk = false;
            }
            int ready = 0;
            for (Map<String, Object> n : listOf(map(get("/api/blackboard?prefix=none-" + run)).get("nodes"))) {
                if (Boolean.TRUE.equals(n.get("up")) && Boolean.TRUE.equals(n.get("ready"))) ready++;
            }
            Map<String, Object> el = map(get("/api/election"));
            boolean agreed = Boolean.TRUE.equals(el.get("agreed")) && el.get("leaderId") != null
                    && ids.contains((int) num(el.get("leaderId")));
            if (up == ids.size() && ready == ids.size() && epochsOk && agreed) return v;
            Thread.sleep(200);
        }
        return null;
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private Map<Integer, Long> waitEpochs(long epoch, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Map<Integer, Long> out = new TreeMap<>();
        while (System.currentTimeMillis() < deadline) {
            out = new TreeMap<>();
            boolean all = true;
            for (Map<String, Object> n : listOf(view().get("nodes"))) {
                Long e = n.get("epoch") == null ? null : num(n.get("epoch"));
                out.put((int) num(n.get("id")), e);
                if (e == null || e != epoch) all = false;
            }
            if (all) return out;
            Thread.sleep(150);
        }
        return out;
    }

    private Map<String, Object> waitLeader(int expected, List<Integer> members, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> el = map(get("/api/election"));
            if (Boolean.TRUE.equals(el.get("agreed")) && el.get("leaderId") != null && num(el.get("leaderId")) == expected
                    && allViews(el, members, expected)) {
                return el;
            }
            Thread.sleep(100);
        }
        return null;
    }

    private static boolean allViews(Map<String, Object> el, List<Integer> members, long leader) {
        Map<String, Object> views = map(el.get("views"));
        for (int id : members) {
            Object v = views.get(String.valueOf(id));
            if (v == null || num(v) != leader) return false;
        }
        return true;
    }

    private List<Integer> waitUpSet(List<Integer> expected, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        List<Integer> up = new ArrayList<>();
        while (System.currentTimeMillis() < deadline) {
            up = new ArrayList<>();
            for (Map<String, Object> n : listOf(get("/api/cluster"))) {
                if (Boolean.TRUE.equals(n.get("up"))) up.add((int) num(n.get("id")));
            }
            Collections.sort(up);
            List<Integer> exp = new ArrayList<>(expected);
            Collections.sort(exp);
            if (up.equals(exp)) return up;
            Thread.sleep(100);
        }
        return up;
    }

    private Map<String, Object> runJob(String policy, String consistency) throws Exception {
        Map<String, Object> resp = map(post("/api/jobs", "{\"query\":\"" + QUERY + "\",\"policy\":\"" + policy
                + "\",\"consistency\":\"" + consistency + "\"}"));
        return waitJob((String) resp.get("jobId"));
    }

    private Map<String, Object> waitJob(String jobId) throws Exception {
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
        Map<String, Object> job;
        do {
            Thread.sleep(100);
            job = map(get("/api/jobs/" + jobId));
        } while (("RUNNING".equals(job.get("status")) || "QUEUED".equals(job.get("status"))) && System.currentTimeMillis() < deadline);
        return job;
    }

    private void restartParallel(List<Integer> nodes) throws Exception {
        CountDownLatch done = new CountDownLatch(nodes.size());
        for (int n : nodes) {
            new Thread(() -> {
                try {
                    HttpResponse<String> r = postRaw("/api/nodes/" + n + "/restart", "{}");
                    if (r.statusCode() != 200) line("restart " + n + " -> HTTP " + r.statusCode() + " " + r.body());
                } catch (Exception e) {
                    line("restart " + n + " failed: " + e.getMessage());
                } finally {
                    done.countDown();
                }
            }).start();
        }
        done.await();
    }

    private Map<String, Object> write(int node, String key, String value, String mode) throws Exception {
        return map(post("/api/blackboard/write", "{\"node\":" + node + ",\"key\":\"" + key + "\",\"value\":\"" + value
                + "\",\"mode\":\"" + mode + "\"}"));
    }

    private Map<String, Object> read(int node, String key) throws Exception {
        return map(get("/api/blackboard/read?node=" + node + "&key=" + URLEncoder.encode(key, StandardCharsets.UTF_8)));
    }

    private Map<String, Object> view() throws Exception {
        return map(get("/api/cluster?view=membership"));
    }

    private static List<Integer> memberIds(Map<String, Object> v) {
        List<Integer> ids = new ArrayList<>();
        for (Map<String, Object> m : listOf(v.get("members"))) {
            ids.add((int) num(m.get("id")));
        }
        return ids;
    }

    private static String summary(Map<String, Object> v) {
        return v == null ? "null" : "epoch " + v.get("epoch") + " members " + memberIds(v) + " quorum " + v.get("quorum");
    }

    private List<Map<String, Object>> events(long since) throws Exception {
        return listOf(get("/api/events?since=" + since));
    }

    private Map<String, Object> firstEvent(long since, String type) throws Exception {
        for (Map<String, Object> e : events(since)) {
            if (type.equals(e.get("type"))) return e;
        }
        return null;
    }

    private Map<String, Object> eventBySeq(long seq) throws Exception {
        if (seq < 0) return null;
        for (Map<String, Object> e : events(seq - 1)) {
            if (num(e.get("seq")) == seq) return e;
        }
        return null;
    }

    private static String offsets(Map<Integer, Long> at, long startMs) {
        Map<Integer, Long> out = new TreeMap<>();
        at.forEach((k, v) -> out.put(k, v - startMs));
        return out.toString();
    }

    private long lastSeq() throws Exception {
        long max = 0;
        for (Map<String, Object> e : events(0)) {
            max = Math.max(max, num(e.get("seq")));
        }
        return max;
    }

    private static boolean portFree(int port) {
        try (ServerSocket probe = new ServerSocket()) {
            probe.setReuseAddress(false);
            probe.bind(new InetSocketAddress(port));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private Object get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path)).timeout(Duration.ofSeconds(20)).GET().build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) {
            throw new IllegalStateException("GET " + path + " -> " + res.statusCode() + " " + res.body());
        }
        return JsonUtil.parse(res.body());
    }

    private String post(String path, String body) throws Exception {
        HttpResponse<String> res = postRaw(path, body);
        if (res.statusCode() != 200) {
            throw new IllegalStateException("POST " + path + " -> " + res.statusCode() + " " + res.body());
        }
        return res.body();
    }

    private HttpResponse<String> postRaw(String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object o) {
        if (o instanceof String s) {
            return (Map<String, Object>) JsonUtil.parse(s);
        }
        return (Map<String, Object>) o;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> listOf(Object o) {
        return o == null ? Collections.emptyList() : (List<Map<String, Object>>) o;
    }

    private static long num(Object o) {
        return o == null ? -1 : ((Number) o).longValue();
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

    private static synchronized void line(String s) {
        System.out.println(s);
    }
}
