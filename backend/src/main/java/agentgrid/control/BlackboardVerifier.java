package agentgrid.control;

import agentgrid.node.BlackboardRecord;
import agentgrid.orchestrator.Corpus;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * End-to-end verifier for Phase 5: the replicated blackboard in the live cluster (Exp 5).
 * Drives the control plane's HTTP API only (127.0.0.1:8080 must be running). B6 checks the
 * merge function in-process, and B8 reads the bundled corpus from the classpath to check that
 * answer sentences come from the cited documents. Prints raw numbers for every step and a
 * PASS/FAIL/INFO table; exits non-zero if a counted check fails.
 *
 * Usage (from backend/): java -cp build/classes agentgrid.control.BlackboardVerifier
 */
public class BlackboardVerifier {

    private static final String BASE = System.getProperty("agentgrid.verifier.base", "http://127.0.0.1:8080");
    private static final String QUERY = "How do leader election and failure detectors handle a crashed node?";
    private static final List<Integer> ALL = List.of(1, 2, 3, 4, 5);
    private static final int ROUNDS = 20;
    private static final long STABLE_TIMEOUT_MS = 30000;
    private static final long JOB_TIMEOUT_MS = 30000;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final Map<String, Boolean> passed = new LinkedHashMap<>();
    private final Map<String, String> results = new LinkedHashMap<>();
    private final Map<String, String> info = new LinkedHashMap<>();
    private final String run = "r" + (System.currentTimeMillis() % 1_000_000);
    private final List<Long> strongWriteMs = new ArrayList<>();
    private final List<Long> eventualWriteMs = new ArrayList<>();
    private long startSeq;
    /** Nodes that were up when recovery last failed; empty while the cluster is whole. */
    private List<Integer> degradedUp = List.of();

    public static void main(String[] args) throws Exception {
        boolean ok = new BlackboardVerifier().runAll();
        System.exit(ok ? 0 : 1);
    }

    private boolean runAll() throws Exception {
        line("=====================================================================");
        line(" AgentGrid-Lite Phase 5 blackboard verifier (HTTP API only) -> " + BASE);
        line("=====================================================================");
        line("run id: " + run + "; modules: " + get("/api/modules"));
        startSeq = lastSeq();
        recover("start");

        // Each check runs on its own: an exception is a FAIL for that check and the run goes on,
        // and the cluster is brought back to 5 ready nodes before the next one.
        step("B1", this::b1);
        step("B2", this::b2);
        step("B3", this::b3);
        step("B4", this::b4);
        step("B5", this::b5);
        step("B6", this::b6);
        step("B7", this::b7);
        step("B8", this::b8);
        step("B9", this::b9);
        step("B10", this::b10);
        step("B11", this::b11);

        line("");
        line("=====================================================================");
        line(" RESULT");
        line("=====================================================================");
        boolean all = true;
        for (Map.Entry<String, Boolean> e : passed.entrySet()) {
            System.out.printf("%-4s  %-40s %s%n", e.getValue() ? "PASS" : "FAIL", e.getKey(), results.get(e.getKey()));
            all &= e.getValue();
        }
        for (Map.Entry<String, String> e : info.entrySet()) {
            System.out.printf("%-4s  %-40s %s%n", "INFO", e.getKey(), e.getValue());
        }
        line("OVERALL: " + (all ? "PASS" : "FAIL"));
        return all;
    }

    private interface Check {
        void run() throws Exception;
    }

    private void step(String id, Check body) {
        if (!degradedUp.isEmpty()) {
            line("");
            line("WARNING: " + id + " runs with only nodes " + degradedUp + " up (recovery failed)");
        }
        try {
            body.run();
        } catch (Exception e) {
            line("EXCEPTION in " + id + ": " + e);
            check(id + " aborted by exception", false, e.toString());
        }
        recover("after " + id);
    }

    /**
     * Brings the cluster back to 5 up and ready nodes with an agreed leader, restarting each
     * missing node up to 3 times. Records a FAIL if that does not work; the remaining checks
     * then run on whatever is up.
     */
    private void recover(String when) {
        try {
            if (waitReadyAll(10000) != null) {
                degradedUp = List.of();
                return;
            }
            for (int attempt = 1; attempt <= 3; attempt++) {
                List<Integer> missing = notReady();
                line("recovery " + when + ", attempt " + attempt + ": nodes not up and ready " + missing + ", restarting them");
                for (int n : missing) {
                    try {
                        post("/api/nodes/" + n + "/restart", "{}");
                    } catch (Exception e) {
                        line("  restart " + n + " failed: " + e.getMessage());
                    }
                }
                if (waitReadyAll(STABLE_TIMEOUT_MS) != null) {
                    line("recovery " + when + ": all 5 up and ready, leader " + map(get("/api/election")).get("leaderId"));
                    degradedUp = List.of();
                    return;
                }
            }
            degradedUp = upNodes();
            check("recovery " + when, false, "cluster not back to 5 ready nodes after 3 attempts; remaining checks run on nodes " + degradedUp);
        } catch (Exception e) {
            check("recovery " + when, false, "threw " + e);
        }
    }

    private List<Integer> notReady() throws Exception {
        List<Integer> out = new ArrayList<>(ALL);
        for (Map<String, Object> n : listOf(map(get("/api/blackboard?prefix=none-" + run)).get("nodes"))) {
            if (Boolean.TRUE.equals(n.get("up")) && Boolean.TRUE.equals(n.get("ready"))) {
                out.remove(Integer.valueOf((int) num(n.get("node"))));
            }
        }
        return out;
    }

    private List<Integer> upNodes() throws Exception {
        List<Integer> up = new ArrayList<>();
        for (Map<String, Object> n : listOf(get("/api/cluster"))) {
            if (Boolean.TRUE.equals(n.get("up"))) up.add((int) num(n.get("id")));
        }
        Collections.sort(up);
        return up;
    }

    // =========================================================================
    // B1 / B2: stale reads
    // =========================================================================

    private void b1() throws Exception {
        header("B1 STRONG: " + ROUNDS + " rounds of write on node X, then an immediate read on all 5 nodes");
        int stale = 0;
        int notReady = 0;
        int stored = 0;
        for (int i = 1; i <= ROUNDS; i++) {
            int x = (i - 1) % 5 + 1;
            String key = "b1/" + run + "/k" + i;
            String value = "strong-" + i + "-" + System.nanoTime();
            Map<String, Object> w = write(x, key, value, "STRONG");
            strongWriteMs.add(num(w.get("latencyMs")));
            if ("STORED".equals(w.get("status"))) {
                stored++;
            }
            List<String> verdicts = new ArrayList<>();
            for (int n : ALL) {
                String v = (String) read(n, key).get("verdict");
                verdicts.add("N" + n + "=" + v);
                if ("STALE".equals(v)) stale++;
                if ("NOT_READY".equals(v)) notReady++;
            }
            line(String.format("round %2d: write via node %d %s in %d ms acked=%s | reads %s", i, x, w.get("status"),
                    num(w.get("latencyMs")), w.get("acked"), verdicts));
        }
        check("B1 STRONG read-after-write", stored == ROUNDS && stale == 0 && notReady == 0,
                stored + "/" + ROUNDS + " STORED; " + stale + " stale and " + notReady + " not-ready reads out of " + (ROUNDS * 5)
                        + "; median STRONG write " + median(strongWriteMs) + " ms");
    }

    private void b2() throws Exception {
        header("B2 EVENTUAL: " + ROUNDS + " rounds of write on node X, an immediate read on all 5, then time until all agree");
        int stale = 0;
        int accepted = 0;
        int converged = 0;
        List<Long> convergence = new ArrayList<>();
        for (int i = 1; i <= ROUNDS; i++) {
            int x = (i - 1) % 5 + 1;
            String key = "b2/" + run + "/k" + i;
            String value = "eventual-" + i + "-" + System.nanoTime();
            long t0 = System.currentTimeMillis();
            Map<String, Object> w = write(x, key, value, "EVENTUAL");
            eventualWriteMs.add(num(w.get("latencyMs")));
            if ("ACCEPTED".equals(w.get("status"))) {
                accepted++;
            }
            int roundStale = 0;
            for (int n : ALL) {
                if ("STALE".equals(read(n, key).get("verdict"))) {
                    roundStale++;
                }
            }
            stale += roundStale;
            long done = -1;
            long deadline = t0 + 10000;
            while (System.currentTimeMillis() < deadline) {
                boolean all = true;
                for (int n : ALL) {
                    if (!"FRESH".equals(read(n, key).get("verdict"))) {
                        all = false;
                        break;
                    }
                }
                if (all) {
                    done = System.currentTimeMillis() - t0;
                    break;
                }
                Thread.sleep(25);
            }
            if (done >= 0) {
                converged++;
                convergence.add(done);
            }
            line(String.format("round %2d: write via node %d %s in %d ms | stale immediate reads %d/5 | all 5 agree after %s ms",
                    i, x, w.get("status"), num(w.get("latencyMs")), roundStale, done < 0 ? "NEVER (10 s)" : String.valueOf(done)));
        }
        line("median time until all 5 replicas agree: " + median(convergence) + " ms (eventualLagMs is a simulated "
                + map(get("/api/blackboard?prefix=none")).get("eventualLagMs") + " ms)");
        check("B2 EVENTUAL stale then converges", accepted == ROUNDS && stale > 0 && converged == ROUNDS,
                accepted + "/" + ROUNDS + " ACCEPTED; " + stale + " of " + (ROUNDS * 5) + " immediate reads stale; "
                        + converged + "/" + ROUNDS + " converged, median " + median(convergence) + " ms, no operator action");
    }

    // =========================================================================
    // B3: quorum
    // =========================================================================

    private void b3() throws Exception {
        header("B3 quorum: STRONG succeeds with 3 of 5 live, is refused with 2 of 5, and the cluster recovers");
        ensureCluster();
        kill(1);
        kill(2);
        waitUp(List.of(3, 4, 5));
        Map<String, Object> sync = map(post("/api/clock/sync", "{}"));
        line("Berkeley round with nodes 1 and 2 down (leader coordinates, dead nodes skipped): nodeCount="
                + sync.get("nodeCount") + " skipped=" + sync.get("skipped") + " coordinator=" + sync.get("coordinator"));
        String keyA = "b3/" + run + "/three-live";
        Map<String, Object> a = write(3, keyA, "written with 3 live", "STRONG");
        line("killed 1, 2; STRONG write via node 3: " + a.get("status") + " liveAtCheck=" + a.get("liveAtCheck")
                + " acked=" + a.get("acked") + " in " + a.get("latencyMs") + " ms");
        List<String> aReads = new ArrayList<>();
        boolean aOk = "STORED".equals(a.get("status"));
        for (int n : List.of(3, 4, 5)) {
            String v = (String) read(n, keyA).get("verdict");
            aReads.add("N" + n + "=" + v);
            aOk &= "FRESH".equals(v);
        }
        line("  reads on live nodes: " + aReads);

        kill(3);
        waitUp(List.of(4, 5));
        String keyB = "b3/" + run + "/two-live";
        Map<String, Object> b = write(4, keyB, "must be refused", "STRONG");
        line("killed 3; STRONG write via node 4: " + b.get("status") + " liveAtCheck=" + b.get("liveAtCheck")
                + " message=\"" + b.get("message") + "\"");
        boolean bOk = "REFUSED".equals(b.get("status"));
        List<String> bReads = new ArrayList<>();
        for (int n : List.of(4, 5)) {
            Map<String, Object> r = read(n, keyB);
            bReads.add("N" + n + " found=" + r.get("found"));
            bOk &= Boolean.FALSE.equals(r.get("found"));
        }
        line("  reads on live nodes: " + bReads);

        restartParallel(List.of(1, 2, 3));
        Map<String, Object> st = waitReadyAll();
        line("restarted 1, 2, 3: " + (st == null ? "NOT all ready" : "all 5 up and ready, leader " + map(get("/api/election")).get("leaderId")));
        boolean recovered = st != null;
        List<String> recovery = new ArrayList<>();
        for (int n : ALL) {
            Map<String, Object> ra = read(n, keyA);
            Map<String, Object> rb = read(n, keyB);
            recovery.add("N" + n + ":" + ra.get("verdict") + "/" + (Boolean.TRUE.equals(rb.get("found")) ? "REFUSED-KEY-PRESENT" : "absent"));
            recovered &= "FRESH".equals(ra.get("verdict")) && Boolean.FALSE.equals(rb.get("found"));
        }
        String keyC = "b3/" + run + "/after-recovery";
        Map<String, Object> c = write(1, keyC, "after recovery", "STRONG");
        recovered &= "STORED".equals(c.get("status"));
        for (int n : ALL) {
            recovered &= "FRESH".equals(read(n, keyC).get("verdict"));
        }
        line("  after restart, three-live key / refused key per node: " + recovery);
        line("  new STRONG write via node 1: " + c.get("status") + " acked=" + c.get("acked"));
        check("B3 STRONG with 3 live", aOk, keyA + " " + a.get("status") + " on " + a.get("acked"));
        check("B3 STRONG refused with 2 live", bOk, keyB + " " + b.get("status") + "; held by no live node");
        check("B3 cluster recovers", recovered, "restarted nodes caught up on the 3-live write, the refused key is absent everywhere, new STRONG write STORED on all 5");
    }

    // =========================================================================
    // B4 / B5: restart catch-up
    // =========================================================================

    private void b4() throws Exception {
        header("B4 restart catch-up (STRONG): kill node 2, 5 STRONG writes, restart node 2: it holds all 5 before it reports ready");
        catchUp("STRONG", "b4");
    }

    private void b5() throws Exception {
        header("B5 restart catch-up (EVENTUAL): the same with EVENTUAL writes, converging without operator action");
        catchUp("EVENTUAL", "b5");
    }

    private void catchUp(String mode, String tag) throws Exception {
        ensureCluster();
        int target = 2;
        kill(target);
        waitUp(List.of(1, 3, 4, 5));
        String prefix = tag + "/" + run + "/";
        List<String> keys = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            String key = prefix + "k" + i;
            keys.add(key);
            Map<String, Object> w = write(4, key, mode.toLowerCase() + "-while-2-down-" + i, mode);
            line(String.format("write %s via node 4 while node %d is down: %s acked=%s", key, target, w.get("status"), w.get("acked")));
        }
        long restartAt = System.currentTimeMillis();
        post("/api/nodes/" + target + "/restart", "{}");
        int notReadyReads = 0;
        boolean everNotReady = false;
        Map<String, Object> firstReady = null;
        long readyAt = -1;
        long deadline = restartAt + STABLE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline && firstReady == null) {
            Map<String, Object> node = overviewNode(prefix, target);
            if (node != null && Boolean.TRUE.equals(node.get("reachable"))) {
                if (Boolean.TRUE.equals(node.get("ready"))) {
                    firstReady = node;
                    readyAt = System.currentTimeMillis() - restartAt;
                } else {
                    everNotReady = true;
                    try {
                        if ("NOT_READY".equals(read(target, keys.get(0)).get("verdict"))) {
                            notReadyReads++;
                        }
                    } catch (IllegalStateException ignored) {
                        // node not bound yet
                    }
                }
            }
            if (firstReady == null) {
                Thread.sleep(40);
            }
        }
        List<String> held = new ArrayList<>();
        if (firstReady != null) {
            for (Map<String, Object> e : listOf(firstReady.get("entries"))) {
                held.add((String) e.get("key"));
            }
        }
        int heldAtReady = 0;
        for (String k : keys) {
            if (held.contains(k)) heldAtReady++;
        }
        line(String.format("node %d restarted: first seen not ready=%s (%d reads answered NOT_READY), ready after %d ms, "
                        + "snapshotSource=%s, clockSynced=%s, holds %d/5 of the keys at its first ready snapshot",
                target, everNotReady, notReadyReads, readyAt, firstReady == null ? null : firstReady.get("snapshotSource"),
                firstReady == null ? null : firstReady.get("clockSynced"), heldAtReady));

        long convergedAt = -1;
        long convDeadline = System.currentTimeMillis() + 20000;
        while (System.currentTimeMillis() < convDeadline) {
            boolean all = true;
            for (String k : keys) {
                for (int n : ALL) {
                    if (!"FRESH".equals(read(n, k).get("verdict"))) {
                        all = false;
                        break;
                    }
                }
                if (!all) break;
            }
            if (all) {
                convergedAt = System.currentTimeMillis() - restartAt;
                break;
            }
            Thread.sleep(100);
        }
        line("all 5 keys FRESH on all 5 nodes " + (convergedAt < 0 ? "NEVER (20 s)" : convergedAt + " ms after the restart request"));
        List<Map<String, Object>> offsets = listOf(get("/api/clock/drift"));
        StringBuilder o = new StringBuilder();
        for (Map<String, Object> m : offsets) {
            o.append(" N").append(m.get("node")).append('=').append(m.get("offsetMs"));
        }
        line("clock offsets vs control plane after the restart (ms):" + o);

        if (mode.equals("STRONG")) {
            check("B4 STRONG restart catch-up", firstReady != null && heldAtReady == 5 && convergedAt >= 0,
                    "node " + target + " held " + heldAtReady + "/5 at its first ready snapshot (ready after " + readyAt
                            + " ms, " + notReadyReads + " NOT_READY reads before); all FRESH after " + convergedAt + " ms");
        } else {
            check("B5 EVENTUAL restart catch-up", firstReady != null && convergedAt >= 0,
                    "node " + target + " ready after " + readyAt + " ms holding " + heldAtReady
                            + "/5; all 5 keys FRESH on all 5 nodes after " + convergedAt + " ms without operator action");
        }
    }

    // =========================================================================
    // B6: merge function
    // =========================================================================

    private void b6() {
        header("B6 merge function (in-process): higher timestamp wins, equal timestamps go to the higher writer, order-independent");
        List<String> problems = new ArrayList<>();
        BlackboardRecord older = new BlackboardRecord("k", "older", 100, 5, 1);
        BlackboardRecord newer = new BlackboardRecord("k", "newer", 200, 1, 1);
        BlackboardRecord tieLow = new BlackboardRecord("k", "tie-writer-2", 300, 2, 1);
        BlackboardRecord tieHigh = new BlackboardRecord("k", "tie-writer-4", 300, 4, 1);
        expect(problems, "merge(older ts100 w5, newer ts200 w1)", BlackboardRecord.merge(older, newer), newer);
        expect(problems, "merge(newer, older)", BlackboardRecord.merge(newer, older), newer);
        expect(problems, "merge(tie ts300 w2, tie ts300 w4)", BlackboardRecord.merge(tieLow, tieHigh), tieHigh);
        expect(problems, "merge(tie w4, tie w2)", BlackboardRecord.merge(tieHigh, tieLow), tieHigh);
        expect(problems, "merge(null, older)", BlackboardRecord.merge(null, older), older);
        expect(problems, "merge(older, null)", BlackboardRecord.merge(older, null), older);
        List<BlackboardRecord> all = List.of(older, newer, tieLow, tieHigh);
        int perms = 0;
        int agree = 0;
        for (List<BlackboardRecord> order : permutations(all)) {
            BlackboardRecord acc = null;
            for (BlackboardRecord r : order) {
                acc = BlackboardRecord.merge(acc, r);
            }
            perms++;
            if (acc == tieHigh) agree++;
        }
        line("all " + perms + " arrival orders of the 4 records end at: " + (agree == perms ? tieHigh.getValue() : "DIFFERENT RESULTS (" + agree + "/" + perms + ")"));
        if (agree != perms) problems.add("arrival order changed the result");
        check("B6 merge function", problems.isEmpty(), perms + " arrival orders agree; problems=" + problems);
    }

    private void expect(List<String> problems, String what, BlackboardRecord got, BlackboardRecord want) {
        boolean ok = got == want;
        line(String.format("  %-38s -> %s (ts %d, writer %d) %s", what, got.getValue(), got.getTimestamp(), got.getWriterNodeId(), ok ? "ok" : "WRONG"));
        if (!ok) problems.add(what);
    }

    private static List<List<BlackboardRecord>> permutations(List<BlackboardRecord> items) {
        if (items.isEmpty()) {
            List<List<BlackboardRecord>> base = new ArrayList<>();
            base.add(new ArrayList<>());
            return base;
        }
        List<List<BlackboardRecord>> out = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            List<BlackboardRecord> rest = new ArrayList<>(items);
            BlackboardRecord first = rest.remove(i);
            for (List<BlackboardRecord> p : permutations(rest)) {
                p.add(0, first);
                out.add(p);
            }
        }
        return out;
    }

    // =========================================================================
    // B7: clock-skew anomaly
    // =========================================================================

    private void b7() throws Exception {
        header("B7 clock-skew anomaly: LWW with skewed clocks keeps the earlier write; a Berkeley round fixes it");
        Map<String, Object> autoBefore = map(get("/api/clock/auto"));
        line("auto-sync before: " + autoBefore);
        try {
            b7Body();
        } finally {
            String restored = post("/api/clock/auto", "{\"enabled\":" + autoBefore.get("enabled") + "}");
            line("auto-sync restored to its previous setting: " + restored);
        }
    }

    private void b7Body() throws Exception {
        line("auto-sync off: " + post("/api/clock/auto", "{\"enabled\":false}"));
        restartParallel(ALL);
        Map<String, Object> st = waitReadyAll();
        line("restarted all 5 nodes so they come back with their configured drift: " + (st == null ? "NOT all ready" : "all ready"));
        if (st == null) {
            // Nothing is written unless all 5 replicas are ready.
            check("B7 prerequisite: all 5 nodes ready after the restart", false,
                    "not all 5 up and ready within " + STABLE_TIMEOUT_MS + " ms; nothing written; nodes not ready " + notReady());
            return;
        }
        Map<Integer, Long> offsets = new TreeMap<>();
        boolean allAtDrift = true;
        for (Map<String, Object> m : listOf(get("/api/clock/drift"))) {
            Long off = m.get("offsetMs") == null ? null : num(m.get("offsetMs"));
            long drift = num(m.get("configuredDriftMs"));
            boolean near = off != null && Math.abs(off - drift) <= 100;
            allAtDrift &= near;
            offsets.put((int) num(m.get("node")), off);
            line(String.format("  node %s offset %s ms (configured drift %s ms) %s", m.get("node"), off, drift,
                    near ? "within 100 ms" : "NOT within 100 ms"));
        }
        allAtDrift &= offsets.size() == 5;
        Long o1 = offsets.get(1);
        Long o2 = offsets.get(2);
        // Skew counts as present only if every node runs at its configured drift.
        boolean skew = allAtDrift && o1 != null && o2 != null && o1 - o2 >= 4000;
        line("all 5 offsets within 100 ms of configured drift: " + allAtDrift + "; skew node1 - node2 = "
                + (o1 == null || o2 == null ? "?" : (o1 - o2)) + " ms -> " + (skew ? "skew present" : "skew NOT present"));

        String k1 = "lww/" + run + "/before-sync";
        String[] r1 = conflict(k1);
        Map<String, Object> sync = map(post("/api/clock/sync", "{}"));
        line("Berkeley round: spreadBefore=" + sync.get("spreadBefore") + " ms spreadAfter=" + sync.get("spreadAfter")
                + " ms nodeCount=" + sync.get("nodeCount") + " coordinator=" + sync.get("coordinator") + " corrections=" + sync.get("corrections"));
        long o1After = 0;
        for (Map<String, Object> m : listOf(get("/api/clock/drift"))) {
            if (num(m.get("node")) == 1 && m.get("offsetMs") != null) o1After = num(m.get("offsetMs"));
        }
        long tsA = Long.parseLong(r1[1]);
        long wait = Math.max(0, tsA - o1After - System.currentTimeMillis()) + 300;
        line("node 1 stamps monotonically, so its next stamp is max(corrected now, " + tsA + " + 1); its corrected clock is now "
                + o1After + " ms ahead of the control plane, so waiting " + wait + " ms for it to pass its last stamp");
        Thread.sleep(wait);
        String k2 = "lww/" + run + "/after-sync";
        String[] r2 = conflict(k2);

        boolean anomaly = "A".equals(r1[0]);
        boolean fixed = "B".equals(r2[0]);
        String summary = "before sync the replicas kept " + r1[0] + " (A ts " + r1[1] + " > B ts " + r1[2]
                + " although B was written later); after sync they kept " + r2[0] + " (A ts " + r2[1] + ", B ts " + r2[2] + ")";
        if (!skew) {
            info.put("B7 clock-skew anomaly", "skew not reproduced (all offsets within 100 ms of configured drift: " + allAtDrift
                    + "; node1 - node2 = " + (o1 == null || o2 == null ? "?" : (o1 - o2)) + " ms); " + summary);
            line("INFO B7: skew not reproduced; not reported as PASS");
        } else {
            check("B7 clock-skew anomaly reproduced then fixed", anomaly && fixed, summary);
        }
    }

    /** EVENTUAL write A via node 1, ~200 ms later B via node 2; returns {winner, tsA, tsB}. */
    private String[] conflict(String key) throws Exception {
        Map<String, Object> a = write(1, key, "A", "EVENTUAL");
        Thread.sleep(200);
        Map<String, Object> b = write(2, key, "B", "EVENTUAL");
        line(String.format("  %s: A via node 1 ts=%s | 200 ms later B via node 2 ts=%s", key, a.get("timestamp"), b.get("timestamp")));
        String winner = null;
        long deadline = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < deadline) {
            List<String> values = new ArrayList<>();
            for (int n : ALL) {
                values.add(String.valueOf(read(n, key).get("value")));
            }
            if (values.stream().distinct().count() == 1 && !"null".equals(values.get(0))) {
                winner = values.get(0);
                break;
            }
            Thread.sleep(50);
        }
        line("  all 5 replicas converged to: " + winner);
        return new String[] {String.valueOf(winner), String.valueOf(num(a.get("timestamp"))), String.valueOf(num(b.get("timestamp")))};
    }

    // =========================================================================
    // B8 / B9 / B10: jobs
    // =========================================================================

    private void b8() throws Exception {
        header("B8 job integration: a STRONG and an EVENTUAL job; every finding and the answer on all replicas");
        ensureCluster();
        for (String mode : List.of("STRONG", "EVENTUAL")) {
            long seq = lastSeq();
            Map<String, Object> job = runJob(mode, "WEIGHTED");
            long completedAt = System.currentTimeMillis();
            String jobId = (String) job.get("jobId");
            Map<String, Object> bb = map(job.get("blackboard"));
            line(mode + " job " + jobId + ": " + job.get("status") + " makespan " + job.get("makespanMs") + " ms; blackboard " + bb);
            List<Map<String, Object>> expected = new ArrayList<>();
            for (Map<String, Object> stage : listOf(job.get("stages"))) {
                for (Map<String, Object> s : listOf(stage.get("subtasks"))) {
                    if (s.get("blackboard") != null) {
                        Map<String, Object> e = map(s.get("blackboard"));
                        e.put("stage", stage.get("type"));
                        e.put("node", s.get("node"));
                        expected.add(e);
                    }
                }
            }
            int summarizeCount = 0;
            for (Map<String, Object> stage : listOf(job.get("stages"))) {
                if ("SUMMARIZE".equals(stage.get("type"))) summarizeCount = listOf(stage.get("subtasks")).size();
            }
            // Present on every replica: STRONG at once, EVENTUAL after convergence.
            long convergedMs = -1;
            int firstPass = -1;
            long deadline = System.currentTimeMillis() + 20000;
            while (System.currentTimeMillis() < deadline) {
                int ok = 0;
                for (Map<String, Object> e : expected) {
                    boolean allNodes = true;
                    for (int n : ALL) {
                        Map<String, Object> r = read(n, (String) e.get("key"));
                        if (!Boolean.TRUE.equals(r.get("found")) || num(r.get("timestamp")) != num(e.get("timestamp"))
                                || num(r.get("writer")) != num(e.get("writer"))) {
                            allNodes = false;
                            break;
                        }
                    }
                    if (allNodes) ok++;
                }
                if (firstPass < 0) firstPass = ok;
                if (ok == expected.size()) {
                    convergedMs = System.currentTimeMillis() - completedAt;
                    break;
                }
                Thread.sleep(100);
            }
            Map<String, Object> answer = read(3, "job/" + jobId + "/answer");
            boolean answerMatches = String.valueOf(job.get("answer")).equals(answer.get("value"));
            boolean writersAreWorkers = true;
            for (Map<String, Object> e : expected) {
                writersAreWorkers &= num(e.get("writer")) == num(e.get("node"));
            }
            Thread.sleep(1500);  // let the monitor pull the leader's summary event
            int summaryEvents = 0;
            int perWriteJobEvents = 0;
            for (Map<String, Object> ev : listOf(get("/api/events?since=" + seq))) {
                Map<String, Object> f = map(ev.get("fields"));
                String type = String.valueOf(ev.get("type"));
                if ("BLACKBOARD_JOB_FINDINGS".equals(type) && f != null && jobId.equals(f.get("jobId"))) summaryEvents++;
                if ((type.equals("BLACKBOARD_WRITE") || type.equals("BLACKBOARD_APPLIED") || type.equals("BLACKBOARD_REFUSED"))
                        && f != null && String.valueOf(f.get("key")).startsWith("job/")) perWriteJobEvents++;
            }
            int verbatim = answerSentencesFromCorpus(String.valueOf(job.get("answer")));
            line(String.format("  %d SUMMARIZE findings + answer = %d keys; all stored=%s; on all 5 replicas: %d/%d at the first full check "
                            + "(%d sequential reads), all present when a check that finished %d ms after COMPLETE ran; "
                            + "writer of each finding = the worker that ran it: %s; blackboard answer equals job answer: %s; "
                            + "BLACKBOARD_JOB_FINDINGS events for this job: %d; per-write events for job keys: %d; answer sentences verbatim in cited docs: %d",
                    summarizeCount, expected.size(), num(bb.get("stored")) == expected.size(), firstPass, expected.size(),
                    expected.size() * 5, convergedMs,
                    writersAreWorkers, answerMatches, summaryEvents, perWriteJobEvents, verbatim));
            boolean ok = "COMPLETE".equals(job.get("status")) && expected.size() == summarizeCount + 1
                    && num(bb.get("stored")) == expected.size() && convergedMs >= 0 && answerMatches && writersAreWorkers
                    && summaryEvents == 1 && perWriteJobEvents == 0 && verbatim > 0;
            if (mode.equals("STRONG")) {
                ok &= firstPass == expected.size();
            }
            check("B8 " + mode + " job findings on all replicas", ok, jobId + " " + job.get("status") + ": " + expected.size()
                    + " keys stored" + (mode.equals("STRONG") ? ", all on all 5 at the first check"
                            : ", all on all 5 by a check finishing " + convergedMs + " ms after COMPLETE")
                    + "; 1 summary event, 0 per-write events");
        }
    }

    private void b9() throws Exception {
        header("B9 kill the leader mid-job (STRONG findings): findings posted before the kill stay readable on the survivors");
        ensureCluster();
        int leader = (int) num(map(get("/api/election")).get("leaderId"));
        Map<String, Object> resp = map(post("/api/jobs", "{\"query\":\"" + QUERY + "\",\"policy\":\"ROUND_ROBIN\",\"consistency\":\"STRONG\"}"));
        String jobId = (String) resp.get("jobId");
        Map<String, Object> job = null;
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            job = map(get("/api/jobs/" + jobId));
            if (!"RUNNING".equals(job.get("status")) && !"QUEUED".equals(job.get("status"))) break;
            if (storedFindings(job).size() >= 1) break;
            Thread.sleep(20);
        }
        line("job " + jobId + " on leader " + leader + ": " + job.get("status") + ", " + storedFindings(job).size() + " findings stored so far; killing the leader");
        post("/api/nodes/" + leader + "/kill", "{}");
        Map<String, Object> orphan = null;
        deadline = System.currentTimeMillis() + STABLE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            orphan = map(get("/api/jobs/" + jobId));
            Map<String, Object> el = map(get("/api/election"));
            if ("ORPHANED".equals(orphan.get("status")) && Boolean.TRUE.equals(el.get("agreed"))
                    && el.get("leaderId") != null && num(el.get("leaderId")) != leader) break;
            Thread.sleep(100);
        }
        Map<String, Object> el = map(get("/api/election"));
        long newLeader = num(el.get("leaderId"));
        List<Map<String, Object>> findings = storedFindings(orphan);
        line("after the kill: job " + orphan.get("status") + ", new leader " + newLeader + ", " + findings.size()
                + " findings the job view lists as stored before the kill");
        List<Integer> survivors = new ArrayList<>(ALL);
        survivors.remove(Integer.valueOf(leader));
        int readable = 0;
        for (Map<String, Object> f : findings) {
            List<String> seen = new ArrayList<>();
            boolean all = true;
            for (int n : survivors) {
                Map<String, Object> r = read(n, (String) f.get("key"));
                boolean ok = Boolean.TRUE.equals(r.get("found")) && num(r.get("timestamp")) == num(f.get("timestamp"));
                seen.add("N" + n + "=" + (ok ? "yes" : "NO"));
                all &= ok;
            }
            if (all) readable++;
            line("  " + f.get("key") + " (writer node " + f.get("writer") + "): " + seen);
        }
        boolean ok = "ORPHANED".equals(orphan.get("status")) && newLeader > 0 && newLeader != leader
                && !findings.isEmpty() && readable == findings.size();
        check("B9 findings survive the leader", ok, readable + "/" + findings.size() + " findings posted before the kill readable on all "
                + survivors.size() + " survivors under new leader " + newLeader);
        post("/api/nodes/" + leader + "/restart", "{}");
        waitReadyAll();
    }

    private List<Map<String, Object>> storedFindings(Map<String, Object> job) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> stage : listOf(job.get("stages"))) {
            if (!"SUMMARIZE".equals(stage.get("type"))) continue;
            for (Map<String, Object> s : listOf(stage.get("subtasks"))) {
                Map<String, Object> bb = map(s.get("blackboard"));
                if ("COMPLETE".equals(s.get("status")) && bb != null && Boolean.TRUE.equals(bb.get("stored"))) {
                    out.add(bb);
                }
            }
        }
        return out;
    }

    private void b10() throws Exception {
        header("B10 cost: STRONG vs EVENTUAL write latency and job makespan (3 jobs each, interleaved)");
        ensureCluster();
        Map<String, List<Long>> makespan = new LinkedHashMap<>();
        Map<String, List<Long>> jobWrite = new LinkedHashMap<>();
        for (String m : List.of("STRONG", "EVENTUAL")) {
            makespan.put(m, new ArrayList<>());
            jobWrite.put(m, new ArrayList<>());
        }
        for (int i = 1; i <= 3; i++) {
            for (String mode : List.of("STRONG", "EVENTUAL")) {
                Map<String, Object> job = runJob(mode, "WEIGHTED");
                Map<String, Object> bb = map(job.get("blackboard"));
                makespan.get(mode).add(num(job.get("makespanMs")));
                jobWrite.get(mode).add(num(bb.get("medianLatencyMs")));
                line(String.format("%-8s run %d: %s makespan %d ms, median finding write %s ms, %s", mode, i, job.get("status"),
                        num(job.get("makespanMs")), bb.get("medianLatencyMs"), bb.get("byStatus")));
            }
        }
        Map<String, Object> metrics = map(get("/api/blackboard/metrics"));
        line("direct writes in B1/B2: median STRONG " + median(strongWriteMs) + " ms, median EVENTUAL " + median(eventualWriteMs) + " ms");
        line("cluster metrics: STRONG " + metrics.get("strong") + "; EVENTUAL " + metrics.get("eventual") + "; reads " + metrics.get("reads"));
        info.put("B10 write latency", "median direct write STRONG " + median(strongWriteMs) + " ms vs EVENTUAL " + median(eventualWriteMs)
                + " ms; median finding write in jobs STRONG " + median(jobWrite.get("STRONG")) + " ms vs EVENTUAL " + median(jobWrite.get("EVENTUAL")) + " ms");
        info.put("B10 job makespan", "median STRONG " + median(makespan.get("STRONG")) + " ms " + makespan.get("STRONG")
                + " vs EVENTUAL " + median(makespan.get("EVENTUAL")) + " ms " + makespan.get("EVENTUAL"));
    }

    // =========================================================================
    // B11: event volume
    // =========================================================================

    private void b11() throws Exception {
        header("B11 event volume: election and leader events from this run are still in the event log");
        List<Map<String, Object>> events = listOf(get("/api/events?since=" + startSeq));
        Map<String, Integer> byType = new TreeMap<>();
        for (Map<String, Object> e : events) {
            byType.merge(String.valueOf(e.get("type")), 1, Integer::sum);
        }
        line("events since the verifier started: " + events.size() + " (log capacity 5000)");
        line("by type: " + byType);
        List<String> required = List.of("NODE_KILLED", "LEADER_LOST", "ELECTION_STARTED", "LEADER_ACCEPTED", "CLOCK_SYNC",
                "BLACKBOARD_REFUSED", "BLACKBOARD_SYNC", "LWW_CONFLICT_RESOLVED", "BLACKBOARD_JOB_FINDINGS");
        List<String> missing = new ArrayList<>();
        for (String r : required) {
            if (!byType.containsKey(r)) missing.add(r);
        }
        long first = events.isEmpty() ? -1 : num(events.get(0).get("seq"));
        line("first event still held: seq " + first + " (verifier started after seq " + startSeq + ")");
        check("B11 election/leader events retained", missing.isEmpty() && first == startSeq + 1,
                events.size() + " events since start, none evicted; present: " + required + (missing.isEmpty() ? "" : "; MISSING " + missing));
        line("ElectionVerifier and JobVerifier are run separately after this verifier (see the report).");
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private int answerSentencesFromCorpus(String answer) {
        Corpus corpus = Corpus.load();
        Matcher m = Pattern.compile("(.+?) \\[(doc-\\d+)\\]\\s*").matcher(answer);
        int found = 0;
        int total = 0;
        while (m.find()) {
            total++;
            Corpus.Doc d = corpus.get(m.group(2));
            if (d != null && d.getSentences().contains(m.group(1).trim())) found++;
        }
        return found == total ? found : -found;
    }

    private Map<String, Object> runJob(String consistency, String policy) throws Exception {
        Map<String, Object> resp = map(post("/api/jobs", "{\"query\":\"" + QUERY + "\",\"policy\":\"" + policy
                + "\",\"consistency\":\"" + consistency + "\"}"));
        String jobId = (String) resp.get("jobId");
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
        Map<String, Object> job;
        do {
            Thread.sleep(100);
            job = map(get("/api/jobs/" + jobId));
        } while (("RUNNING".equals(job.get("status")) || "QUEUED".equals(job.get("status"))) && System.currentTimeMillis() < deadline);
        return job;
    }

    private Map<String, Object> write(int node, String key, String value, String mode) throws Exception {
        return map(post("/api/blackboard/write", "{\"node\":" + node + ",\"key\":\"" + key + "\",\"value\":\"" + value
                + "\",\"mode\":\"" + mode + "\"}"));
    }

    private Map<String, Object> read(int node, String key) throws Exception {
        return map(get("/api/blackboard/read?node=" + node + "&key=" + URLEncoder.encode(key, StandardCharsets.UTF_8)));
    }

    private Map<String, Object> overviewNode(String prefix, int node) throws Exception {
        Map<String, Object> o = map(get("/api/blackboard?prefix=" + URLEncoder.encode(prefix, StandardCharsets.UTF_8)));
        for (Map<String, Object> n : listOf(o.get("nodes"))) {
            if (num(n.get("node")) == node) return n;
        }
        return null;
    }

    private void kill(int node) throws Exception {
        post("/api/nodes/" + node + "/kill", "{}");
    }

    private void restartParallel(List<Integer> nodes) throws Exception {
        CountDownLatch done = new CountDownLatch(nodes.size());
        for (int n : nodes) {
            new Thread(() -> {
                try {
                    post("/api/nodes/" + n + "/restart", "{}");
                } catch (Exception e) {
                    line("restart " + n + " failed: " + e.getMessage());
                } finally {
                    done.countDown();
                }
            }).start();
        }
        done.await();
    }

    /** Waits until exactly these nodes are UP (the others DOWN) in /api/cluster. */
    private void waitUp(List<Integer> nodes) throws Exception {
        long deadline = System.currentTimeMillis() + STABLE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            List<Integer> up = new ArrayList<>();
            for (Map<String, Object> n : listOf(get("/api/cluster"))) {
                if (Boolean.TRUE.equals(n.get("up"))) up.add((int) num(n.get("id")));
            }
            Collections.sort(up);
            if (up.equals(nodes)) return;
            Thread.sleep(100);
        }
        line("WARNING: UP nodes never became " + nodes);
    }

    /** All 5 up, an agreed leader, and every replica ready. Returns /api/blackboard or null. */
    private Map<String, Object> waitReadyAll() throws Exception {
        return waitReadyAll(STABLE_TIMEOUT_MS);
    }

    private Map<String, Object> waitReadyAll(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> el = map(get("/api/election"));
            Map<String, Object> o = map(get("/api/blackboard?prefix=none-" + run));
            boolean ok = Boolean.TRUE.equals(el.get("agreed"));
            int ready = 0;
            for (Map<String, Object> n : listOf(o.get("nodes"))) {
                if (Boolean.TRUE.equals(n.get("up")) && Boolean.TRUE.equals(n.get("ready"))) ready++;
            }
            if (ok && ready == 5) return o;
            Thread.sleep(150);
        }
        return null;
    }

    private void ensureCluster() throws Exception {
        for (Map<String, Object> n : listOf(get("/api/cluster"))) {
            if (!Boolean.TRUE.equals(n.get("up"))) {
                post("/api/nodes/" + num(n.get("id")) + "/restart", "{}");
            }
        }
        Map<String, Object> o = waitReadyAll();
        Map<String, Object> el = map(get("/api/election"));
        line("cluster: " + (o == null ? "NOT all 5 ready" : "5 nodes up and ready") + ", agreed leader " + el.get("leaderId"));
    }

    private long lastSeq() throws Exception {
        long max = 0;
        for (Map<String, Object> e : listOf(get("/api/events?since=0"))) {
            max = Math.max(max, num(e.get("seq")));
        }
        return max;
    }

    private Object get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path)).timeout(Duration.ofSeconds(15)).GET().build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) {
            throw new IllegalStateException("GET " + path + " -> " + res.statusCode() + " " + res.body());
        }
        return JsonUtil.parse(res.body());
    }

    private String post(String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) {
            throw new IllegalStateException("POST " + path + " -> " + res.statusCode() + " " + res.body());
        }
        return res.body();
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

    private static String median(List<Long> values) {
        List<Long> s = new ArrayList<>();
        for (Long v : values) {
            if (v != null && v >= 0) s.add(v);
        }
        if (s.isEmpty()) return "n/a";
        Collections.sort(s);
        return String.valueOf(s.get(s.size() / 2));
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
