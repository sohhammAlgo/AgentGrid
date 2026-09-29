package agentgrid.control;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

/**
 * End-to-end verifier for Experiment 4 in the live cluster.
 *
 * Drives the control plane's HTTP API only (the control plane must already be running on
 * 127.0.0.1:8080), prints the raw numbers of every step, and ends with a PASS/FAIL table.
 * Exits non-zero if any check fails.
 *
 * Usage (from backend/):  java -cp build/classes agentgrid.control.ElectionVerifier
 */
public class ElectionVerifier {

    private static final String BASE = System.getProperty("agentgrid.verifier.base", "http://127.0.0.1:8080");
    private static final int RUNS = 5;
    private static final int SPLIT_RUNS = 10;
    private static final long STABLE_TIMEOUT_MS = 20000;
    /** How long the cluster must stay unchanged before a run starts. */
    private static final long SETTLE_MS = 1500;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final Map<String, String> results = new LinkedHashMap<>();
    private final Map<String, Boolean> passed = new LinkedHashMap<>();
    private final Map<String, String> info = new LinkedHashMap<>();
    private final List<List<Map<String, Object>>> killEpisodes = new ArrayList<>();
    private final List<Long> killLamports = new ArrayList<>();
    private final List<Integer> killedNodes = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        ElectionVerifier v = new ElectionVerifier();
        boolean ok = v.run();
        System.exit(ok ? 0 : 1);
    }

    private boolean run() throws Exception {
        line("=====================================================================");
        line(" AgentGrid-Lite Experiment 4 verifier (HTTP API only) -> " + BASE);
        line("=====================================================================");
        List<Map<String, Object>> modules = listOf(get("/api/modules"));
        line("modules: " + modules);

        v1ColdStart();
        Map<String, List<Map<String, Object>>> restarts = new LinkedHashMap<>();
        for (String alg : List.of("BULLY", "RING")) {
            restarts.put(alg, v2KillLeader(alg));
        }
        v3Restart(restarts);
        for (String alg : List.of("BULLY", "RING")) {
            v4SplitBrain(alg);
        }
        v5CascadedKill("BULLY", true);
        // Not part of the spec's V5: a Ring election lasts tens of ms, shorter than the ~200 ms
        // it takes the control plane to observe it, so an HTTP-driven kill cannot reliably land
        // inside it. Reported for information only; it does not affect the exit code.
        v5CascadedKill("RING", false);
        v6Lamport();

        line("");
        line("=====================================================================");
        line(" RESULT");
        line("=====================================================================");
        boolean all = true;
        for (Map.Entry<String, Boolean> e : passed.entrySet()) {
            System.out.printf("%-4s  %-26s %s%n", e.getValue() ? "PASS" : "FAIL", e.getKey(), results.get(e.getKey()));
            all &= e.getValue();
        }
        for (Map.Entry<String, String> e : info.entrySet()) {
            System.out.printf("%-4s  %-26s %s%n", "INFO", e.getKey(), e.getValue());
        }
        line("OVERALL: " + (all ? "PASS" : "FAIL"));
        return all;
    }

    // =========================================================================
    // V1: cold start
    // =========================================================================

    private void v1ColdStart() throws Exception {
        header("V1 cold start (BULLY): all 5 nodes report leader 5 within 5 s");
        for (int id = 1; id <= 5; id++) {
            post("/api/nodes/" + id + "/kill", "{}");
        }
        waitUntil(() -> upNodes().isEmpty(), 10000);
        line("all nodes down: " + upNodes().isEmpty());
        Map<String, Object> alg = map(post("/api/election/algorithm", "{\"name\":\"BULLY\"}"));
        line("set algorithm (no node up): " + alg);

        long t0 = System.currentTimeMillis();
        CountDownLatch done = new CountDownLatch(5);
        for (int id = 1; id <= 5; id++) {
            int node = id;
            new Thread(() -> {
                try {
                    post("/api/nodes/" + node + "/restart", "{}");
                } catch (Exception e) {
                    line("restart " + node + " failed: " + e.getMessage());
                } finally {
                    done.countDown();
                }
            }).start();
        }
        long allUpMs = -1;
        long agreedMs = -1;
        Map<String, Object> el = null;
        while (System.currentTimeMillis() - t0 < 15000) {
            List<Integer> up = upNodes();
            el = election();
            long t = System.currentTimeMillis() - t0;
            if (allUpMs < 0 && up.size() == 5) {
                allUpMs = t;
            }
            if (up.size() == 5 && Boolean.TRUE.equals(el.get("agreed")) && num(el.get("leaderId")) == 5
                    && allViewsAre(el, 5, Set.of(1, 2, 3, 4, 5))) {
                agreedMs = t;
                break;
            }
            Thread.sleep(50);
        }
        done.await();
        line("restart requests sent at t=0; all 5 UP at t=" + allUpMs + " ms; all 5 views = 5 and agreed at t="
                + agreedMs + " ms");
        line("views: " + (el == null ? null : el.get("views")) + " agreed=" + (el == null ? null : el.get("agreed")));
        boolean ok = agreedMs >= 0 && agreedMs <= 5000;
        check("V1 cold start BULLY", ok, "all agree on 5 at " + agreedMs + " ms (limit 5000 ms), all UP at "
                + allUpMs + " ms");
        waitStable(5, 5, STABLE_TIMEOUT_MS, 0);
    }

    // =========================================================================
    // V2: kill leader, 5 runs per algorithm
    // =========================================================================

    private List<Map<String, Object>> v2KillLeader(String alg) throws Exception {
        header("V2 kill leader x" + RUNS + " (" + alg + ")");
        ensureCluster(alg);
        List<Long> detection = new ArrayList<>();
        List<Long> convergence = new ArrayList<>();
        List<Long> messages = new ArrayList<>();
        List<Map<String, Object>> restartEpisodes = new ArrayList<>();
        int good = 0;

        for (int run = 1; run <= RUNS; run++) {
            long seqBefore = lastSeq();
            post("/api/nodes/5/kill", "{}");
            Map<String, Object> killEv = findEvent(seqBefore, "NODE_KILLED", 5);
            long killTrue = num(killEv.get("trueMs"));
            Map<String, Object> st = waitStable(4, 4, STABLE_TIMEOUT_MS, killTrue);
            List<Map<String, Object>> evs = eventsSince(seqBefore);

            Long lostTrue = null;
            Integer lostBy = null;
            for (Map<String, Object> e : evs) {
                if ("LEADER_LOST".equals(e.get("type")) && num(e.get("trueMs")) >= killTrue) {
                    if (lostTrue == null || num(e.get("trueMs")) < lostTrue) {
                        lostTrue = num(e.get("trueMs"));
                        lostBy = (int) num(e.get("node"));
                    }
                }
            }
            if (st == null) {
                line(String.format("run %d: NOT STABLE within %d ms; election=%s", run, STABLE_TIMEOUT_MS, election()));
            } else {
                Map<String, Object> ep = map(st.get("lastEpisode"));
                long det = lostTrue == null ? -1 : lostTrue - killTrue;
                long conv = num(ep.get("convergenceMs"));
                long msgs = num(ep.get("messages"));
                boolean ok = num(st.get("leaderId")) == 4 && num(st.get("backupId")) == 3
                        && Boolean.TRUE.equals(st.get("agreed")) && allViewsAre(st, 4, Set.of(1, 2, 3, 4))
                        && lostTrue != null;
                line(String.format("run %d: detection=%d ms (first LEADER_LOST by node %s), convergence=%d ms, "
                                + "messages=%d byKind=%s initiators=%s | leader=%s backup=%s agreed=%s views=%s -> %s",
                        run, det, lostBy, conv, msgs, ep.get("byKind"), ep.get("initiators"), st.get("leaderId"),
                        st.get("backupId"), st.get("agreed"), st.get("views"), ok ? "ok" : "WRONG"));
                if (ok) {
                    good++;
                    detection.add(det);
                    convergence.add(conv);
                    messages.add(msgs);
                }
                killEpisodes.add(evs);
                killLamports.add(num(killEv.get("lamport")));
                killedNodes.add(5);
            }

            // Restart the killed node and wait for a stable state (node 5 reclaims leadership).
            long restartTrue = System.currentTimeMillis();
            post("/api/nodes/5/restart", "{}");
            Map<String, Object> back = waitStable(5, 5, STABLE_TIMEOUT_MS, restartTrue);
            Map<String, Object> rec = new LinkedHashMap<>();
            rec.put("run", run);
            rec.put("state", back);
            restartEpisodes.add(rec);
            if (back == null) {
                line("  restart of node 5 did not reach a stable state; election=" + election());
                ensureCluster(alg);
            } else {
                waitSettled(5, 5);
            }
        }
        line(String.format("%s medians over %d good runs: detection=%s ms, convergence=%s ms, messages=%s",
                alg, good, median(detection), median(convergence), median(messages)));
        check("V2 kill leader " + alg, good == RUNS, good + "/" + RUNS + " runs -> leader 4, backup 3, agreed; "
                + "median detection " + median(detection) + " ms, convergence " + median(convergence)
                + " ms, messages " + median(messages));
        return restartEpisodes;
    }

    // =========================================================================
    // V3: restarted node 5 reclaims leadership (the restarts between V2 runs)
    // =========================================================================

    private void v3Restart(Map<String, List<Map<String, Object>>> restarts) {
        header("V3 restart node 5: it reclaims leadership, all 5 agree");
        for (Map.Entry<String, List<Map<String, Object>>> e : restarts.entrySet()) {
            int good = 0;
            List<Long> conv = new ArrayList<>();
            for (Map<String, Object> rec : e.getValue()) {
                Map<String, Object> st = map(rec.get("state"));
                if (st == null) {
                    line(e.getKey() + " restart after run " + rec.get("run") + ": NOT STABLE");
                    continue;
                }
                Map<String, Object> ep = map(st.get("lastEpisode"));
                boolean ok = num(st.get("leaderId")) == 5 && Boolean.TRUE.equals(st.get("agreed"))
                        && allViewsAre(st, 5, Set.of(1, 2, 3, 4, 5));
                line(String.format("%s restart after run %s: leader=%s agreed=%s views=%s convergence=%s ms "
                                + "messages=%s byKind=%s initiators=%s -> %s",
                        e.getKey(), rec.get("run"), st.get("leaderId"), st.get("agreed"), st.get("views"),
                        ep.get("convergenceMs"), ep.get("messages"), ep.get("byKind"), ep.get("initiators"),
                        ok ? "ok" : "WRONG"));
                if (ok) {
                    good++;
                    conv.add(num(ep.get("convergenceMs")));
                }
            }
            check("V3 restart node 5 " + e.getKey(), good == e.getValue().size() && good > 0,
                    good + "/" + e.getValue().size() + " restarts -> node 5 leader, all 5 agree; median convergence "
                            + median(conv) + " ms");
        }
    }

    // =========================================================================
    // V4: simultaneous elections from nodes 1 and 2
    // =========================================================================

    private void v4SplitBrain(String alg) throws Exception {
        header("V4 split-brain (" + alg + "): start elections on nodes 1 and 2 simultaneously, x" + SPLIT_RUNS);
        ensureCluster(alg);
        int good = 0;
        for (int run = 1; run <= SPLIT_RUNS; run++) {
            long seqBefore = lastSeq();
            long t0 = System.currentTimeMillis();
            CountDownLatch go = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(2);
            String[] replies = new String[2];
            for (int i = 0; i < 2; i++) {
                int idx = i;
                new Thread(() -> {
                    try {
                        go.await();
                        replies[idx] = post("/api/election/start", "{\"node\":" + (idx + 1) + "}");
                    } catch (Exception e) {
                        replies[idx] = "ERROR " + e.getMessage();
                    } finally {
                        done.countDown();
                    }
                }).start();
            }
            go.countDown();
            done.await();
            Map<String, Object> st = waitStable(5, 5, STABLE_TIMEOUT_MS, t0);
            List<Map<String, Object>> evs = eventsSince(seqBefore);
            Set<Integer> accepted = new TreeSet<>();
            Set<Integer> selfClaims = new TreeSet<>();
            for (Map<String, Object> e : evs) {
                if ("LEADER_ACCEPTED".equals(e.get("type"))) {
                    int leader = (int) num(map(e.get("fields")).get("leader"));
                    accepted.add(leader);
                    if (leader == num(e.get("node"))) {
                        selfClaims.add(leader);
                    }
                }
            }
            List<Integer> leadersNow = new ArrayList<>();
            for (Map<String, Object> n : listOf(get("/api/cluster"))) {
                if (Boolean.TRUE.equals(n.get("isLeader"))) {
                    leadersNow.add((int) num(n.get("id")));
                }
            }
            if (st == null) {
                line(String.format("run %2d: NOT STABLE; election=%s", run, election()));
                continue;
            }
            Map<String, Object> ep = map(st.get("lastEpisode"));
            boolean ok = leadersNow.equals(List.of(5)) && accepted.equals(Set.of(5)) && selfClaims.equals(Set.of(5))
                    && Boolean.TRUE.equals(st.get("agreed"));
            line(String.format("run %2d: nodes claiming leadership=%s, leaders accepted by anyone=%s, isLeader now=%s, "
                            + "agreed=%s, initiators=%s, convergence=%s ms, messages=%s byKind=%s -> %s",
                    run, selfClaims, accepted, leadersNow, st.get("agreed"), ep.get("initiators"),
                    ep.get("convergenceMs"), ep.get("messages"), ep.get("byKind"), ok ? "ok" : "WRONG"));
            if (ok) {
                good++;
            }
        }
        check("V4 split-brain " + alg, good == SPLIT_RUNS, good + "/" + SPLIT_RUNS + " runs with exactly one leader (5)");
    }

    // =========================================================================
    // V5: kill 5, then kill 4 while its election is running
    // =========================================================================

    /**
     * BULLY (counted): kill 4 as soon as node 4's own ELECTION_STARTED is visible; node 4 then
     * sits in its 500 ms ANSWER window. RING (counted=false, informational): node 4 is often
     * only a forwarder, so the trigger is the first ELECTION_STARTED of any survivor.
     * An attempt counts as mid-election only if no node ever accepted node 4 as leader.
     */
    private void v5CascadedKill(String alg, boolean counted) throws Exception {
        boolean anyInitiator = !counted;
        header("V5 cascaded failure (" + alg + (counted ? "" : ", informational") + "): kill 5, then kill 4 while "
                + (anyInitiator ? "the failover election runs" : "its election runs"));
        boolean passedOnce = false;
        String summary = "no attempt landed inside the election";
        for (int attempt = 1; attempt <= 3 && !passedOnce; attempt++) {
            ensureCluster(alg);
            long seqBefore = lastSeq();
            post("/api/nodes/5/kill", "{}");
            long kill5True = num(findEvent(seqBefore, "NODE_KILLED", 5).get("trueMs"));

            // Wait until the trigger ELECTION_STARTED is visible, then kill node 4 at once.
            Map<String, Object> started4 = null;
            long deadline = System.currentTimeMillis() + 10000;
            while (started4 == null && System.currentTimeMillis() < deadline) {
                for (Map<String, Object> e : eventsSince(seqBefore)) {
                    if ("ELECTION_STARTED".equals(e.get("type")) && (anyInitiator || num(e.get("node")) == 4)) {
                        started4 = e;
                        break;
                    }
                }
                if (started4 == null) {
                    Thread.sleep(10);
                }
            }
            if (started4 == null) {
                line("attempt " + attempt + ": no matching ELECTION_STARTED within 10 s");
                for (int id : new int[] {4, 5}) {
                    post("/api/nodes/" + id + "/restart", "{}");
                }
                waitStable(5, 5, STABLE_TIMEOUT_MS, System.currentTimeMillis());
                continue;
            }
            post("/api/nodes/4/kill", "{}");
            Map<String, Object> kill4 = findEvent(seqBefore, "NODE_KILLED", 4);
            long kill4True = num(kill4.get("trueMs"));

            Map<String, Object> st = waitStable(3, 3, STABLE_TIMEOUT_MS, kill5True);
            List<Map<String, Object>> evs = eventsSince(seqBefore);
            boolean node4EverAccepted = false;
            int restartsAfterTimeout = 0;
            List<String> timeline = new ArrayList<>();
            for (Map<String, Object> e : evs) {
                String type = String.valueOf(e.get("type"));
                Map<String, Object> f = map(e.get("fields"));
                if ("LEADER_ACCEPTED".equals(type) && f != null && num(f.get("leader")) == 4) {
                    node4EverAccepted = true;
                }
                if ("ELECTION_STARTED".equals(type) && String.valueOf(e.get("details")).contains("restarting")) {
                    restartsAfterTimeout++;
                }
                if (!"ELECTION_MSG".equals(type) && !"HEARTBEAT_MISS".equals(type)) {
                    timeline.add(String.format("    +%5d ms  L=%-5s node %s  %-17s %s", num(e.get("trueMs")) - kill5True,
                            e.get("lamport"), e.get("node"), type, e.get("details")));
                }
            }
            line(String.format("attempt %d: trigger ELECTION_STARTED on node %s at +%d ms, node 4 killed at +%d ms "
                            + "(after kill of 5), any node ever accepted node 4 as leader: %s",
                    attempt, started4.get("node"), num(started4.get("trueMs")) - kill5True, kill4True - kill5True,
                    node4EverAccepted));
            line("  timeline (without ELECTION_MSG / HEARTBEAT_MISS):");
            timeline.forEach(ElectionVerifier::line);
            if (st == null) {
                line("  NOT STABLE; election=" + election());
                summary = "attempt " + attempt + " did not stabilise";
            } else {
                Map<String, Object> ep = map(st.get("lastEpisode"));
                boolean ok = num(st.get("leaderId")) == 3 && Boolean.TRUE.equals(st.get("agreed"))
                        && allViewsAre(st, 3, Set.of(1, 2, 3));
                line(String.format("  result: leader=%s backup=%s agreed=%s views=%s convergence=%s ms messages=%s "
                                + "byKind=%s restarts-after-timeout=%d -> %s",
                        st.get("leaderId"), st.get("backupId"), st.get("agreed"), st.get("views"),
                        ep.get("convergenceMs"), ep.get("messages"), ep.get("byKind"), restartsAfterTimeout,
                        ok ? "ok" : "WRONG"));
                if (!node4EverAccepted && ok) {
                    passedOnce = true;
                    summary = "attempt " + attempt + ": node 4 killed mid-election at +" + (kill4True - kill5True)
                            + " ms; survivors agreed on 3 (convergence " + ep.get("convergenceMs") + " ms, "
                            + restartsAfterTimeout + " restarts after COORDINATOR timeout)";
                } else if (!ok) {
                    summary = "attempt " + attempt + ": survivors did not converge on 3";
                } else {
                    summary = "attempt " + attempt + ": kill of 4 landed after the election had chosen node 4";
                }
            }
            for (int id : new int[] {4, 5}) {
                post("/api/nodes/" + id + "/restart", "{}");
            }
            waitStable(5, 5, STABLE_TIMEOUT_MS, System.currentTimeMillis());
        }
        if (counted) {
            check("V5 cascaded kill " + alg, passedOnce, summary);
        } else {
            info.put("V5 cascaded kill " + alg, (passedOnce ? "landed mid-election: " : "not achieved: ") + summary);
            line("INFO V5 cascaded kill " + alg + " (not counted): " + info.get("V5 cascaded kill " + alg));
        }
    }

    // =========================================================================
    // V6: Lamport order of kill-leader episodes
    // =========================================================================

    private void v6Lamport() {
        header("V6 Lamport order in kill-leader episodes (" + killEpisodes.size() + " episodes from V2)");
        int bad = 0;
        for (int i = 0; i < killEpisodes.size(); i++) {
            List<Map<String, Object>> evs = killEpisodes.get(i);
            long killLamport = killLamports.get(i);
            List<String> problems = new ArrayList<>();

            // Each node's own events, in the node's seq order, must have strictly increasing Lamport times.
            Map<Integer, List<Map<String, Object>>> byNode = new TreeMap<>();
            for (Map<String, Object> e : evs) {
                if (e.get("nodeSeq") != null) {
                    byNode.computeIfAbsent((int) num(e.get("node")), k -> new ArrayList<>()).add(e);
                }
            }
            for (Map.Entry<Integer, List<Map<String, Object>>> n : byNode.entrySet()) {
                List<Map<String, Object>> list = n.getValue();
                list.sort((a, b) -> Long.compare(num(a.get("nodeSeq")), num(b.get("nodeSeq"))));
                for (int k = 1; k < list.size(); k++) {
                    if (num(list.get(k).get("lamport")) <= num(list.get(k - 1).get("lamport"))) {
                        problems.add("node " + n.getKey() + " seq " + list.get(k).get("nodeSeq") + " L="
                                + list.get(k).get("lamport") + " after L=" + list.get(k - 1).get("lamport"));
                    }
                }
            }
            // NODE_KILLED precedes every HEARTBEAT_MISS and ELECTION_STARTED of the episode.
            int checked = 0;
            for (Map<String, Object> e : evs) {
                String type = String.valueOf(e.get("type"));
                if (e.get("nodeSeq") != null && ("HEARTBEAT_MISS".equals(type) || "ELECTION_STARTED".equals(type))) {
                    checked++;
                    if (num(e.get("lamport")) <= killLamport) {
                        problems.add(type + " on node " + e.get("node") + " L=" + e.get("lamport")
                                + " not after NODE_KILLED L=" + killLamport);
                    }
                }
            }
            line(String.format("episode %d: NODE_KILLED(node %d) L=%d; %d nodes' own sequences checked; "
                            + "%d HEARTBEAT_MISS/ELECTION_STARTED events checked; problems=%s",
                    i + 1, killedNodes.get(i), killLamport, byNode.size(), checked, problems));
            if (!problems.isEmpty() || checked == 0) {
                bad++;
            }
        }
        if (!killEpisodes.isEmpty()) {
            line("");
            line("episode 1 in Lamport order (lamport, node, nodeSeq):");
            List<Map<String, Object>> evs = new ArrayList<>(killEpisodes.get(0));
            long base = 0;
            for (Map<String, Object> e : evs) {
                if ("NODE_KILLED".equals(e.get("type"))) {
                    base = num(e.get("trueMs"));
                }
            }
            evs.sort((a, b) -> {
                int c = Long.compare(num(a.get("lamport")), num(b.get("lamport")));
                if (c != 0) return c;
                c = Long.compare(num(a.get("node")), num(b.get("node")));
                if (c != 0) return c;
                return Long.compare(num(a.get("nodeSeq")), num(b.get("nodeSeq")));
            });
            for (Map<String, Object> e : evs) {
                System.out.printf("  L=%-5s node %s  %-18s +%5d ms  %s%n", e.get("lamport"), e.get("node"),
                        e.get("type"), num(e.get("trueMs")) - base, e.get("details"));
            }
        }
        check("V6 Lamport order", bad == 0 && !killEpisodes.isEmpty(),
                (killEpisodes.size() - bad) + "/" + killEpisodes.size() + " episodes with per-node strictly "
                        + "increasing Lamport and NODE_KILLED first");
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Brings all 5 nodes up with the given algorithm and waits for leader 5. */
    private void ensureCluster(String alg) throws Exception {
        for (int id = 1; id <= 5; id++) {
            if (!upNodes().contains(id)) {
                post("/api/nodes/" + id + "/restart", "{}");
            }
        }
        post("/api/election/algorithm", "{\"name\":\"" + alg + "\"}");
        Map<String, Object> st = waitStable(5, 5, STABLE_TIMEOUT_MS, 0);
        if (st == null) {
            line("cluster not stable on leader 5; starting an election on node 1");
            post("/api/election/start", "{\"node\":1}");
            st = waitStable(5, 5, STABLE_TIMEOUT_MS, 0);
        }
        long settledMs = waitSettled(5, 5);
        line("cluster ready: algorithm=" + (st == null ? null : st.get("algorithm")) + " leader="
                + (st == null ? null : st.get("leaderId")) + " views=" + (st == null ? null : st.get("views"))
                + " (settled for " + SETTLE_MS + " ms after " + settledMs + " ms)");
    }

    /**
     * Waits until the cluster has stayed agreed on leader with upCount nodes UP, and the
     * reported last episode has not changed, for SETTLE_MS. Returns the time waited.
     */
    private long waitSettled(int leader, int upCount) throws Exception {
        long t0 = System.currentTimeMillis();
        long deadline = t0 + STABLE_TIMEOUT_MS;
        String lastKey = null;
        long since = System.currentTimeMillis();
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> el = election();
            boolean ok = upNodes().size() == upCount && Boolean.TRUE.equals(el.get("agreed"))
                    && num(el.get("leaderId")) == leader;
            Map<String, Object> ep = map(el.get("lastEpisode"));
            String key = ok ? (ep == null ? "none" : ep.get("startTrueMs") + ":" + ep.get("endTrueMs")) : null;
            if (key == null || !key.equals(lastKey)) {
                lastKey = key;
                since = System.currentTimeMillis();
            } else if (System.currentTimeMillis() - since >= SETTLE_MS) {
                return System.currentTimeMillis() - t0;
            }
            Thread.sleep(100);
        }
        return -1;
    }

    /**
     * Waits until exactly upCount nodes are UP, they agree on leader, and (if afterTrueMs > 0)
     * an election episode that started after afterTrueMs has been closed. Returns /api/election.
     */
    private Map<String, Object> waitStable(int leader, int upCount, long timeoutMs, long afterTrueMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> el = election();
            if (upNodes().size() == upCount && Boolean.TRUE.equals(el.get("agreed")) && num(el.get("leaderId")) == leader) {
                if (afterTrueMs <= 0) {
                    return el;
                }
                Map<String, Object> ep = map(el.get("lastEpisode"));
                if (ep != null && num(ep.get("startTrueMs")) > afterTrueMs && num(ep.get("leaderId")) == leader) {
                    return el;
                }
            }
            Thread.sleep(100);
        }
        return null;
    }

    private interface Cond {
        boolean ok() throws Exception;
    }

    private void waitUntil(Cond c, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!c.ok() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
    }

    private boolean allViewsAre(Map<String, Object> el, int leader, Set<Integer> nodes) {
        Map<String, Object> views = map(el.get("views"));
        for (int id : nodes) {
            Object v = views.get(String.valueOf(id));
            if (v == null || num(v) != leader) {
                return false;
            }
        }
        return true;
    }

    private List<Integer> upNodes() throws Exception {
        List<Integer> up = new ArrayList<>();
        for (Map<String, Object> n : listOf(get("/api/cluster"))) {
            if (Boolean.TRUE.equals(n.get("up"))) {
                up.add((int) num(n.get("id")));
            }
        }
        return up;
    }

    private Map<String, Object> election() throws Exception {
        return map(get("/api/election"));
    }

    private long lastSeq() throws Exception {
        long max = 0;
        for (Map<String, Object> e : eventsSince(0)) {
            max = Math.max(max, num(e.get("seq")));
        }
        return max;
    }

    private List<Map<String, Object>> eventsSince(long seq) throws Exception {
        return listOf(get("/api/events?since=" + seq));
    }

    private Map<String, Object> findEvent(long sinceSeq, String type, int node) throws Exception {
        for (Map<String, Object> e : eventsSince(sinceSeq)) {
            if (type.equals(e.get("type")) && num(e.get("node")) == node) {
                return e;
            }
        }
        throw new IllegalStateException(type + " for node " + node + " not found after seq " + sinceSeq);
    }

    private Object get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path)).timeout(Duration.ofSeconds(10)).GET().build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) {
            throw new IllegalStateException("GET " + path + " -> " + res.statusCode() + " " + res.body());
        }
        return JsonUtil.parse(res.body());
    }

    private String post(String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path))
                .timeout(Duration.ofSeconds(15))
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
        if (values.isEmpty()) {
            return "n/a";
        }
        List<Long> s = new ArrayList<>(values);
        Collections.sort(s);
        int n = s.size();
        return n % 2 == 1 ? String.valueOf(s.get(n / 2)) : String.valueOf((s.get(n / 2 - 1) + s.get(n / 2)) / 2.0);
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
