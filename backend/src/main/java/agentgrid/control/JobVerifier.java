package agentgrid.control;

import agentgrid.orchestrator.Corpus;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * End-to-end verifier for Phase 4: the orchestrator on the elected leader and real work
 * flowing through the cluster. Drives the control plane's HTTP API only (127.0.0.1:8080 must
 * be running); the bundled corpus is read from the classpath solely to check that the answer's
 * sentences come from the cited documents. Prints raw numbers for every step and a PASS/FAIL
 * table; exits non-zero if any check fails.
 *
 * Usage (from backend/): java -cp build/classes agentgrid.control.JobVerifier
 */
public class JobVerifier {

    private static final String BASE = System.getProperty("agentgrid.verifier.base", "http://127.0.0.1:8080");
    private static final String QUERY = "How do leader election and failure detectors handle a crashed node?";
    private static final List<String> POLICIES = List.of("ROUND_ROBIN", "WEIGHTED", "LEAST_LOADED");
    private static final int RUNS = 3;
    private static final long JOB_TIMEOUT_MS = 30000;
    private static final long STABLE_TIMEOUT_MS = 20000;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final Map<String, Boolean> passed = new LinkedHashMap<>();
    private final Map<String, String> results = new LinkedHashMap<>();

    public static void main(String[] args) throws Exception {
        boolean ok = new JobVerifier().run();
        System.exit(ok ? 0 : 1);
    }

    private boolean run() throws Exception {
        line("=====================================================================");
        line(" AgentGrid-Lite Phase 4 job verifier (HTTP API only) -> " + BASE);
        line("=====================================================================");
        line("modules: " + get("/api/modules"));
        ensureCluster();

        j1();
        j2();
        j3();
        j5();
        j4();

        line("");
        line("=====================================================================");
        line(" RESULT");
        line("=====================================================================");
        boolean all = true;
        for (Map.Entry<String, Boolean> e : passed.entrySet()) {
            System.out.printf("%-4s  %-34s %s%n", e.getValue() ? "PASS" : "FAIL", e.getKey(), results.get(e.getKey()));
            all &= e.getValue();
        }
        line("OVERALL: " + (all ? "PASS" : "FAIL"));
        return all;
    }

    // =========================================================================
    // J1: a job completes with a corpus-derived answer
    // =========================================================================

    private void j1() throws Exception {
        header("J1 submit a job; it reaches COMPLETE with an answer derived from the corpus");
        Map<String, Object> job = runJob(QUERY, "WEIGHTED");
        printGraph(job);
        String answer = (String) job.get("answer");
        line("answer: " + answer);

        Corpus corpus = Corpus.load();
        Matcher m = Pattern.compile("(.+?) \\[(doc-\\d+)\\]\\s*").matcher(answer == null ? "" : answer);
        int sentences = 0;
        int verified = 0;
        List<String> cited = new ArrayList<>();
        while (m.find()) {
            sentences++;
            String sentence = m.group(1).trim();
            String docId = m.group(2);
            cited.add(docId);
            Corpus.Doc doc = corpus.get(docId);
            boolean found = doc != null && doc.getSentences().contains(sentence);
            if (found) {
                verified++;
            }
            line(String.format("  [%s] %s -> sentence found verbatim in %s: %s", docId,
                    doc == null ? "(no such document)" : doc.getTitle(), docId, found));
        }
        boolean allComplete = "COMPLETE".equals(job.get("status")) && num(job.get("completedSubtasks")) == num(job.get("subtasks"));
        boolean ok = allComplete && answer != null && !answer.isBlank() && sentences > 0 && verified == sentences;
        check("J1 job completes, answer from corpus", ok, "status=" + job.get("status") + ", "
                + job.get("completedSubtasks") + "/" + job.get("subtasks") + " subtasks, makespan "
                + job.get("makespanMs") + " ms, " + verified + "/" + sentences
                + " answer sentences found verbatim in cited documents " + cited);
    }

    // =========================================================================
    // J2: dispatch comes from the leader
    // =========================================================================

    private void j2() throws Exception {
        header("J2 the orchestrator runs only on the leader");
        Map<String, Object> el = map(get("/api/election"));
        long leader = num(el.get("leaderId"));
        line("agreed leader: node " + leader + " (agreed=" + el.get("agreed") + ", views=" + el.get("views") + ")");
        long seqBefore = lastSeq();
        Map<String, Object> job = runJob(QUERY, "LEAST_LOADED");
        String jobId = (String) job.get("jobId");
        Thread.sleep(1500);  // let the monitor pull the leader's last events
        Map<Long, Integer> dispatchByRecordingNode = new TreeMap<>();
        Map<Long, Integer> jobEventsByRecordingNode = new TreeMap<>();
        for (Map<String, Object> e : eventsSince(seqBefore)) {
            Map<String, Object> f = map(e.get("fields"));
            if (f == null || !jobId.equals(f.get("jobId"))) {
                continue;
            }
            jobEventsByRecordingNode.merge(num(e.get("node")), 1, Integer::sum);
            if ("SUBTASK_DISPATCHED".equals(e.get("type"))) {
                dispatchByRecordingNode.merge(num(e.get("node")), 1, Integer::sum);
            }
        }
        line("job " + jobId + " reported leaderNode=" + job.get("leaderNode"));
        line("SUBTASK_DISPATCHED events by recording node: " + dispatchByRecordingNode);
        line("all events of this job by recording node:    " + jobEventsByRecordingNode);
        line("subtasks run per worker node: " + job.get("subtasksPerNode"));
        boolean ok = num(job.get("leaderNode")) == leader
                && dispatchByRecordingNode.keySet().equals(java.util.Set.of(leader))
                && jobEventsByRecordingNode.keySet().equals(java.util.Set.of(leader))
                && dispatchByRecordingNode.get(leader) >= num(job.get("subtasks"));
        check("J2 dispatch only from leader", ok, "leader node " + leader + "; " + dispatchByRecordingNode.getOrDefault(leader, 0)
                + " dispatch events, all recorded by node(s) " + dispatchByRecordingNode.keySet()
                + "; workers used " + job.get("subtasksPerNode"));
    }

    // =========================================================================
    // J3: policy comparison
    // =========================================================================

    private void j3() throws Exception {
        header("J3 the same job under all three policies, " + RUNS + " runs each (runs interleaved)");
        Map<String, List<Long>> makespans = new LinkedHashMap<>();
        for (String p : POLICIES) {
            makespans.put(p, new ArrayList<>());
        }
        for (int run = 1; run <= RUNS; run++) {
            for (String policy : POLICIES) {
                Map<String, Object> job = runJob(QUERY, policy);
                long makespan = num(job.get("makespanMs"));
                makespans.get(policy).add(makespan);
                line(String.format("%-12s run %d: %s makespan=%d ms subtasks per node=%s peak queue depth=%s",
                        policy, run, job.get("status"), makespan, job.get("subtasksPerNode"), job.get("peakQueueDepth")));
            }
        }
        Map<String, Long> medians = new LinkedHashMap<>();
        for (String p : POLICIES) {
            medians.put(p, median(makespans.get(p)));
            line(String.format("%-12s median makespan = %d ms (runs %s)", p, medians.get(p), makespans.get(p)));
        }
        long rr = medians.get("ROUND_ROBIN");
        check("J3 WEIGHTED beats ROUND_ROBIN", medians.get("WEIGHTED") < rr,
                "median " + medians.get("WEIGHTED") + " ms vs " + rr + " ms");
        check("J3 LEAST_LOADED beats ROUND_ROBIN", medians.get("LEAST_LOADED") < rr,
                "median " + medians.get("LEAST_LOADED") + " ms vs " + rr + " ms");
    }

    // =========================================================================
    // J4: leader killed mid-job
    // =========================================================================

    private void j4() throws Exception {
        header("J4 kill the leader mid-job: ORPHANED with completed subtasks kept; the new leader takes new jobs");
        ensureCluster();
        long leader = num(election().get("leaderId"));
        Map<String, Object> resp = map(post("/api/jobs", "{\"query\":\"" + QUERY + "\",\"policy\":\"ROUND_ROBIN\"}"));
        String jobId = (String) resp.get("jobId");
        line("submitted " + jobId + " to leader node " + resp.get("leader"));

        Map<String, Object> job = null;
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            job = map(get("/api/jobs/" + jobId));
            if (!"RUNNING".equals(job.get("status")) && !"QUEUED".equals(job.get("status"))) {
                break;
            }
            if (num(job.get("completedSubtasks")) >= 1) {
                break;
            }
            Thread.sleep(20);
        }
        line("before kill: status=" + job.get("status") + " completed " + job.get("completedSubtasks") + "/"
                + job.get("subtasks") + " subtasks created so far");
        long seqBefore = lastSeq();
        post("/api/nodes/" + leader + "/kill", "{}");
        long killTrue = num(findEvent(seqBefore, "NODE_KILLED", leader).get("trueMs"));
        line("killed leader node " + leader);

        Map<String, Object> orphan = null;
        deadline = System.currentTimeMillis() + STABLE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            orphan = map(get("/api/jobs/" + jobId));
            if ("ORPHANED".equals(orphan.get("status")) && orphan.get("orphanAdoptedBy") != null) {
                break;
            }
            Thread.sleep(100);
        }
        Map<String, Object> el = election();
        long newLeader = num(el.get("leaderId"));
        line("after kill: election agreed=" + el.get("agreed") + " new leader=" + newLeader + " views=" + el.get("views"));
        line("orphaned job: status=" + orphan.get("status") + " completed " + orphan.get("completedSubtasks") + "/"
                + orphan.get("subtasks") + ", recorded as orphan by node " + orphan.get("orphanAdoptedBy"));
        int retained = 0;
        for (Map<String, Object> stage : listOf(orphan.get("stages"))) {
            List<String> done = new ArrayList<>();
            for (Map<String, Object> s : listOf(stage.get("subtasks"))) {
                if ("COMPLETE".equals(s.get("status"))) {
                    retained++;
                    done.add(s.get("subtaskId") + "@N" + s.get("node"));
                }
            }
            line(String.format("  %-10s %-8s %d subtasks, %d complete %s", stage.get("type"), stage.get("status"),
                    listOf(stage.get("subtasks")).size(), done.size(), done));
        }
        // The new leader records JOB_ORPHANED in its own buffer; the control plane pulls it on
        // its next cycle, so wait for it to appear in the merged log.
        Map<String, Object> orphanEvent = null;
        long electionStarted = -1;
        deadline = System.currentTimeMillis() + 10000;
        while (orphanEvent == null && System.currentTimeMillis() < deadline) {
            electionStarted = -1;
            for (Map<String, Object> e : eventsSince(seqBefore)) {
                Map<String, Object> f = map(e.get("fields"));
                if ("JOB_ORPHANED".equals(e.get("type")) && f != null && jobId.equals(f.get("jobId"))) {
                    orphanEvent = e;
                }
                if ("ELECTION_STARTED".equals(e.get("type")) && electionStarted < 0) {
                    electionStarted = num(e.get("trueMs")) - killTrue;
                }
            }
            if (orphanEvent == null) {
                Thread.sleep(100);
            }
        }
        line("first ELECTION_STARTED at +" + electionStarted + " ms after the kill; JOB_ORPHANED event: "
                + (orphanEvent == null ? "none" : "node " + orphanEvent.get("node") + " L=" + orphanEvent.get("lamport")
                + " \"" + orphanEvent.get("details") + "\""));

        boolean orphanOk = "ORPHANED".equals(orphan.get("status")) && retained >= 1
                && retained == num(orphan.get("completedSubtasks")) && retained < countSubtasksPlanned(orphan)
                && orphanEvent != null && num(orphanEvent.get("node")) == newLeader && electionStarted >= 0;
        check("J4 leader killed -> job ORPHANED", orphanOk, jobId + " ORPHANED with " + retained
                + " completed subtasks retained; election ran; JOB_ORPHANED recorded by new leader node "
                + (orphanEvent == null ? "?" : orphanEvent.get("node")));

        Map<String, Object> second = runJob(QUERY, "WEIGHTED");
        line("new job on the new leader:");
        printGraph(second);
        boolean newOk = "COMPLETE".equals(second.get("status")) && num(second.get("leaderNode")) == newLeader
                && newLeader != leader;
        check("J4 new leader completes a new job", newOk, second.get("jobId") + " " + second.get("status")
                + " on leader node " + second.get("leaderNode") + " in " + second.get("makespanMs") + " ms");

        post("/api/nodes/" + leader + "/restart", "{}");
        Map<String, Object> back = waitAgreed(5, 5);
        line("restarted node " + leader + ": leader=" + (back == null ? "not stable" : back.get("leaderId")));
    }

    /** Subtasks the full pipeline would have had: RETRIEVE fan-out + RANK + SUMMARIZE fan-out + SYNTHESIZE. */
    private static int countSubtasksPlanned(Map<String, Object> job) {
        int planned = 0;
        for (Map<String, Object> stage : listOf(job.get("stages"))) {
            int n = listOf(stage.get("subtasks")).size();
            planned += n == 0 ? 1 : n;
        }
        return planned;
    }

    // =========================================================================
    // J5: Lamport chain
    // =========================================================================

    private void j5() throws Exception {
        header("J5 Lamport chain of one completed job");
        long seqBefore = lastSeq();
        Map<String, Object> job = runJob(QUERY, "WEIGHTED");
        String jobId = (String) job.get("jobId");
        Thread.sleep(1500);  // let the monitor pull the leader's last events
        List<Map<String, Object>> events = eventsSince(seqBefore);

        List<Map<String, Object>> chain = new ArrayList<>();
        for (Map<String, Object> e : events) {
            Map<String, Object> f = map(e.get("fields"));
            String type = String.valueOf(e.get("type"));
            if (f != null && jobId.equals(f.get("jobId"))
                    && (type.equals("SUBTASK_DISPATCHED") || type.equals("SUBTASK_COMPLETED"))) {
                chain.add(e);
            }
        }
        chain.sort((a, b) -> {
            int c = Long.compare(num(a.get("lamport")), num(b.get("lamport")));
            if (c != 0) return c;
            c = Long.compare(num(a.get("node")), num(b.get("node")));
            return c != 0 ? c : Long.compare(num(a.get("nodeSeq")), num(b.get("nodeSeq")));
        });
        line(String.format("%-7s %-5s %-19s %-11s %-13s %-6s %s", "lamport", "node", "type", "stage", "subtask",
                "worker", "carried/result"));
        for (Map<String, Object> e : chain) {
            Map<String, Object> f = map(e.get("fields"));
            String extra = "SUBTASK_COMPLETED".equals(e.get("type"))
                    ? "dispatch L=" + f.get("dispatchLamport") + " result L=" + f.get("resultLamport") : "";
            line(String.format("L=%-5s N%-4s %-19s %-11s %-13s N%-5s %s", e.get("lamport"), e.get("node"), e.get("type"),
                    f.get("stage"), f.get("subtaskId"), f.get("node"), extra));
        }

        List<String> problems = new ArrayList<>();
        Map<String, Long> dispatchL = new HashMap<>();
        Map<String, Long> completeL = new HashMap<>();
        Map<String, String> stageOf = new HashMap<>();
        for (Map<String, Object> e : chain) {
            Map<String, Object> f = map(e.get("fields"));
            String id = (String) f.get("subtaskId");
            stageOf.put(id, (String) f.get("stage"));
            if ("SUBTASK_DISPATCHED".equals(e.get("type"))) {
                dispatchL.put(id, num(e.get("lamport")));
            } else {
                completeL.put(id, num(e.get("lamport")));
                long d = num(f.get("dispatchLamport"));
                long r = num(f.get("resultLamport"));
                long c = num(e.get("lamport"));
                if (!(d < r && r < c)) {
                    problems.add(id + ": dispatch " + d + " < result " + r + " < completion " + c + " fails");
                }
                if (dispatchL.get(id) == null || dispatchL.get(id) != d) {
                    problems.add(id + ": carried timestamp " + d + " differs from its dispatch event " + dispatchL.get(id));
                }
            }
        }
        if (completeL.size() != num(job.get("subtasks"))) {
            problems.add("found " + completeL.size() + " completion events for " + job.get("subtasks") + " subtasks");
        }
        // Across stages: every dispatch of a stage follows every completion of the stage before it.
        List<String> order = List.of("RETRIEVE", "RANK", "SUMMARIZE", "SYNTHESIZE");
        int stageLinks = 0;
        for (int i = 1; i < order.size(); i++) {
            long prevMaxComplete = -1;
            long minDispatch = Long.MAX_VALUE;
            for (String id : completeL.keySet()) {
                if (order.get(i - 1).equals(stageOf.get(id))) {
                    prevMaxComplete = Math.max(prevMaxComplete, completeL.get(id));
                }
            }
            for (String id : dispatchL.keySet()) {
                if (order.get(i).equals(stageOf.get(id))) {
                    minDispatch = Math.min(minDispatch, dispatchL.get(id));
                }
            }
            stageLinks++;
            line(String.format("stage link %s -> %s: last completion L=%d, first dispatch L=%d", order.get(i - 1),
                    order.get(i), prevMaxComplete, minDispatch));
            if (!(minDispatch > prevMaxComplete)) {
                problems.add(order.get(i) + " dispatched at L=" + minDispatch + " before " + order.get(i - 1)
                        + " finished at L=" + prevMaxComplete);
            }
        }
        // Each node's own events, in the node's seq order, strictly increase.
        Map<Long, List<Map<String, Object>>> byNode = new TreeMap<>();
        for (Map<String, Object> e : events) {
            if (e.get("nodeSeq") != null) {
                byNode.computeIfAbsent(num(e.get("node")), k -> new ArrayList<>()).add(e);
            }
        }
        int nodeEvents = 0;
        for (Map.Entry<Long, List<Map<String, Object>>> n : byNode.entrySet()) {
            List<Map<String, Object>> list = n.getValue();
            list.sort((a, b) -> Long.compare(num(a.get("nodeSeq")), num(b.get("nodeSeq"))));
            nodeEvents += list.size();
            for (int k = 1; k < list.size(); k++) {
                if (num(list.get(k).get("lamport")) <= num(list.get(k - 1).get("lamport"))) {
                    problems.add("node " + n.getKey() + " seq " + list.get(k).get("nodeSeq") + " L=" + list.get(k).get("lamport")
                            + " after L=" + list.get(k - 1).get("lamport"));
                }
            }
        }
        line("nodes' own event sequences checked: " + byNode.keySet() + " (" + nodeEvents + " events)");
        line("problems: " + problems);
        check("J5 Lamport chain causally consistent", problems.isEmpty() && !chain.isEmpty(),
                completeL.size() + " subtasks with dispatch < result < completion; " + stageLinks
                        + " stage links ordered; " + nodeEvents + " node events strictly increasing per node");
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private Map<String, Object> runJob(String query, String policy) throws Exception {
        Map<String, Object> resp = map(post("/api/jobs", "{\"query\":\"" + query + "\",\"policy\":\"" + policy + "\"}"));
        String jobId = (String) resp.get("jobId");
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
        Map<String, Object> job;
        do {
            Thread.sleep(100);
            job = map(get("/api/jobs/" + jobId));
        } while (("RUNNING".equals(job.get("status")) || "QUEUED".equals(job.get("status")))
                && System.currentTimeMillis() < deadline);
        return job;
    }

    private void printGraph(Map<String, Object> job) {
        line("job " + job.get("jobId") + " status=" + job.get("status") + " policy=" + job.get("policy") + " leader=node "
                + job.get("leaderNode") + " makespan=" + job.get("makespanMs") + " ms subtasks per node="
                + job.get("subtasksPerNode") + " peak queue depth=" + job.get("peakQueueDepth"));
        for (Map<String, Object> stage : listOf(job.get("stages"))) {
            line(String.format("  %-10s %-8s %d subtasks in %s ms", stage.get("type"), stage.get("status"),
                    listOf(stage.get("subtasks")).size(), stage.get("durationMs")));
            for (Map<String, Object> s : listOf(stage.get("subtasks"))) {
                line(String.format("    %-13s node %-4s %-9s L dispatch=%-5s result=%-5s complete=%-5s %s",
                        s.get("subtaskId"), s.get("node"), s.get("status"), s.get("dispatchLamport"),
                        s.get("resultLamport"), s.get("completeLamport"), s.get("input")));
            }
        }
    }

    /** All 5 nodes up and agreed on leader 5; restarts any node that is down. */
    private void ensureCluster() throws Exception {
        for (Map<String, Object> n : listOf(get("/api/cluster"))) {
            if (!Boolean.TRUE.equals(n.get("up"))) {
                post("/api/nodes/" + num(n.get("id")) + "/restart", "{}");
            }
        }
        Map<String, Object> el = waitAgreed(5, 5);
        line("cluster: " + (el == null ? "NOT agreed on leader 5" : "5 nodes up, agreed leader " + el.get("leaderId")));
    }

    private Map<String, Object> waitAgreed(int leader, int upCount) throws Exception {
        long deadline = System.currentTimeMillis() + STABLE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> el = election();
            int up = 0;
            for (Map<String, Object> n : listOf(get("/api/cluster"))) {
                if (Boolean.TRUE.equals(n.get("up"))) {
                    up++;
                }
            }
            if (up == upCount && Boolean.TRUE.equals(el.get("agreed")) && num(el.get("leaderId")) == leader) {
                return el;
            }
            Thread.sleep(100);
        }
        return null;
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

    private Map<String, Object> findEvent(long sinceSeq, String type, long node) throws Exception {
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

    private static long median(List<Long> values) {
        List<Long> s = new ArrayList<>(values);
        Collections.sort(s);
        return s.get(s.size() / 2);
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
