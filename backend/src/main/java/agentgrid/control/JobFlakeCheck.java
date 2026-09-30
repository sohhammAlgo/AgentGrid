package agentgrid.control;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Checks the control plane's job status against false orphaning (FIXES.md). Drives the HTTP
 * API only (127.0.0.1:8080 must be running).
 *
 * Default mode: submits 30 jobs back to back, alternating STRONG and EVENTUAL, waits for each
 * to finish, and reports how many ended COMPLETE and how many were reported ORPHANED although no
 * node was killed. Prints every makespan and min/median/max; exits non-zero unless 30/30 COMPLETE.
 *
 * "orphan N" mode: N times, kills the leader while a STRONG job runs and measures, on this
 * process's clock from just before the kill request, when a new agreed leader, the job's
 * ORPHANED status and its JOB_ORPHANED event become visible through the API; then restarts the
 * killed node and waits for 5 ready nodes. Exits non-zero if any cycle never shows JOB_ORPHANED.
 *
 * Usage (from backend/): java -cp build/classes agentgrid.control.JobFlakeCheck [orphan N]
 */
public class JobFlakeCheck {

    private static final String BASE = System.getProperty("agentgrid.verifier.base", "http://127.0.0.1:8080");
    private static final String QUERY = "How do leader election and failure detectors handle a crashed node?";
    private static final int JOBS = 30;
    private static final long JOB_TIMEOUT_MS = 30000;
    private static final long READY_TIMEOUT_MS = 60000;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    public static void main(String[] args) throws Exception {
        JobFlakeCheck c = new JobFlakeCheck();
        boolean ok = args.length >= 1 && args[0].equals("orphan")
                ? c.orphanTiming(args.length >= 2 ? Integer.parseInt(args[1]) : 5)
                : c.flake();
        System.exit(ok ? 0 : 1);
    }

    // =========================================================================
    // 30 jobs, no failures injected
    // =========================================================================

    private boolean flake() throws Exception {
        System.out.println("=====================================================================");
        System.out.println(" JobFlakeCheck: " + JOBS + " jobs back to back, alternating STRONG / EVENTUAL -> " + BASE);
        System.out.println("=====================================================================");
        if (waitReady() < 0) {
            System.out.println("OVERALL: FAIL (cluster not 5 ready with an agreed leader)");
            return false;
        }
        long startSeq = lastSeq();
        Map<String, Integer> firstStatus = new TreeMap<>();
        Map<String, Integer> finalStatus = new TreeMap<>();
        List<Long> makespans = new ArrayList<>();
        List<String> orphaned = new ArrayList<>();
        for (int i = 1; i <= JOBS; i++) {
            String mode = i % 2 == 1 ? "STRONG" : "EVENTUAL";
            String jobId = submit(mode, "WEIGHTED");
            Map<String, Object> job = waitDone(jobId);
            String first = String.valueOf(job.get("status"));
            String last = first;
            if ("ORPHANED".equals(first)) {
                // Report whether a provisional ORPHANED later gives way to the leader's real outcome.
                long until = System.currentTimeMillis() + 5000;
                while (System.currentTimeMillis() < until && "ORPHANED".equals(last)) {
                    Thread.sleep(200);
                    last = String.valueOf(map(get("/api/jobs/" + jobId)).get("status"));
                }
                orphaned.add(jobId);
            }
            firstStatus.merge(first, 1, Integer::sum);
            finalStatus.merge(last, 1, Integer::sum);
            long ms = num(job.get("makespanMs"));
            if ("COMPLETE".equals(first)) makespans.add(ms);
            System.out.printf("job %2d %-8s %s: %s makespan %d ms%s%n", i, mode, jobId, first, ms,
                    first.equals(last) ? "" : " (5 s later: " + last + ")");
        }
        Map<String, Integer> byType = new TreeMap<>();
        for (Map<String, Object> e : listOf(get("/api/events?since=" + startSeq))) {
            String t = String.valueOf(e.get("type"));
            if (t.equals("NODE_DOWN_DETECTED") || t.equals("NODE_UP_DETECTED") || t.equals("LEADER_LOST")
                    || t.equals("ELECTION_STARTED") || t.equals("JOB_ORPHANED") || t.equals("NODE_KILLED")) {
                byType.merge(t, 1, Integer::sum);
            }
        }
        List<Long> sorted = new ArrayList<>(makespans);
        Collections.sort(sorted);
        System.out.println();
        System.out.println("status when each job first left RUNNING: " + firstStatus + "; 5 s later: " + finalStatus);
        System.out.println("falsely ORPHANED (no node was killed): " + orphaned.size() + (orphaned.isEmpty() ? "" : " " + orphaned));
        System.out.println("failure-related events during the check: " + (byType.isEmpty() ? "none" : byType));
        System.out.println("makespans of COMPLETE jobs (ms, sorted): " + sorted);
        if (!sorted.isEmpty()) {
            System.out.println("makespan min " + sorted.get(0) + " ms, median " + sorted.get(sorted.size() / 2)
                    + " ms, max " + sorted.get(sorted.size() - 1) + " ms");
        }
        int complete = firstStatus.getOrDefault("COMPLETE", 0);
        boolean ok = complete == JOBS;
        System.out.println((ok ? "PASS" : "FAIL") + "  " + complete + "/" + JOBS + " COMPLETE, " + orphaned.size() + " falsely ORPHANED");
        System.out.println("OVERALL: " + (ok ? "PASS" : "FAIL"));
        return ok;
    }

    // =========================================================================
    // kill the leader mid-job: time to JOB_ORPHANED
    // =========================================================================

    private boolean orphanTiming(int runs) throws Exception {
        System.out.println("=====================================================================");
        System.out.println(" JobFlakeCheck orphan: kill the leader mid-job " + runs + " times -> " + BASE);
        System.out.println("=====================================================================");
        List<Long> toEvent = new ArrayList<>();
        List<Long> toLeader = new ArrayList<>();
        boolean ok = true;
        for (int r = 1; r <= runs; r++) {
            if (waitReady() < 0) {
                System.out.println("run " + r + ": cluster not 5 ready with an agreed leader; stopping");
                ok = false;
                break;
            }
            int leader = (int) num(map(get("/api/election")).get("leaderId"));
            long seq = lastSeq();
            String jobId = submit("STRONG", "ROUND_ROBIN");
            long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
            String status = "?";
            while (System.currentTimeMillis() < deadline) {
                Map<String, Object> job = map(get("/api/jobs/" + jobId));
                status = String.valueOf(job.get("status"));
                if (!"QUEUED".equals(status) && !"RUNNING".equals(status)) break;
                if ("RUNNING".equals(status) && completedSubtasks(job) >= 1) break;
                Thread.sleep(20);
            }
            if (!"RUNNING".equals(status)) {
                System.out.println("run " + r + ": job " + jobId + " was " + status + " before the kill; not counted");
                ok = false;
                continue;
            }
            long t0 = System.currentTimeMillis();
            post("/api/nodes/" + leader + "/kill");
            long leaderAt = -1;
            long orphanStatusAt = -1;
            long eventAt = -1;
            long eventTrueMs = -1;
            long killTrueMs = -1;
            deadline = t0 + 15000;
            while (System.currentTimeMillis() < deadline && (leaderAt < 0 || orphanStatusAt < 0 || eventAt < 0)) {
                long now = System.currentTimeMillis() - t0;
                Map<String, Object> el = map(get("/api/election"));
                if (leaderAt < 0 && Boolean.TRUE.equals(el.get("agreed")) && el.get("leaderId") != null
                        && num(el.get("leaderId")) != leader) {
                    leaderAt = now;
                }
                if (orphanStatusAt < 0 && "ORPHANED".equals(map(get("/api/jobs/" + jobId)).get("status"))) {
                    orphanStatusAt = now;
                }
                if (eventAt < 0) {
                    for (Map<String, Object> e : listOf(get("/api/events?since=" + seq))) {
                        Map<String, Object> f = map(e.get("fields"));
                        if ("NODE_KILLED".equals(e.get("type")) && num(e.get("node")) == leader) {
                            killTrueMs = num(e.get("trueMs"));
                        }
                        if ("JOB_ORPHANED".equals(e.get("type")) && f != null && jobId.equals(f.get("jobId"))) {
                            eventAt = now;
                            eventTrueMs = num(e.get("trueMs"));
                        }
                    }
                }
                Thread.sleep(50);
            }
            if (eventAt >= 0) toEvent.add(eventAt);
            if (leaderAt >= 0) toLeader.add(leaderAt);
            ok &= eventAt >= 0;
            System.out.printf("run %d: killed leader %d during %s; seen by this client after the kill request: new agreed leader %s ms, "
                            + "job ORPHANED %s ms, JOB_ORPHANED event %s ms (event trueMs - NODE_KILLED trueMs = %s ms)%n",
                    r, leader, jobId, fmt(leaderAt), fmt(orphanStatusAt), fmt(eventAt),
                    eventTrueMs < 0 || killTrueMs < 0 ? "n/a" : String.valueOf(eventTrueMs - killTrueMs));
            post("/api/nodes/" + leader + "/restart");
        }
        waitReady();
        Collections.sort(toEvent);
        Collections.sort(toLeader);
        System.out.println();
        System.out.println("kill -> new agreed leader (ms, sorted): " + toLeader);
        System.out.println("kill -> JOB_ORPHANED event visible (ms, sorted): " + toEvent);
        System.out.println("OVERALL: " + (ok ? "PASS" : "FAIL") + " (" + toEvent.size() + "/" + runs + " runs produced JOB_ORPHANED)");
        return ok;
    }

    private static int completedSubtasks(Map<String, Object> job) {
        int n = 0;
        for (Map<String, Object> stage : listOf(job.get("stages"))) {
            for (Map<String, Object> s : listOf(stage.get("subtasks"))) {
                if ("COMPLETE".equals(s.get("status"))) n++;
            }
        }
        return n;
    }

    private static String fmt(long ms) {
        return ms < 0 ? "NEVER (15 s)" : String.valueOf(ms);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private String submit(String consistency, String policy) throws Exception {
        HttpResponse<String> res = post("/api/jobs", "{\"query\":\"" + QUERY + "\",\"policy\":\"" + policy
                + "\",\"consistency\":\"" + consistency + "\"}");
        if (res.statusCode() != 200) {
            throw new IllegalStateException("POST /api/jobs -> " + res.statusCode() + " " + res.body());
        }
        return (String) map(JsonUtil.parse(res.body())).get("jobId");
    }

    private Map<String, Object> waitDone(String jobId) throws Exception {
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
        Map<String, Object> job;
        do {
            Thread.sleep(100);
            job = map(get("/api/jobs/" + jobId));
        } while (("RUNNING".equals(job.get("status")) || "QUEUED".equals(job.get("status"))) && System.currentTimeMillis() < deadline);
        return job;
    }

    /** Ms until all 5 nodes are up with ready replicas and an agreed leader, or -1. */
    private long waitReady() throws Exception {
        long t0 = System.currentTimeMillis();
        while (System.currentTimeMillis() - t0 < READY_TIMEOUT_MS) {
            Map<String, Object> el = map(get("/api/election"));
            int ready = 0;
            for (Map<String, Object> n : listOf(map(get("/api/blackboard?prefix=none-flake")).get("nodes"))) {
                if (Boolean.TRUE.equals(n.get("up")) && Boolean.TRUE.equals(n.get("ready"))) ready++;
            }
            if (ready == 5 && Boolean.TRUE.equals(el.get("agreed")) && el.get("leaderId") != null) {
                return System.currentTimeMillis() - t0;
            }
            Thread.sleep(100);
        }
        return -1;
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

    private HttpResponse<String> post(String path) throws Exception {
        return post(path, "{}");
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object o) {
        return (Map<String, Object>) o;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> listOf(Object o) {
        return o == null ? Collections.emptyList() : (List<Map<String, Object>>) o;
    }

    private static long num(Object o) {
        return o == null ? -1 : ((Number) o).longValue();
    }
}
