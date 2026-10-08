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
import java.util.TreeMap;

/**
 * End-to-end verifier for Exp 8: primary-backup replication of the orchestrator's job state.
 * Drives the control plane's HTTP API only (127.0.0.1:8080 must be running with the default 5 nodes
 * and -Dagentgrid.testHooks=true, which R4 needs to play a stale primary). Prints raw numbers for
 * every step and a PASS/FAIL table; exits non-zero if any check fails.
 *
 * R1 backup's log equals the primary's after a normal job (same seq, same events)
 * R2 kill the primary mid-job, replication ON: the job finishes RECOVERED, same answer as a
 *    no-failure run, no subtask counted twice
 * R3 the same kill with replication OFF: the job is ORPHANED (the old behaviour)
 * R4 the old primary restarts: it catches up and the backup steps down; lower-term writes are refused
 * R5 kill the backup: the primary keeps working, shows "no backup", and a new backup is chosen
 * R6 add and remove nodes: the backup is re-chosen and the log is re-synced
 *
 * R6 changes the membership (it ends with a 4-member cluster); run the other verifiers against a
 * fresh control plane afterwards.
 *
 * Usage (from backend/): java -cp build/classes agentgrid.control.ReplicationVerifier
 */
public class ReplicationVerifier {

    private static final String BASE = System.getProperty("agentgrid.verifier.base", "http://127.0.0.1:8080");
    private static final String QUERY = "How do leader election and failure detectors handle a crashed node?";
    private static final long JOB_TIMEOUT_MS = 45000;
    private static final long STABLE_TIMEOUT_MS = 40000;
    private static final int KILL_ATTEMPTS = 3;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final Map<String, Boolean> passed = new LinkedHashMap<>();
    private final Map<String, String> results = new LinkedHashMap<>();

    // Carried from R2 to R4.
    private long termAfterR2 = -1;
    private long lastSeqAfterR2 = -1;
    private int oldPrimary = -1;
    private int interimPrimary = -1;

    public static void main(String[] args) throws Exception {
        boolean ok = new ReplicationVerifier().run();
        System.exit(ok ? 0 : 1);
    }

    private boolean run() throws Exception {
        line("=====================================================================");
        line(" AgentGrid-Lite Exp 8 replication verifier (HTTP API only) -> " + BASE);
        line("=====================================================================");
        line("modules: " + get("/api/modules"));
        ensureCluster();

        r1();
        r2();
        r4();
        r3();
        r5();
        r6();

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

    // =========================================================================
    // R1: the backup's log equals the primary's after a normal job
    // =========================================================================

    private void r1() throws Exception {
        header("R1 a normal job: the backup's log equals the primary's (same seq, same events)");
        Map<String, Object> job = runJob("WEIGHTED");
        line("job " + job.get("jobId") + " " + job.get("status") + " " + job.get("completedSubtasks") + "/" + job.get("subtasks")
                + " subtasks, makespan " + job.get("makespanMs") + " ms, replicated=" + job.get("replicated"));
        Map<String, Object> v = waitSync(null, null, 10000);
        if (v == null) {
            check("R1 backup log equals primary log", false, "no synced primary/backup within 10 s: " + view());
            return;
        }
        int primary = (int) num(v.get("primary"));
        int backup = (int) num(v.get("backup"));
        line("primary node " + primary + " term " + v.get("term") + " lastSeq " + v.get("lastSeq") + "; backup node " + backup
                + " lastSeq " + v.get("backupLastSeq") + "; lag " + v.get("lag"));
        List<String> lp = logKeys(primary);
        List<String> lb = logKeys(backup);
        String diff = diff(lp, lb);
        line("primary log: " + lp.size() + " events; backup log: " + lb.size() + " events; first difference: " + (diff == null ? "none" : diff));

        String jobId = (String) job.get("jobId");
        Map<String, Integer> byType = new TreeMap<>();
        boolean contiguous = true;
        long expectSeq = 1;
        for (String k : lp) {
            String[] f = k.split("\\|", -1);
            contiguous &= Long.parseLong(f[0]) == expectSeq++;
            if (f[2].equals(jobId)) {
                byType.merge(f[3], 1, Integer::sum);
            }
        }
        line("events of " + jobId + " in the log: " + byType + "; seq contiguous 1.." + lp.size() + ": " + contiguous);
        long subtasks = num(job.get("subtasks"));
        boolean shape = byType.getOrDefault("JOB_SUBMITTED", 0) == 1
                && byType.getOrDefault("SUBTASK_DONE", 0) == subtasks
                && byType.getOrDefault("SUBTASK_DISPATCHED", 0) >= subtasks
                && byType.getOrDefault("JOB_DONE", 0) == 1;
        boolean ok = diff == null && lp.size() > 0 && contiguous && shape && "COMPLETE".equals(job.get("status"))
                && num(v.get("lastSeq")) == num(v.get("backupLastSeq"));
        check("R1 backup log equals primary log", ok, lp.size() + " events, identical on primary " + primary + " and backup "
                + backup + " (seq, term, job, type, subtask, digest); job events " + byType);
    }

    // =========================================================================
    // R2: kill the primary mid-job, replication ON
    // =========================================================================

    private void r2() throws Exception {
        header("R2 kill the primary mid-job (replication ON): the job finishes RECOVERED, same result, no subtask counted twice");
        for (int attempt = 1; attempt <= KILL_ATTEMPTS; attempt++) {
            ensureCluster();
            Map<String, Object> base = runJob("ROUND_ROBIN");
            String baseAnswer = (String) base.get("answer");
            line("no-failure run: " + base.get("jobId") + " " + base.get("status") + " " + base.get("completedSubtasks") + "/"
                    + base.get("subtasks") + ", answer " + (baseAnswer == null ? "none" : baseAnswer.length() + " chars"));
            Map<String, Object> v0 = view();
            int primary = (int) num(v0.get("primary"));
            long termBefore = num(v0.get("term"));

            Map<String, Object> resp = map(post("/api/jobs", "{\"query\":\"" + QUERY + "\",\"policy\":\"ROUND_ROBIN\"}"));
            String jobId = (String) resp.get("jobId");
            Map<String, Object> job = waitFirstSubtask(jobId);
            line("attempt " + attempt + ": " + jobId + " on primary " + primary + " term " + termBefore + ": " + job.get("status")
                    + ", " + job.get("completedSubtasks") + "/" + job.get("subtasks") + " subtasks created; killing the primary");
            post("/api/nodes/" + primary + "/kill", "{}");

            job = waitFinal(jobId, JOB_TIMEOUT_MS);
            line("after the kill: status=" + job.get("status") + " recovered=" + job.get("recovered") + " currentLeader="
                    + job.get("currentLeader") + " " + job.get("completedSubtasks") + "/" + job.get("subtasks") + " subtasks");
            if (!"RECOVERED".equals(job.get("status"))) {
                line("the job was " + job.get("status") + ", not RECOVERED (it may have finished before the kill landed): "
                        + (attempt < KILL_ATTEMPTS ? "retrying" : "giving up"));
                if (attempt < KILL_ATTEMPTS) {
                    continue;
                }
                check("R2 kill primary -> RECOVERED", false, jobId + " ended " + job.get("status"));
                return;
            }

            Map<String, Object> el = election();
            int newLeader = (int) num(el.get("leaderId"));
            Map<String, Object> v1 = waitSync(newLeader, null, 15000);
            line("new primary node " + newLeader + ", view " + (v1 == null ? view() : summary(v1)));
            long newTerm = v1 == null ? -1 : num(v1.get("term"));
            List<String> lg = logKeys(newLeader);
            Map<String, Integer> donePerSubtask = new TreeMap<>();
            boolean newTermSeen = false;
            for (String k : lg) {
                String[] f = k.split("\\|", -1);
                if (f[2].equals(jobId)) {
                    if (f[3].equals("SUBTASK_DONE")) {
                        donePerSubtask.merge(f[4], 1, Integer::sum);
                    }
                    newTermSeen |= Long.parseLong(f[1]) == newTerm;
                }
            }
            int dup = 0;
            for (int n : donePerSubtask.values()) {
                if (n > 1) {
                    dup++;
                }
            }
            long subtasks = num(job.get("subtasks"));
            long perNode = 0;
            for (Object o : map(job.get("subtasksPerNode")).values()) {
                perNode += num(o);
            }
            List<String> redispatched = new ArrayList<>();
            for (Map<String, Object> stage : listOf(job.get("stages"))) {
                for (Map<String, Object> s : listOf(stage.get("subtasks"))) {
                    if (num(s.get("attempts")) > 1) {
                        redispatched.add(s.get("subtaskId") + " x" + s.get("attempts"));
                    }
                }
            }
            line("SUBTASK_DONE per subtask in the new primary's log: " + donePerSubtask.size() + " subtasks, duplicates: " + dup
                    + "; job.subtasks=" + subtasks + ", completed on nodes " + job.get("subtasksPerNode") + " (sum " + perNode
                    + "); re-dispatched after the failover: " + redispatched);
            line("answer identical to the no-failure run: " + (baseAnswer != null && baseAnswer.equals(job.get("answer"))));

            boolean recoveringEvent = false;
            boolean orphanEvent = false;
            for (int i = 0; i < 40 && !recoveringEvent; i++) {
                for (Map<String, Object> e : eventsSince(0)) {
                    Map<String, Object> f = map(e.get("fields"));
                    if (f == null || !jobId.equals(f.get("jobId"))) {
                        continue;
                    }
                    if ("JOB_RECOVERING".equals(e.get("type")) && num(e.get("node")) == newLeader) {
                        recoveringEvent = true;
                    }
                    if ("JOB_ORPHANED".equals(e.get("type"))) {
                        orphanEvent = true;
                    }
                }
                if (!recoveringEvent) {
                    Thread.sleep(250);
                }
            }
            line("JOB_RECOVERING recorded by node " + newLeader + ": " + recoveringEvent + "; JOB_ORPHANED for this job: " + orphanEvent);

            boolean ok = "RECOVERED".equals(job.get("status")) && Boolean.TRUE.equals(job.get("recovered"))
                    && num(job.get("currentLeader")) == newLeader && newLeader != primary
                    && num(job.get("completedSubtasks")) == subtasks
                    && baseAnswer != null && baseAnswer.equals(job.get("answer"))
                    && dup == 0 && donePerSubtask.size() == subtasks && perNode == subtasks
                    && newTerm > termBefore && newTermSeen && recoveringEvent && !orphanEvent;
            check("R2 kill primary -> RECOVERED, same result", ok, jobId + " RECOVERED by node " + newLeader + " at term " + newTerm
                    + " (was " + termBefore + "), " + subtasks + "/" + subtasks + " subtasks, answer identical to the no-failure run, "
                    + dup + " subtasks counted twice, " + redispatched.size() + " re-dispatched");
            oldPrimary = primary;
            interimPrimary = newLeader;
            termAfterR2 = newTerm;
            lastSeqAfterR2 = v1 == null ? -1 : num(v1.get("lastSeq"));
            return;
        }
    }

    // =========================================================================
    // R4: the old primary restarts (run right after R2, which killed it)
    // =========================================================================

    private void r4() throws Exception {
        header("R4 the old primary restarts: it catches up from the log, the interim primary steps down, lower-term writes are refused");
        if (oldPrimary < 0) {
            check("R4 old primary restarts, stale writes refused", false, "R2 did not complete, nothing to restart");
            return;
        }
        post("/api/nodes/" + oldPrimary + "/restart", "{}");
        Map<String, Object> v = waitSync(null, null, STABLE_TIMEOUT_MS);
        if (v == null) {
            check("R4 old primary restarts, stale writes refused", false, "no synced primary/backup within " + STABLE_TIMEOUT_MS + " ms: " + view());
            return;
        }
        int primary = (int) num(v.get("primary"));
        int backup = (int) num(v.get("backup"));
        line("after the restart: " + summary(v));
        Map<String, Object> mine = nodeEntry(v, oldPrimary);
        Map<String, Object> interim = nodeEntry(v, interimPrimary);
        line("restarted node " + oldPrimary + ": role " + mine.get("role") + ", term " + mine.get("term") + ", lastSeq " + mine.get("lastSeq")
                + " (log was last seen at seq " + lastSeqAfterR2 + " on the interim primary, term " + termAfterR2 + ")");
        line("interim primary node " + interimPrimary + ": role " + interim.get("role") + ", term " + interim.get("term") + ", lastSeq " + interim.get("lastSeq"));
        List<String> lp = logKeys(primary);
        List<String> lb = logKeys(backup);
        String diff = diff(lp, lb);
        line("primary " + primary + " log " + lp.size() + " events, backup " + backup + " log " + lb.size() + " events, first difference: "
                + (diff == null ? "none" : diff));
        boolean caughtUp = num(mine.get("lastSeq")) >= lastSeqAfterR2 && lp.size() >= lastSeqAfterR2;
        boolean steppedDown = !"PRIMARY".equals(interim.get("role")) || interimPrimary == primary;
        boolean newerTerm = num(v.get("term")) > termAfterR2;

        // A job now, so every worker has seen the new term; then play a stale primary against each node.
        Map<String, Object> job = runJob("WEIGHTED");
        line("job after the restart: " + job.get("jobId") + " " + job.get("status") + " " + job.get("completedSubtasks") + "/" + job.get("subtasks"));
        waitSync(null, null, 10000);
        // The stale primary is the interim one that was just replaced (its term is termAfterR2). Its event is only
        // refusable by a node whose term is above that; an idle member still at a lower term would take it, so
        // the event goes only to nodes above the stale term, and the dispatch to every worker.
        long staleTerm = termAfterR2;
        int tested = 0;
        int eventsTested = 0;
        int refusedDispatch = 0;
        int refusedWrites = 0;
        boolean allWritesRefused = true;
        boolean hooks = true;
        for (Map<String, Object> n : listOf(view().get("nodes"))) {
            if (!Boolean.TRUE.equals(n.get("up"))) {
                continue;
            }
            boolean sendEvent = num(n.get("term")) > staleTerm;
            HttpResponse<String> r = postStatus("/api/replication/test/stale", "{\"node\":" + n.get("node") + ",\"term\":" + staleTerm
                    + ",\"event\":" + sendEvent + "}");
            if (r.statusCode() == 404) {
                hooks = false;
                line("POST /api/replication/test/stale -> 404: " + r.body());
                break;
            }
            Map<String, Object> t = map(r.body());
            Map<String, Object> rep = map(t.get("replicate"));
            Map<String, Object> dis = map(t.get("dispatch"));
            String eventText = "not sent (node term " + n.get("term") + " is not above " + staleTerm + ")";
            if (sendEvent) {
                boolean writeRefused = "STALE_TERM".equals(rep.get("status")) && num(rep.get("lastSeqAfter")) == num(rep.get("lastSeqBefore"))
                        && num(rep.get("staleRejectedAfter")) == num(rep.get("staleRejectedBefore")) + 1;
                eventText = rep.get("status") + " (node term " + rep.get("nodeTerm") + ", lastSeq " + rep.get("lastSeqBefore") + " -> "
                        + rep.get("lastSeqAfter") + ", refused " + rep.get("staleRejectedBefore") + " -> " + rep.get("staleRejectedAfter") + ")";
                allWritesRefused &= writeRefused;
                refusedWrites += writeRefused ? 1 : 0;
                eventsTested++;
            }
            line("node " + n.get("node") + ": stale event (term " + staleTerm + "): " + eventText + "; stale dispatch (term " + staleTerm + "): "
                    + (Boolean.TRUE.equals(dis.get("refused")) ? "REFUSED" : "accepted") + " (worker highest term " + dis.get("highestTerm") + ")"
                    + (dis.get("error") == null ? "" : " - " + dis.get("error")));
            tested++;
            if (num(dis.get("highestTerm")) > staleTerm) {
                if (Boolean.TRUE.equals(dis.get("refused"))) {
                    refusedDispatch++;
                } else {
                    allWritesRefused = false;
                }
            }
        }
        boolean ok = hooks && caughtUp && steppedDown && newerTerm && diff == null && "COMPLETE".equals(job.get("status"))
                && allWritesRefused && eventsTested >= 2 && tested >= 3 && refusedDispatch >= 3;
        check("R4 old primary restarts, stale writes refused", ok, hooks
                ? "node " + oldPrimary + " back as " + mine.get("role") + " at term " + v.get("term") + " (> " + termAfterR2 + "), caught up to seq "
                + mine.get("lastSeq") + " (>= " + lastSeqAfterR2 + "), node " + interimPrimary + " is " + interim.get("role") + "; logs identical; "
                + refusedWrites + "/" + eventsTested + " stale events (term " + staleTerm + ") refused by the nodes above that term, "
                + refusedDispatch + "/" + tested + " stale dispatches refused"
                : "the control plane has test hooks disabled; start it with -Dagentgrid.testHooks=true");
    }

    // =========================================================================
    // R3: the same kill with replication OFF
    // =========================================================================

    private void r3() throws Exception {
        header("R3 kill the primary mid-job with replication OFF: the job is ORPHANED (the old behaviour)");
        ensureCluster();
        post("/api/replication/toggle", "{\"enabled\":false}");
        Map<String, Object> v = view();
        line("replication switched OFF: enabled=" + v.get("enabled") + ", warning: " + v.get("warning"));
        try {
            for (int attempt = 1; attempt <= KILL_ATTEMPTS; attempt++) {
                int primary = (int) num(election().get("leaderId"));
                Map<String, Object> resp = map(post("/api/jobs", "{\"query\":\"" + QUERY + "\",\"policy\":\"ROUND_ROBIN\"}"));
                String jobId = (String) resp.get("jobId");
                Map<String, Object> job = waitFirstSubtask(jobId);
                line("attempt " + attempt + ": " + jobId + " on primary " + primary + ": " + job.get("status") + ", " + job.get("completedSubtasks")
                        + "/" + job.get("subtasks") + " subtasks created; killing the primary");
                long seqBefore = lastEventSeq();
                post("/api/nodes/" + primary + "/kill", "{}");
                Map<String, Object> orphan = null;
                long deadline = System.currentTimeMillis() + 25000;
                while (System.currentTimeMillis() < deadline) {
                    orphan = map(get("/api/jobs/" + jobId));
                    if (!"RUNNING".equals(orphan.get("status")) && !"QUEUED".equals(orphan.get("status"))
                            && (!"ORPHANED".equals(orphan.get("status")) || orphan.get("orphanAdoptedBy") != null)) {
                        break;
                    }
                    Thread.sleep(100);
                }
                long newLeader = num(election().get("leaderId"));
                line("after the kill: status=" + orphan.get("status") + " completed " + orphan.get("completedSubtasks") + "/" + orphan.get("subtasks")
                        + ", recorded as orphan by node " + orphan.get("orphanAdoptedBy") + ", new leader " + newLeader);
                if (!"ORPHANED".equals(orphan.get("status")) && attempt < KILL_ATTEMPTS) {
                    line("the job was " + orphan.get("status") + " (finished before the kill landed): retrying");
                    ensureClusterKeepOff();
                    continue;
                }
                boolean orphanEvent = false;
                for (int i = 0; i < 40 && !orphanEvent; i++) {
                    for (Map<String, Object> e : eventsSince(seqBefore)) {
                        Map<String, Object> f = map(e.get("fields"));
                        if ("JOB_ORPHANED".equals(e.get("type")) && f != null && jobId.equals(f.get("jobId")) && num(e.get("node")) == newLeader) {
                            orphanEvent = true;
                        }
                    }
                    if (!orphanEvent) {
                        Thread.sleep(250);
                    }
                }
                long retained = num(orphan.get("completedSubtasks"));
                boolean ok = "ORPHANED".equals(orphan.get("status")) && orphan.get("orphanAdoptedBy") != null && retained >= 1
                        && orphanEvent && newLeader != primary && !Boolean.TRUE.equals(orphan.get("recovered"));
                check("R3 replication OFF -> ORPHANED", ok, jobId + " " + orphan.get("status") + " with " + retained
                        + " completed subtasks retained; JOB_ORPHANED recorded by new leader node " + newLeader + ": " + orphanEvent);
                return;
            }
        } finally {
            post("/api/replication/toggle", "{\"enabled\":true}");
            line("replication switched back ON");
            ensureCluster();
        }
    }

    // =========================================================================
    // R5: kill the backup
    // =========================================================================

    private void r5() throws Exception {
        header("R5 kill the backup: the primary keeps working, a new backup is chosen; with no live backup it shows \"no backup\"");
        ensureCluster();
        Map<String, Object> v = view();
        int primary = (int) num(v.get("primary"));
        int backup = (int) num(v.get("backup"));
        line("before: " + summary(v));
        post("/api/nodes/" + backup + "/kill", "{}");
        Map<String, Object> job = runJob("WEIGHTED");
        line("job right after killing backup " + backup + ": " + job.get("jobId") + " " + job.get("status") + " " + job.get("completedSubtasks") + "/"
                + job.get("subtasks") + ", replicated=" + job.get("replicated"));
        Map<String, Object> v2 = waitSync(primary, null, 20000);
        int newBackup = v2 == null ? -1 : (int) num(v2.get("backup"));
        line("a new backup: " + (v2 == null ? "none within 20 s: " + view() : summary(v2)));
        String diffA = v2 == null ? "no view" : diff(logKeys(primary), logKeys(newBackup));
        line("primary " + primary + " vs new backup " + newBackup + " logs: first difference " + (diffA == null ? "none" : diffA));
        boolean partA = "COMPLETE".equals(job.get("status")) && v2 != null && newBackup != backup && newBackup > 0 && diffA == null;
        check("R5a backup killed -> new backup chosen", partA, "primary " + primary + " finished job " + job.get("jobId") + " " + job.get("status")
                + "; backup " + backup + " -> " + newBackup + ", logs identical");

        // Now leave the primary alone: kill every other node.
        for (Map<String, Object> n : listOf(view().get("nodes"))) {
            int id = (int) num(n.get("node"));
            if (id != primary && Boolean.TRUE.equals(n.get("up"))) {
                post("/api/nodes/" + id + "/kill", "{}");
            }
        }
        Map<String, Object> alone = null;
        long deadline = System.currentTimeMillis() + 20000;
        while (System.currentTimeMillis() < deadline) {
            alone = view();
            if (Boolean.TRUE.equals(alone.get("noBackup")) && alone.get("warning") != null) {
                break;
            }
            Thread.sleep(300);
        }
        line("every other node killed: noBackup=" + alone.get("noBackup") + ", warning: " + alone.get("warning"));
        Map<String, Object> solo = runJob("WEIGHTED");
        line("job with no backup: " + solo.get("jobId") + " " + solo.get("status") + " " + solo.get("completedSubtasks") + "/" + solo.get("subtasks")
                + ", replicated=" + solo.get("replicated"));
        boolean partB = Boolean.TRUE.equals(alone.get("noBackup")) && String.valueOf(alone.get("warning")).contains("no backup")
                && "COMPLETE".equals(solo.get("status")) && num(solo.get("completedSubtasks")) == num(solo.get("subtasks"))
                && Boolean.FALSE.equals(solo.get("replicated"));
        check("R5b no live backup -> \"no backup\", job still runs", partB, "warning \"" + alone.get("warning") + "\"; job " + solo.get("jobId")
                + " " + solo.get("status") + " unreplicated");

        // Bring the others back: a backup is chosen again and the whole log is shipped to it.
        for (int id : new int[] {1, 2, 3, 4}) {
            post("/api/nodes/" + id + "/restart", "{}");
        }
        Map<String, Object> back = waitSync(primary, null, STABLE_TIMEOUT_MS);
        int resynced = back == null ? -1 : (int) num(back.get("backup"));
        line("after restarting 1-4: " + (back == null ? "no backup within " + STABLE_TIMEOUT_MS + " ms: " + view() : summary(back)));
        String diffC = back == null ? "no view" : diff(logKeys(primary), logKeys(resynced));
        line("primary " + primary + " vs backup " + resynced + " logs: first difference " + (diffC == null ? "none" : diffC));
        waitAgreed(primary, 5);
        check("R5c backup returns -> log re-synced", back != null && diffC == null && Boolean.FALSE.equals(back.get("noBackup")),
                "backup " + resynced + " chosen again, log of " + (back == null ? "?" : back.get("lastSeq")) + " events identical to the primary's");
    }

    // =========================================================================
    // R6: add and remove nodes
    // =========================================================================

    private void r6() throws Exception {
        header("R6 add and remove nodes: the backup is re-chosen and the log is re-synced");
        ensureCluster();
        Map<String, Object> v0 = view();
        int primary0 = (int) num(v0.get("primary"));
        List<String> before = logKeys(primary0);
        line("before: " + summary(v0) + "; primary log " + before.size() + " events");

        // (a) add a node: it becomes the highest id, is elected, catches up, and the old primary becomes its backup.
        Map<String, Object> added = map(post("/api/cluster/nodes", "{}"));
        int newId = (int) num(added.get("node"));
        line("added node " + newId + " (epoch " + map(added.get("membership")).get("epoch") + ", quorum " + map(added.get("membership")).get("quorum") + ")");
        Map<String, Object> va = waitAgreed(newId, 6) == null ? null : waitSync(newId, null, STABLE_TIMEOUT_MS);
        line("after the add: " + (va == null ? "no synced primary/backup: " + view() : summary(va)));
        List<String> afterAdd = va == null ? List.of() : logKeys(newId);
        int backupA = va == null ? -1 : (int) num(va.get("backup"));
        String diffA = va == null ? "no view" : diff(logKeys(newId), logKeys(backupA));
        boolean prefixA = isPrefix(before, afterAdd);
        line("new primary " + newId + " log " + afterAdd.size() + " events (earlier " + before.size() + " kept: " + prefixA + "); vs backup " + backupA
                + " first difference " + (diffA == null ? "none" : diffA));
        Map<String, Object> jobA = runJob("WEIGHTED");
        waitSync(newId, null, 10000);
        String diffA2 = diff(logKeys(newId), logKeys(backupA));
        line("job on the new primary: " + jobA.get("jobId") + " " + jobA.get("status") + "; logs after it: first difference " + (diffA2 == null ? "none" : diffA2));
        boolean okA = va != null && num(va.get("term")) > num(v0.get("term")) && prefixA && diffA == null && diffA2 == null
                && "COMPLETE".equals(jobA.get("status")) && backupA > 0;
        check("R6a add node -> backup re-chosen, log re-synced", okA, "node " + newId + " primary at term " + (va == null ? "?" : va.get("term"))
                + " (was " + v0.get("term") + "), backup " + backupA + ", " + afterAdd.size() + " events identical, earlier " + before.size() + " kept");

        // (b) remove the backup: the primary picks another and re-syncs.
        List<String> beforeB = logKeys(newId);
        Map<String, Object> remB = map(post("/api/cluster/nodes/" + backupA + "/remove", "{}"));
        Map<String, Object> vb = waitSync(newId, null, STABLE_TIMEOUT_MS);
        int backupB = vb == null ? -1 : (int) num(vb.get("backup"));
        String diffB = vb == null ? "no view" : diff(logKeys(newId), logKeys(backupB));
        line("removed backup node " + backupA + " (epoch " + map(remB.get("membership")).get("epoch") + "): " + (vb == null ? "no backup: " + view() : summary(vb))
                + "; first difference vs the new backup " + (diffB == null ? "none" : diffB));
        boolean okB = vb != null && backupB != backupA && backupB > 0 && diffB == null && isPrefix(beforeB, logKeys(newId));
        check("R6b remove the backup -> new backup, log re-synced", okB, "backup " + backupA + " removed; backup " + backupB
                + " chosen with an identical log of " + (vb == null ? "?" : vb.get("lastSeq")) + " events");

        // (c) remove the primary: the next node is elected, takes a newer term and keeps the log.
        List<String> beforeC = logKeys(newId);
        Map<String, Object> remC = map(post("/api/cluster/nodes/" + newId + "/remove", "{}"));
        line("removed primary node " + newId + " (epoch " + map(remC.get("membership")).get("epoch") + ")");
        Map<String, Object> vc = waitSync(null, null, STABLE_TIMEOUT_MS);
        int primaryC = vc == null ? -1 : (int) num(vc.get("primary"));
        int backupC = vc == null ? -1 : (int) num(vc.get("backup"));
        line("after removing the primary: " + (vc == null ? "no synced primary/backup: " + view() : summary(vc)));
        Map<String, Object> jobC = runJob("WEIGHTED");
        waitSync(primaryC, null, 10000);
        String diffC = vc == null ? "no view" : diff(logKeys(primaryC), logKeys(backupC));
        boolean prefixC = vc != null && isPrefix(beforeC, logKeys(primaryC));
        line("new primary " + primaryC + ": earlier " + beforeC.size() + " events kept: " + prefixC + "; job " + jobC.get("jobId") + " " + jobC.get("status")
                + "; logs vs backup " + backupC + " first difference " + (diffC == null ? "none" : diffC));
        long termB = vb == null ? Long.MAX_VALUE : num(vb.get("term"));
        boolean okC = vc != null && primaryC != newId && backupC > 0 && num(vc.get("term")) > termB
                && prefixC && diffC == null && "COMPLETE".equals(jobC.get("status"));
        check("R6c remove the primary -> new primary keeps the log", okC, "node " + primaryC + " primary at term " + (vc == null ? "?" : vc.get("term"))
                + " (was " + (vb == null ? "?" : vb.get("term")) + "), backup " + backupC + ", earlier " + beforeC.size()
                + " events kept, logs identical");
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static boolean isPrefix(List<String> shorter, List<String> longer) {
        return longer.size() >= shorter.size() && longer.subList(0, shorter.size()).equals(shorter);
    }

    /** First difference between two logs as text, or null if they are identical. */
    private static String diff(List<String> a, List<String> b) {
        int n = Math.min(a.size(), b.size());
        for (int i = 0; i < n; i++) {
            if (!a.get(i).equals(b.get(i))) {
                return "index " + i + ": " + a.get(i) + "  vs  " + b.get(i);
            }
        }
        return a.size() == b.size() ? null : "length " + a.size() + " vs " + b.size();
    }

    /** One line per log event: seq|term|jobId|type|subtaskId|digest. */
    private List<String> logKeys(int node) throws Exception {
        List<String> out = new ArrayList<>();
        for (Map<String, Object> e : listOf(map(get("/api/replication/log?node=" + node)).get("events"))) {
            out.add(e.get("seq") + "|" + e.get("term") + "|" + e.get("jobId") + "|" + e.get("type") + "|" + e.get("subtaskId") + "|" + e.get("digest"));
        }
        return out;
    }

    private Map<String, Object> view() throws Exception {
        return map(get("/api/replication"));
    }

    private static String summary(Map<String, Object> v) {
        return "primary " + v.get("primary") + " backup " + v.get("backup") + " term " + v.get("term") + " lastSeq " + v.get("lastSeq")
                + " backupLastSeq " + v.get("backupLastSeq") + " lag " + v.get("lag") + " noBackup " + v.get("noBackup");
    }

    private static Map<String, Object> nodeEntry(Map<String, Object> v, int node) {
        for (Map<String, Object> n : listOf(v.get("nodes"))) {
            if (num(n.get("node")) == node) {
                return n;
            }
        }
        return new LinkedHashMap<>();
    }

    /**
     * Waits for a view with an agreed, ready primary (node primary if non-null) and a backup (node backup
     * if non-null) whose last seq equals the primary's; null on timeout.
     */
    private Map<String, Object> waitSync(Integer primary, Integer backup, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Map<String, Object> v = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                v = view();
                if (v.get("primary") != null && v.get("backup") != null && Boolean.TRUE.equals(v.get("ready"))
                        && (primary == null || num(v.get("primary")) == primary) && (backup == null || num(v.get("backup")) == backup)
                        && v.get("lag") != null && num(v.get("lag")) == 0 && !Boolean.TRUE.equals(v.get("noBackup"))) {
                    return v;
                }
            } catch (Exception ignored) {
                // the control plane may be busy for a moment
            }
            Thread.sleep(200);
        }
        return null;
    }

    private Map<String, Object> waitFirstSubtask(String jobId) throws Exception {
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
        Map<String, Object> job = null;
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
        return job;
    }

    private Map<String, Object> waitFinal(String jobId, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Map<String, Object> job = null;
        while (System.currentTimeMillis() < deadline) {
            job = map(get("/api/jobs/" + jobId));
            if (!"RUNNING".equals(job.get("status")) && !"QUEUED".equals(job.get("status"))) {
                // an ORPHANED record may be provisional: it is final once the new leader has adopted it
                if (!"ORPHANED".equals(job.get("status")) || job.get("orphanAdoptedBy") != null) {
                    return job;
                }
            }
            Thread.sleep(100);
        }
        return job;
    }

    private Map<String, Object> runJob(String policy) throws Exception {
        Map<String, Object> resp = map(post("/api/jobs", "{\"query\":\"" + QUERY + "\",\"policy\":\"" + policy + "\"}"));
        return waitFinal((String) resp.get("jobId"), JOB_TIMEOUT_MS);
    }

    /** All members up, an agreed leader, replication ON and a synced backup (restarts any node that is down). */
    private void ensureCluster() throws Exception {
        ensureClusterKeepOff();
        if (!Boolean.TRUE.equals(view().get("enabled"))) {
            post("/api/replication/toggle", "{\"enabled\":true}");
        }
        Map<String, Object> v = waitSync(null, null, STABLE_TIMEOUT_MS);
        line("cluster: " + (v == null ? "NO synced primary/backup: " + view() : summary(v)));
    }

    private void ensureClusterKeepOff() throws Exception {
        List<Map<String, Object>> nodes = listOf(get("/api/cluster"));
        for (Map<String, Object> n : nodes) {
            if (!Boolean.TRUE.equals(n.get("up"))) {
                post("/api/nodes/" + num(n.get("id")) + "/restart", "{}");
            }
        }
        Map<String, Object> el = waitAgreed(-1, nodes.size());
        line("cluster: " + nodes.size() + " nodes, " + (el == null ? "NOT agreed" : "agreed leader " + el.get("leaderId")));
    }

    /** Waits until upCount nodes are up and agree on a leader (leader -1: any). */
    private Map<String, Object> waitAgreed(int leader, int upCount) throws Exception {
        long deadline = System.currentTimeMillis() + STABLE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                Map<String, Object> el = election();
                int up = 0;
                for (Map<String, Object> n : listOf(get("/api/cluster"))) {
                    if (Boolean.TRUE.equals(n.get("up"))) {
                        up++;
                    }
                }
                if (up == upCount && Boolean.TRUE.equals(el.get("agreed")) && (leader < 0 || num(el.get("leaderId")) == leader)) {
                    return el;
                }
            } catch (Exception ignored) {
                // retry
            }
            Thread.sleep(150);
        }
        return null;
    }

    private Map<String, Object> election() throws Exception {
        return map(get("/api/election"));
    }

    private long lastEventSeq() throws Exception {
        long max = 0;
        for (Map<String, Object> e : eventsSince(0)) {
            max = Math.max(max, num(e.get("seq")));
        }
        return max;
    }

    private List<Map<String, Object>> eventsSince(long seq) throws Exception {
        return listOf(get("/api/events?since=" + seq));
    }

    private Object get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path)).timeout(Duration.ofSeconds(20)).GET().build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) {
            throw new IllegalStateException("GET " + path + " -> " + res.statusCode() + " " + res.body());
        }
        return JsonUtil.parse(res.body());
    }

    private HttpResponse<String> postStatus(String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private String post(String path, String body) throws Exception {
        HttpResponse<String> res = postStatus(path, body);
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
        return o == null ? new LinkedHashMap<>() : (Map<String, Object>) o;
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

    private static void line(String s) {
        System.out.println(s);
    }
}
