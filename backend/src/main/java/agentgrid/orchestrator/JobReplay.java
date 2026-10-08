package agentgrid.orchestrator;

import agentgrid.common.Subtask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Rebuilds jobs from the replicated log. A new primary replays its whole log through this to get
 * back the jobs (their stages, their subtasks, who ran what and with which output); the jobs that
 * had not finished are then resumed by the JobRunner.
 *
 * Replay is idempotent per (jobId, subtaskId): a second SUBTASK_DONE for a subtask that is already
 * COMPLETE changes nothing, so a subtask re-dispatched after a failover is counted once.
 */
final class JobReplay {

    private JobReplay() {
    }

    /** The jobs the log describes, in submission order. */
    static Map<String, Job> rebuild(List<ReplEvent> log) {
        Map<String, Job> jobs = new LinkedHashMap<>();
        for (ReplEvent e : log) {
            apply(jobs, e);
        }
        return jobs;
    }

    static void apply(Map<String, Job> jobs, ReplEvent e) {
        if (e.getType() == ReplEvent.Type.JOB_SUBMITTED) {
            jobs.put(e.getJobId(), submitted(e));
            return;
        }
        Job job = jobs.get(e.getJobId());
        if (job == null) {
            return;   // an event of a job whose JOB_SUBMITTED is not in this log
        }
        switch (e.getType()) {
            case SUBTASK_DISPATCHED -> dispatched(job, e);
            case SUBTASK_DONE -> done(job, e);
            case JOB_DONE -> jobDone(job, e);
            case JOB_FAILED -> {
                synchronized (job) {
                    job.finished(JobStatus.FAILED, null, e.str("error"), e.getTrueMs());
                }
            }
            default -> {
            }
        }
    }

    private static Job submitted(ReplEvent e) {
        RetrievalPlan plan = RetrievalPlan.scan();
        if (RetrievalPlan.INDEX.equals(e.str("retrieval"))) {
            plan = RetrievalPlan.index(split(e.str("indexDocs")), e.str("indexBuiltAt"), split(e.str("queryTerms")));
        }
        return new Job(e.getJobId(), e.str("query"), e.str("policy"), e.str("consistency"),
                (int) e.num("leaderNode", 0), e.num("submittedMs", e.getTrueMs()), plan);
    }

    private static void dispatched(Job job, ReplEvent e) {
        JobStage stage = job.getGraph().stage(Subtask.Type.valueOf(e.str("stage")));
        synchronized (job) {
            if (job.getStatus() == JobStatus.QUEUED) {
                job.started(e.getTrueMs());
            }
            if (stage.getStatus() == JobStage.Status.PENDING) {
                stage.started(e.getTrueMs());
            }
            JobStage.SubtaskState s = stage.find(e.str("subtaskId"));
            if (s == null) {
                s = stage.add(e.str("subtaskId"), e.str("input"));
            }
            s.dispatched((int) e.num("node", 0), e.num("dispatchLamport", 0), e.getTrueMs());
        }
    }

    private static void done(Job job, ReplEvent e) {
        JobStage stage = job.getGraph().stage(Subtask.Type.valueOf(e.str("stage")));
        synchronized (job) {
            JobStage.SubtaskState s = stage.find(e.str("subtaskId"));
            if (s == null || s.getStatus() == JobStage.SubtaskStatus.COMPLETE) {
                return;   // unknown, or already counted
            }
            s.completed(e.num("resultLamport", 0), e.num("completeLamport", 0), e.getTrueMs(), e.str("output"),
                    Boolean.TRUE.equals(e.getPayload().get("ok")));
            if (e.str("bbKey") != null) {
                Long latency = e.getPayload().get("bbLatencyMs") instanceof Number ? e.num("bbLatencyMs", 0) : null;
                Long stamp = e.getPayload().get("bbStamp") instanceof Number ? e.num("bbStamp", 0) : null;
                Integer writer = e.getPayload().get("bbWriter") instanceof Number ? (int) e.num("bbWriter", 0) : null;
                s.blackboard(e.str("bbKey"), e.str("bbStatus"), latency, stamp, writer, e.str("bbMessage"));
            }
        }
    }

    private static void jobDone(Job job, ReplEvent e) {
        synchronized (job) {
            if ("RECOVERED".equals(e.str("finalStatus"))) {
                job.markRecovered((int) e.num("recoveredBy", job.getLeaderNode()));
            }
            for (JobStage stage : job.getGraph().getStages()) {
                if (!stage.getSubtasks().isEmpty()) {
                    stage.finished(stage.completedCount() == stage.getSubtasks().size(), e.getTrueMs());
                }
            }
            job.finished(JobStatus.COMPLETE, e.str("answer"), null, e.getTrueMs());
        }
    }

    private static List<String> split(String joined) {
        if (joined == null || joined.isEmpty()) {
            return new ArrayList<>();
        }
        return new ArrayList<>(Arrays.asList(joined.split(",")));
    }
}
