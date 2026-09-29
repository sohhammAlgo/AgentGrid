package agentgrid.orchestrator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * A research job: a query run through the four-stage pipeline on the leader's orchestrator.
 * The runner mutates it under its monitor; everyone else works on copy().
 */
public class Job implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobId;
    private final String query;
    private final String policy;
    private final int leaderNode;
    private final long submittedTrueMs;
    private final TaskGraph graph = new TaskGraph();
    private JobStatus status = JobStatus.QUEUED;
    private Long startTrueMs;
    private Long endTrueMs;
    private String answer;
    private String error;
    private Integer orphanAdoptedBy;
    private final TreeMap<Integer, Integer> peakQueueDepth = new TreeMap<>();

    public Job(String jobId, String query, String policy, int leaderNode, long submittedTrueMs) {
        this.jobId = jobId;
        this.query = query;
        this.policy = policy;
        this.leaderNode = leaderNode;
        this.submittedTrueMs = submittedTrueMs;
    }

    public String getJobId() { return jobId; }
    public String getQuery() { return query; }
    public String getPolicy() { return policy; }
    public int getLeaderNode() { return leaderNode; }
    public synchronized JobStatus getStatus() { return status; }
    public synchronized String getAnswer() { return answer; }
    public synchronized String getError() { return error; }
    public TaskGraph getGraph() { return graph; }

    synchronized void started(long trueMs) {
        status = JobStatus.RUNNING;
        startTrueMs = trueMs;
    }

    synchronized void finished(JobStatus finalStatus, String answer, String error, long trueMs) {
        if (status == JobStatus.ORPHANED) {
            return;
        }
        this.status = finalStatus;
        this.answer = answer;
        this.error = error;
        this.endTrueMs = trueMs;
    }

    /** Marks an unfinished job ORPHANED; completed subtasks stay as they are. False if already final. */
    public synchronized boolean orphan(long trueMs) {
        if (!status.isActive()) {
            return false;
        }
        status = JobStatus.ORPHANED;
        endTrueMs = trueMs;
        return true;
    }

    synchronized void adoptedBy(int nodeId) {
        orphanAdoptedBy = nodeId;
    }

    synchronized void peakQueueDepth(Map<Integer, Integer> peaks) {
        peakQueueDepth.clear();
        peakQueueDepth.putAll(peaks);
    }

    public synchronized Long makespanMs() {
        return startTrueMs != null && endTrueMs != null ? endTrueMs - startTrueMs : null;
    }

    /** Deep copy taken under the monitor, safe to send or render while the job runs. */
    public synchronized Job copy() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(this);
            }
            try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                return (Job) in.readObject();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Subtasks run per node, over all stages. */
    public synchronized Map<Integer, Integer> subtasksPerNode() {
        Map<Integer, Integer> counts = new TreeMap<>();
        for (JobStage stage : graph.getStages()) {
            for (JobStage.SubtaskState s : stage.getSubtasks()) {
                if (s.getNode() != null && s.getStatus() == JobStage.SubtaskStatus.COMPLETE) {
                    counts.merge(s.getNode(), 1, Integer::sum);
                }
            }
        }
        return counts;
    }

    public synchronized Map<String, Object> toSummaryMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("jobId", jobId);
        m.put("query", query);
        m.put("policy", policy);
        m.put("status", status.name());
        m.put("leaderNode", leaderNode);
        m.put("submittedTrueMs", submittedTrueMs);
        m.put("startTrueMs", startTrueMs);
        m.put("endTrueMs", endTrueMs);
        m.put("makespanMs", makespanMs());
        m.put("subtasks", graph.subtaskCount());
        m.put("completedSubtasks", graph.completedCount());
        return m;
    }

    public synchronized Map<String, Object> toMap() {
        Map<String, Object> m = toSummaryMap();
        m.put("answer", answer);
        m.put("error", error);
        m.put("orphanAdoptedBy", orphanAdoptedBy);
        m.put("subtasksPerNode", stringKeys(subtasksPerNode()));
        m.put("peakQueueDepth", stringKeys(peakQueueDepth));
        m.put("stages", graph.toList());
        return m;
    }

    private static Map<String, Object> stringKeys(Map<Integer, Integer> in) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<Integer, Integer> e : in.entrySet()) {
            out.put(String.valueOf(e.getKey()), e.getValue());
        }
        return out;
    }
}
