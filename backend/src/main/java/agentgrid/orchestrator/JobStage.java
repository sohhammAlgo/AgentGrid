package agentgrid.orchestrator;

import agentgrid.common.Subtask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One pipeline stage of a job and the state of each of its subtasks.
 * Mutated only by the job's runner while holding the owning Job's monitor.
 */
public class JobStage implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int MAX_OUTPUT_CHARS = 600;

    public enum Status { PENDING, RUNNING, COMPLETE, FAILED }

    public enum SubtaskStatus { PENDING, DISPATCHED, COMPLETE, FAILED }

    /** One subtask: what it was asked, where it ran, and the Lamport times of its chain. */
    public static class SubtaskState implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String subtaskId;
        private final String input;
        private SubtaskStatus status = SubtaskStatus.PENDING;
        private Integer node;
        private int attempts;
        private Long dispatchLamport;
        private Long resultLamport;
        private Long completeLamport;
        private Long dispatchTrueMs;
        private Long completeTrueMs;
        private String output;
        private String fullOutput;

        SubtaskState(String subtaskId, String input) {
            this.subtaskId = subtaskId;
            this.input = input;
        }

        void dispatched(int nodeId, long lamport, long trueMs) {
            status = SubtaskStatus.DISPATCHED;
            node = nodeId;
            attempts++;
            dispatchLamport = lamport;
            dispatchTrueMs = trueMs;
        }

        void completed(long resultLamport, long completeLamport, long trueMs, String result, boolean ok) {
            status = ok ? SubtaskStatus.COMPLETE : SubtaskStatus.FAILED;
            this.resultLamport = resultLamport;
            this.completeLamport = completeLamport;
            this.completeTrueMs = trueMs;
            this.fullOutput = result;
            this.output = result == null || result.length() <= MAX_OUTPUT_CHARS
                    ? result : result.substring(0, MAX_OUTPUT_CHARS) + "...";
        }

        void failed(String reason) {
            status = SubtaskStatus.FAILED;
            output = reason;
        }

        public String getSubtaskId() { return subtaskId; }
        public SubtaskStatus getStatus() { return status; }
        public Integer getNode() { return node; }
        public int getAttempts() { return attempts; }
        public Long getDispatchLamport() { return dispatchLamport; }
        public Long getResultLamport() { return resultLamport; }
        public Long getCompleteLamport() { return completeLamport; }
        public String getInput() { return input; }
        /** Untruncated output; the runner builds the next stage's payloads from it. */
        String fullOutput() { return fullOutput; }

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("subtaskId", subtaskId);
            m.put("status", status.name());
            m.put("node", node);
            m.put("attempts", attempts);
            m.put("dispatchLamport", dispatchLamport);
            m.put("resultLamport", resultLamport);
            m.put("completeLamport", completeLamport);
            m.put("dispatchTrueMs", dispatchTrueMs);
            m.put("completeTrueMs", completeTrueMs);
            m.put("input", input);
            m.put("output", output);
            return m;
        }
    }

    private final Subtask.Type type;
    private final ArrayList<SubtaskState> subtasks = new ArrayList<>();
    private Status status = Status.PENDING;
    private Long startTrueMs;
    private Long endTrueMs;

    public JobStage(Subtask.Type type) {
        this.type = type;
    }

    SubtaskState add(String subtaskId, String input) {
        SubtaskState s = new SubtaskState(subtaskId, input);
        subtasks.add(s);
        return s;
    }

    void started(long trueMs) {
        status = Status.RUNNING;
        startTrueMs = trueMs;
    }

    void finished(boolean ok, long trueMs) {
        status = ok ? Status.COMPLETE : Status.FAILED;
        endTrueMs = trueMs;
    }

    public Subtask.Type getType() { return type; }
    public Status getStatus() { return status; }
    public List<SubtaskState> getSubtasks() { return subtasks; }

    public long completedCount() {
        return subtasks.stream().filter(s -> s.getStatus() == SubtaskStatus.COMPLETE).count();
    }

    Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", type.name());
        m.put("status", status.name());
        m.put("startTrueMs", startTrueMs);
        m.put("endTrueMs", endTrueMs);
        m.put("durationMs", startTrueMs != null && endTrueMs != null ? endTrueMs - startTrueMs : null);
        List<Map<String, Object>> list = new ArrayList<>();
        for (SubtaskState s : subtasks) {
            list.add(s.toMap());
        }
        m.put("subtasks", list);
        return m;
    }
}
