package agentgrid.replication;

import java.io.Serializable;

/**
 * Represents an agent subtask research finding stored on the shared replicated blackboard.
 * Marshalled across RMI for multi-node state replication.
 */
public class BlackboardEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String taskId;
    private final String subtaskId;
    private final String findingPayload;
    private final String agentId;
    private final long timestamp;
    private final long version;

    public BlackboardEntry(
            String taskId,
            String subtaskId,
            String findingPayload,
            String agentId,
            long timestamp,
            long version) {

        this.taskId = taskId;
        this.subtaskId = subtaskId;
        this.findingPayload = findingPayload;
        this.agentId = agentId;
        this.timestamp = timestamp;
        this.version = version;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getSubtaskId() {
        return subtaskId;
    }

    public String getFindingPayload() {
        return findingPayload;
    }

    public String getAgentId() {
        return agentId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "BlackboardEntry{" + subtaskId
                + " by " + agentId
                + ", ts=" + timestamp
                + ", v=" + version
                + ", payload='" + findingPayload + "'}";
    }
}
