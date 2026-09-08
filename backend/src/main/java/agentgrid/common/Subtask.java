package agentgrid.common;

import java.io.Serializable;

/**
 * Represents one unit of work dispatched to an AgentNode.
 * Implements Serializable because Java RMI marshals whole objects
 * across the network by value.
 */
public class Subtask implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Type {
        RETRIEVE, RANK, SUMMARIZE, SYNTHESIZE
    }

    private final String taskId;
    private final String subtaskId;
    private final Type type;
    private final String payload;
    private final long lamportTimestamp;

    public Subtask(
            String taskId,
            String subtaskId,
            Type type,
            String payload,
            long lamportTimestamp) {

        this.taskId = taskId;
        this.subtaskId = subtaskId;
        this.type = type;
        this.payload = payload;
        this.lamportTimestamp = lamportTimestamp;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getSubtaskId() {
        return subtaskId;
    }

    public Type getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }

    public long getLamportTimestamp() {
        return lamportTimestamp;
    }

    @Override
    public String toString() {
        return "Subtask{" + subtaskId
                + ", type=" + type
                + ", ts=" + lamportTimestamp + "}";
    }
}
