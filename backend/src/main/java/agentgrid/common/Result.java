
package agentgrid.common;

import java.io.Serializable;

/**
 * Result of executing a Subtask.
 * Returned from an AgentNode back to the client over RMI.
 */
public class Result implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String subtaskId;
    private final String agentId;
    private final String output;
    private final long lamportTimestamp;
    private final boolean success;

    public Result(
            String subtaskId,
            String agentId,
            String output,
            long lamportTimestamp,
            boolean success) {

        this.subtaskId = subtaskId;
        this.agentId = agentId;
        this.output = output;
        this.lamportTimestamp = lamportTimestamp;
        this.success = success;
    }

    public String getSubtaskId() {
        return subtaskId;
    }

    public String getAgentId() {
        return agentId;
    }

    public String getOutput() {
        return output;
    }

    public long getLamportTimestamp() {
        return lamportTimestamp;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "Result{" + subtaskId
                + " <- " + agentId
                + ", ts=" + lamportTimestamp
                + ", success=" + success
                + ", output='" + output + "'}";
    }
}