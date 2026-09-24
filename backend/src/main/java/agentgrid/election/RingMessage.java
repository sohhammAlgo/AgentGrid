package agentgrid.election;

import java.io.Serializable;

/**
 * Serializable message marshalled across RMI for Ring Election algorithm.
 */
public class RingMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Type {
        ELECTION, COORDINATOR
    }

    private final Type type;
    private final int candidateId;
    private final int initiatorId;
    private final int hopCount;

    public RingMessage(Type type, int candidateId, int initiatorId, int hopCount) {
        this.type = type;
        this.candidateId = candidateId;
        this.initiatorId = initiatorId;
        this.hopCount = hopCount;
    }

    public Type getType() {
        return type;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public int getInitiatorId() {
        return initiatorId;
    }

    public int getHopCount() {
        return hopCount;
    }

    @Override
    public String toString() {
        return "RingMessage{" + type
                + ", candidate=" + candidateId
                + ", initiator=" + initiatorId
                + ", hops=" + hopCount + "}";
    }
}
