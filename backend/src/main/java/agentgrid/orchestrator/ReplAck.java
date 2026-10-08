package agentgrid.orchestrator;

import java.io.Serializable;

/** A backup's reply to a replicated event or a snapshot. */
public final class ReplAck implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        /** Appended (or already had it). */
        OK,
        /** The sender's term is below the receiver's: the sender is a stale primary. */
        STALE_TERM,
        /** The event does not continue the receiver's log (a gap or a different history): send a snapshot. */
        NEED_SYNC
    }

    private final Status status;
    private final long term;
    private final long lastSeq;
    private final String message;

    public ReplAck(Status status, long term, long lastSeq, String message) {
        this.status = status;
        this.term = term;
        this.lastSeq = lastSeq;
        this.message = message;
    }

    public Status getStatus() { return status; }
    public boolean isOk() { return status == Status.OK; }
    /** The receiver's highest term seen. */
    public long getTerm() { return term; }
    /** The receiver's last log seq after handling the request. */
    public long getLastSeq() { return lastSeq; }
    public String getMessage() { return message; }

    @Override
    public String toString() {
        return status + " (term " + term + ", lastSeq " + lastSeq + (message == null ? "" : ", " + message) + ")";
    }
}
