package agentgrid.common;

import java.rmi.RemoteException;

/**
 * A dispatch or a replicated event carried a term lower than the highest one the receiver has
 * seen: its sender is a primary that has been replaced (a stale leader). It is a RemoteException
 * so it crosses RMI unchanged, but callers must tell it apart from a dead node.
 */
public class StaleTermException extends RemoteException {

    private static final long serialVersionUID = 1L;

    private final long staleTerm;
    private final long currentTerm;

    public StaleTermException(long staleTerm, long currentTerm, String what) {
        super(what + " carries term " + staleTerm + " but the highest term seen is " + currentTerm);
        this.staleTerm = staleTerm;
        this.currentTerm = currentTerm;
    }

    public long getStaleTerm() {
        return staleTerm;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }
}
