package agentgrid.node;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.LinkedHashMap;

/**
 * Berkeley clock synchronization run by the elected leader, bound as "clock" on every node.
 * Only the leader's coordinator runs rounds; every node accepts the auto-sync setting and the
 * "you were synced" notice.
 */
public interface ClockCoordinatorService extends Remote {

    /**
     * Runs one Berkeley round now (the leader only; throws otherwise). The result has
     * spreadBefore, spreadAfter, averageOffset, nodeCount and corrections (node -> ms), plus
     * coordinator, reason and skipped nodes.
     */
    LinkedHashMap<String, Object> runSyncRound(String reason) throws RemoteException;

    /**
     * A restarted node asks the leader to sync it. Rounds are debounced to at most one per
     * REJOIN_DEBOUNCE_MS; returns true if a round that included nodeId completed after the
     * request. Returns false at once if auto-sync is off or this node is not the leader.
     */
    boolean requestRejoinSync(int nodeId) throws RemoteException;

    /** Enables or disables the periodic and the rejoin sync (the manual round still works). */
    void setAutoSync(boolean enabled) throws RemoteException;

    boolean isAutoSync() throws RemoteException;

    /** Told by the coordinator after it corrected this node's clock in a round. */
    void noteSynced(long coordinatorLamport) throws RemoteException;
}
