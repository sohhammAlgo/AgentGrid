package agentgrid.node;

import agentgrid.replication.ReplicationService;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;

/**
 * Replicated blackboard of the integrated cluster, bound under the submitted name
 * "blackboard-node-node-<id>". It extends the submitted ReplicationService, so the old
 * methods (and the Phase 1 health check) keep working, and adds what the cluster needs:
 * quorum-gated STRONG and lagged EVENTUAL writes, reads that say whether the replica is
 * ready, snapshots with metadata, metrics, and the peer-to-peer replication calls.
 */
public interface ClusterBlackboardService extends ReplicationService {

    /** Writes key = value through this node, mode "STRONG" or "EVENTUAL". */
    BlackboardWriteOutcome write(String key, String value, String mode) throws RemoteException;

    /** This replica's record for key (possibly none), with its ready / clock flags. */
    BlackboardState read(String key) throws RemoteException;

    /** All of this replica's records (keys starting with prefix; "" for all). */
    BlackboardState snapshot(String prefix) throws RemoteException;

    BlackboardMetrics metrics() throws RemoteException;

    boolean isReady() throws RemoteException;

    /** Liveness probe used by the STRONG pre-check. */
    boolean alive() throws RemoteException;

    /**
     * Peer replication: merge record into this replica (LWW). Returns true if this replica
     * now holds this record or a newer one for the key, which is the acknowledgement STRONG
     * writes count. senderLamport is the writer's Lamport time (0 if none).
     */
    boolean replicaApply(BlackboardRecord record, long senderLamport) throws RemoteException;

    /**
     * Anti-entropy and restart catch-up: the records this replica holds that are missing from,
     * or newer than, the caller's digest (key -> "timestamp:writer").
     */
    List<BlackboardRecord> pullNewer(HashMap<String, String> digest) throws RemoteException;
}
