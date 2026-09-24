package agentgrid.replication;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

/**
 * Remote RMI interface exposing replicated blackboard operations.
 */
public interface ReplicationService extends Remote {

    /**
     * Reads a subtask research finding from this node's local blackboard.
     * @param subtaskId ID of the subtask finding to retrieve
     * @return BlackboardEntry or null if not present
     */
    BlackboardEntry getFinding(String subtaskId) throws RemoteException;

    /**
     * Publishes a subtask research finding to the replicated blackboard.
     * @param entry finding entry to write
     * @param level STRONG (synchronous multi-node write) or EVENTUAL (async with LWW)
     * @return true if write succeeded according to the consistency policy
     */
    boolean publishFinding(BlackboardEntry entry, ConsistencyLevel level) throws RemoteException;

    /**
     * Internal peer RPC: Receives a replicated finding pushed from a peer node.
     * Applies Last-Write-Wins (LWW) timestamp conflict resolution.
     * @param entry replicated finding entry
     * @return true if the entry was stored (newer timestamp), false if ignored (older timestamp)
     */
    boolean receiveReplicate(BlackboardEntry entry) throws RemoteException;

    /**
     * Returns a snapshot map of all research findings on this node.
     */
    Map<String, BlackboardEntry> getAllFindings() throws RemoteException;

    /**
     * Identifier of this replicated node.
     */
    String getNodeId() throws RemoteException;
}
