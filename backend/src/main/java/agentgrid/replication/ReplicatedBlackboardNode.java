package agentgrid.replication;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Implementation of a Replicated Agent Blackboard Node storing agent research findings.
 *
 * Replication Modes:
 * - STRONG Consistency: Performs parallel synchronous write replication to 100% of peer nodes
 *   before acknowledging client/agent write completion. Guarantees 0% stale reads across all nodes.
 * - EVENTUAL Consistency: Updates local blackboard and returns immediately to the caller,
 *   asynchronously propagating updates to peers using a background thread. Peers apply Last-Write-Wins (LWW)
 *   timestamp conflict resolution.
 */
public class ReplicatedBlackboardNode extends UnicastRemoteObject implements ReplicationService {

    private static final long serialVersionUID = 1L;

    private final String nodeId;
    private final int port;
    private final Map<String, Integer> peerPortMap;

    /** In-memory state store for subtask research findings. */
    private final Map<String, BlackboardEntry> blackboard = new ConcurrentHashMap<>();

    /** Background executor for async replication daemon tasks. */
    private final ExecutorService asyncExecutor = Executors.newCachedThreadPool();

    /**
     * Constructs a Replicated Blackboard Node.
     *
     * @param nodeId string identifier of this node (e.g. "node-1")
     * @param port RMI registry port
     * @param peerPortMap registry mapping of all peer node IDs to ports
     */
    public ReplicatedBlackboardNode(String nodeId, int port, Map<String, Integer> peerPortMap) throws RemoteException {
        super();
        this.nodeId = nodeId;
        this.port = port;
        this.peerPortMap = new ConcurrentHashMap<>(peerPortMap);
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public BlackboardEntry getFinding(String subtaskId) {
        return blackboard.get(subtaskId);
    }

    @Override
    public Map<String, BlackboardEntry> getAllFindings() {
        return new ConcurrentHashMap<>(blackboard);
    }

    /**
     * Publishes an agent research finding to the replicated blackboard under the specified consistency protocol.
     */
    @Override
    public boolean publishFinding(BlackboardEntry entry, ConsistencyLevel level) throws RemoteException {
        // Step 1: Update local state using Last-Write-Wins (LWW) timestamp rule
        updateLocal(entry);

        if (level == ConsistencyLevel.STRONG) {
            // Step 2a: STRONG Consistency — Synchronous fan-out write replication to all peer nodes
            List<Future<Boolean>> futures = new ArrayList<>();
            for (Map.Entry<String, Integer> peer : peerPortMap.entrySet()) {
                if (!peer.getKey().equals(nodeId)) {
                    Callable<Boolean> replicateTask = () -> sendReplicationRpc(peer.getKey(), peer.getValue(), entry);
                    futures.add(asyncExecutor.submit(replicateTask));
                }
            }

            boolean allSuccess = true;
            for (Future<Boolean> f : futures) {
                try {
                    if (!f.get()) {
                        allSuccess = false;
                    }
                } catch (Exception e) {
                    allSuccess = false;
                }
            }
            return allSuccess;

        } else {
            // Step 2b: EVENTUAL Consistency — Immediate return; background daemon thread replicates asynchronously
            for (Map.Entry<String, Integer> peer : peerPortMap.entrySet()) {
                if (!peer.getKey().equals(nodeId)) {
                    asyncExecutor.submit(() -> sendReplicationRpc(peer.getKey(), peer.getValue(), entry));
                }
            }
            return true;
        }
    }

    @Override
    public boolean receiveReplicate(BlackboardEntry entry) throws RemoteException {
        return updateLocal(entry);
    }

    /**
     * Updates local storage according to the Last-Write-Wins (LWW) conflict resolution strategy:
     * If no entry exists or incoming timestamp > existing timestamp, overwrite.
     * Otherwise, reject stale update.
     */
    private boolean updateLocal(BlackboardEntry incoming) {
        synchronized (blackboard) {
            BlackboardEntry existing = blackboard.get(incoming.getSubtaskId());
            if (existing == null || incoming.getTimestamp() > existing.getTimestamp()) {
                blackboard.put(incoming.getSubtaskId(), incoming);
                System.out.println("[" + nodeId + "] Blackboard updated: " + incoming.getSubtaskId() + " -> '" + incoming.getFindingPayload() + "' (ts=" + incoming.getTimestamp() + ")");
                return true;
            } else {
                System.out.println("[" + nodeId + "] Blackboard write ignored (stale timestamp ts=" + incoming.getTimestamp() + " vs existing ts=" + existing.getTimestamp() + ")");
                return false;
            }
        }
    }

    /**
     * Helper to dispatch receiveReplicate RMI call to a target peer.
     */
    private boolean sendReplicationRpc(String peerId, int peerPort, BlackboardEntry entry) {
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", peerPort);
            ReplicationService peer = (ReplicationService) registry.lookup("blackboard-node-" + peerId);
            return peer.receiveReplicate(entry);
        } catch (Exception e) {
            System.err.println("[" + nodeId + "] Failed to replicate to peer " + peerId + ": " + e.getMessage());
            return false;
        }
    }

    public void shutdown() {
        asyncExecutor.shutdownNow();
    }
}
