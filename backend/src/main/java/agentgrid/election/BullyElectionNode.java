package agentgrid.election;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of Garcia-Molina's Bully Leader Election algorithm.
 *
 * Algorithm Overview:
 * Node IDs are distinct integer values where higher numbers indicate higher capacity/priority.
 * When a node detects leader failure or starts an election:
 * 1. It sends an ELECTION message to all nodes with nodeId > myId.
 * 2. If any higher node responds with ANSWER/OK, this node steps back and waits for a COORDINATOR message.
 * 3. If no higher node responds within the timeout window, this node claims leadership and broadcasts a COORDINATOR message to all lower-ID nodes.
 */
public class BullyElectionNode extends UnicastRemoteObject implements ElectionNodeService {

    private static final long serialVersionUID = 1L;
    private static final long ELECTION_TIMEOUT_MS = 500L;

    private final int nodeId;
    private final int port;
    private final Map<Integer, Integer> nodePortMap;
    private final ElectionMetrics metrics;

    private final AtomicBoolean alive = new AtomicBoolean(true);
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private volatile int leaderId = -1;

    private final ExecutorService asyncExecutor = Executors.newCachedThreadPool();

    /**
     * Constructs a Bully Election Node.
     *
     * @param nodeId integer identifier of this node
     * @param port RMI registry port
     * @param nodePortMap registry mapping of all cluster node IDs to ports
     * @param metrics empirical metrics collector (optional)
     */
    public BullyElectionNode(int nodeId, int port, Map<Integer, Integer> nodePortMap, ElectionMetrics metrics)
            throws RemoteException {
        super();
        this.nodeId = nodeId;
        this.port = port;
        this.nodePortMap = new ConcurrentHashMap<>(nodePortMap);
        this.metrics = metrics;
    }

    @Override
    public int getNodeId() {
        return nodeId;
    }

    @Override
    public int getLeaderId() {
        return leaderId;
    }

    @Override
    public boolean isLeader() {
        return alive.get() && (leaderId == nodeId);
    }

    @Override
    public boolean isAlive() {
        return alive.get();
    }

    @Override
    public void simulateCrash() {
        alive.set(false);
        leaderId = -1;
        System.out.println("[Bully Node " + nodeId + "] CRASHED");
    }

    @Override
    public void recover() {
        alive.set(true);
        System.out.println("[Bully Node " + nodeId + "] RECOVERED");
        try {
            startElection();
        } catch (Exception e) {
            System.err.println("[Bully Node " + nodeId + "] Error starting election on recovery: " + e.getMessage());
        }
    }

    /**
     * Initiates the election asynchronously on worker thread pool to avoid blocking RMI handlers.
     */
    @Override
    public void startElection() {
        if (!alive.get()) {
            return;
        }
        asyncExecutor.submit(() -> runBullyElection());
    }

    /**
     * Core Bully election sequence.
     */
    private void runBullyElection() {
        // Prevent concurrent overlapping elections on the same node
        if (!alive.get() || !electionInProgress.compareAndSet(false, true)) {
            return;
        }

        if (metrics != null) {
            metrics.recordStart();
        }

        System.out.println("[Bully Node " + nodeId + "] Initiating election...");

        AtomicBoolean higherNodeAnswered = new AtomicBoolean(false);
        int higherCount = 0;

        // Step 1: Send ELECTION message to all peers with higher node IDs
        for (Map.Entry<Integer, Integer> entry : nodePortMap.entrySet()) {
            int peerId = entry.getKey();
            int peerPort = entry.getValue();

            if (peerId > nodeId) {
                higherCount++;
                try {
                    Registry registry = LocateRegistry.getRegistry("localhost", peerPort);
                    ElectionNodeService peer = (ElectionNodeService) registry.lookup("election-node-" + peerId);

                    if (peer.isAlive()) {
                        if (metrics != null) {
                            metrics.incrementMessages();
                        }
                        peer.receiveBullyElection(nodeId);
                        higherNodeAnswered.set(true);
                    }
                } catch (Exception e) {
                    // Higher node is offline/unreachable
                }
            }
        }

        // Step 2: If no higher nodes exist or none responded, claim leadership
        if (higherCount == 0 || !higherNodeAnswered.get()) {
            claimLeadership();
        } else {
            // Step 3: Wait for a higher node to send COORDINATOR
            try {
                Thread.sleep(ELECTION_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Retry election if no coordinator was established within timeout
            if (electionInProgress.get() && leaderId != nodeId) {
                electionInProgress.set(false);
                if (alive.get()) {
                    claimLeadership();
                }
            }
        }
    }

    /**
     * Declares this node as the elected leader and broadcasts COORDINATOR to all active peers.
     */
    private void claimLeadership() {
        if (!alive.get()) {
            electionInProgress.set(false);
            return;
        }

        this.leaderId = nodeId;
        System.out.println("[Bully Node " + nodeId + "] ELECTED LEADER!");

        if (metrics != null) {
            metrics.recordCompletion(nodeId);
        }

        // Broadcast COORDINATOR message to all lower-ID peer nodes
        for (Map.Entry<Integer, Integer> entry : nodePortMap.entrySet()) {
            int peerId = entry.getKey();
            int peerPort = entry.getValue();

            if (peerId != nodeId) {
                try {
                    Registry registry = LocateRegistry.getRegistry("localhost", peerPort);
                    ElectionNodeService peer = (ElectionNodeService) registry.lookup("election-node-" + peerId);

                    if (peer.isAlive()) {
                        if (metrics != null) {
                            metrics.incrementMessages();
                        }
                        peer.receiveBullyCoordinator(nodeId);
                    }
                } catch (Exception e) {
                    // Peer offline
                }
            }
        }

        electionInProgress.set(false);
    }

    @Override
    public void receiveBullyElection(int senderId) {
        if (!alive.get()) {
            return;
        }

        System.out.println("[Bully Node " + nodeId + "] Received ELECTION from Node " + senderId);

        // If this node has a higher ID than the sender, send ANSWER and start own election
        if (nodeId > senderId) {
            try {
                int senderPort = nodePortMap.get(senderId);
                Registry registry = LocateRegistry.getRegistry("localhost", senderPort);
                ElectionNodeService sender = (ElectionNodeService) registry.lookup("election-node-" + senderId);

                if (metrics != null) {
                    metrics.incrementMessages();
                }
                sender.receiveBullyAnswer(nodeId);
            } catch (Exception e) {
                // Ignore unreachable sender
            }

            startElection();
        }
    }

    @Override
    public void receiveBullyAnswer(int responderId) {
        if (!alive.get()) {
            return;
        }

        System.out.println("[Bully Node " + nodeId + "] Received ANSWER from Node " + responderId);
    }

    @Override
    public void receiveBullyCoordinator(int leaderId) {
        if (!alive.get()) {
            return;
        }

        this.leaderId = leaderId;
        this.electionInProgress.set(false);
        System.out.println("[Bully Node " + nodeId + "] Recognized Leader: Node " + leaderId);
    }

    @Override
    public void receiveRingMessage(RingMessage message) {
        // No-op for Bully algorithm
    }

    public void shutdown() {
        asyncExecutor.shutdownNow();
    }
}
