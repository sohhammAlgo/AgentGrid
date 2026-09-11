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
 * Node IDs are distinct integers. Higher numbers indicate higher priority/capacity.
 * When a node detects leader failure or starts an election:
 * 1. It sends an ELECTION message to all nodes with ID > myId.
 * 2. If any higher node answers with OK/ANSWER, this node steps back and waits for a COORDINATOR message.
 * 3. If no higher node answers within the timeout, this node declares itself leader and broadcasts COORDINATOR.
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

    @Override
    public void startElection() {
        if (!alive.get()) {
            return;
        }

        asyncExecutor.submit(() -> runBullyElection());
    }

    private void runBullyElection() {
        if (!alive.get() || !electionInProgress.compareAndSet(false, true)) {
            return;
        }

        if (metrics != null) {
            metrics.recordStart();
        }

        System.out.println("[Bully Node " + nodeId + "] Initiating election...");

        AtomicBoolean higherNodeAnswered = new AtomicBoolean(false);
        int higherCount = 0;

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
                    // Higher node unreachable / down
                }
            }
        }

        if (higherCount == 0 || !higherNodeAnswered.get()) {
            // No higher nodes answered — I am the leader!
            claimLeadership();
        } else {
            // Wait briefly for a COORDINATOR message from a higher node
            try {
                Thread.sleep(ELECTION_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // If still no coordinator elected, retry election
            if (electionInProgress.get() && leaderId != nodeId) {
                electionInProgress.set(false);
                if (alive.get()) {
                    claimLeadership();
                }
            }
        }
    }

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

        // Broadcast COORDINATOR to all other nodes
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
                    // Peer down
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

        if (nodeId > senderId) {
            // Answer back to the lower node
            try {
                int senderPort = nodePortMap.get(senderId);
                Registry registry = LocateRegistry.getRegistry("localhost", senderPort);
                ElectionNodeService sender = (ElectionNodeService) registry.lookup("election-node-" + senderId);

                if (metrics != null) {
                    metrics.incrementMessages();
                }
                sender.receiveBullyAnswer(nodeId);
            } catch (Exception e) {
                // Ignore failure sending answer
            }

            // Start own election if not already running
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
        // No-op for Bully mode
    }

    public void shutdown() {
        asyncExecutor.shutdownNow();
    }
}
