package agentgrid.election;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the Chang-Roberts Ring Leader Election algorithm.
 *
 * Nodes are arranged in a logical ring sequence (ordered by ID or explicit list).
 * When an election is triggered:
 * 1. An ELECTION token containing the candidate ID is passed clockwise to the next live node in the ring.
 * 2. Higher candidate IDs override lower ones.
 * 3. When a node receives its own candidate ID back, it is elected LEADER and circulates a COORDINATOR message.
 */
public class RingElectionNode extends UnicastRemoteObject implements ElectionNodeService {

    private static final long serialVersionUID = 1L;

    private final int nodeId;
    private final int port;
    private final List<Integer> ringOrder;
    private final Map<Integer, Integer> nodePortMap;
    private final ElectionMetrics metrics;

    private final AtomicBoolean alive = new AtomicBoolean(true);
    private final AtomicBoolean electionParticipant = new AtomicBoolean(false);
    private volatile int leaderId = -1;

    private final ExecutorService asyncExecutor = Executors.newCachedThreadPool();

    public RingElectionNode(
            int nodeId,
            int port,
            List<Integer> ringOrder,
            Map<Integer, Integer> nodePortMap,
            ElectionMetrics metrics) throws RemoteException {

        super();
        this.nodeId = nodeId;
        this.port = port;
        this.ringOrder = ringOrder;
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
        electionParticipant.set(false);
        System.out.println("[Ring Node " + nodeId + "] CRASHED");
    }

    @Override
    public void recover() {
        alive.set(true);
        electionParticipant.set(false);
        System.out.println("[Ring Node " + nodeId + "] RECOVERED");
        try {
            startElection();
        } catch (Exception e) {
            System.err.println("[Ring Node " + nodeId + "] Error starting election on recovery: " + e.getMessage());
        }
    }

    @Override
    public void startElection() {
        if (!alive.get()) {
            return;
        }

        asyncExecutor.submit(() -> {
            if (metrics != null) {
                metrics.recordStart();
            }
            electionParticipant.set(true);
            System.out.println("[Ring Node " + nodeId + "] Initiating ring election...");
            RingMessage msg = new RingMessage(RingMessage.Type.ELECTION, nodeId, nodeId, 1);
            forwardToSuccessor(msg);
        });
    }

    @Override
    public void receiveRingMessage(RingMessage msg) {
        if (!alive.get()) {
            return;
        }

        if (metrics != null) {
            metrics.incrementMessages();
        }

        if (msg.getType() == RingMessage.Type.ELECTION) {
            handleElectionMessage(msg);
        } else if (msg.getType() == RingMessage.Type.COORDINATOR) {
            handleCoordinatorMessage(msg);
        }
    }

    private void handleElectionMessage(RingMessage msg) {
        int candidate = msg.getCandidateId();

        if (candidate > nodeId) {
            // Forward higher candidate ID
            electionParticipant.set(true);
            System.out.println("[Ring Node " + nodeId + "] Forwarding candidate Node " + candidate);
            forwardToSuccessor(new RingMessage(RingMessage.Type.ELECTION, candidate, msg.getInitiatorId(), msg.getHopCount() + 1));
        } else if (candidate < nodeId) {
            if (!electionParticipant.get()) {
                // Replace candidate with self ID and forward
                electionParticipant.set(true);
                System.out.println("[Ring Node " + nodeId + "] Replacing candidate Node " + candidate + " with self Node " + nodeId);
                forwardToSuccessor(new RingMessage(RingMessage.Type.ELECTION, nodeId, msg.getInitiatorId(), msg.getHopCount() + 1));
            } else {
                // Discard lower candidate message if already participating with a higher candidate
                System.out.println("[Ring Node " + nodeId + "] Discarded lower candidate Node " + candidate);
            }
        } else {
            // candidate == nodeId: Message completed full ring circuit!
            this.leaderId = nodeId;
            this.electionParticipant.set(false);
            System.out.println("[Ring Node " + nodeId + "] ELECTED LEADER via Ring!");

            if (metrics != null) {
                metrics.recordCompletion(nodeId);
            }

            // Circulate COORDINATOR message
            RingMessage coordMsg = new RingMessage(RingMessage.Type.COORDINATOR, nodeId, nodeId, 1);
            forwardToSuccessor(coordMsg);
        }
    }

    private void handleCoordinatorMessage(RingMessage msg) {
        int electedLeader = msg.getCandidateId();

        if (electedLeader != nodeId) {
            this.leaderId = electedLeader;
            this.electionParticipant.set(false);
            System.out.println("[Ring Node " + nodeId + "] Recognized Leader: Node " + electedLeader);

            // Forward coordinator message around ring
            forwardToSuccessor(new RingMessage(RingMessage.Type.COORDINATOR, electedLeader, msg.getInitiatorId(), msg.getHopCount() + 1));
        } else {
            // Coordinator message returned to leader — ring setup complete
            this.electionParticipant.set(false);
            System.out.println("[Ring Node " + nodeId + "] Coordinator message ring traversal complete.");
        }
    }

    private void forwardToSuccessor(RingMessage msg) {
        int myIndex = ringOrder.indexOf(nodeId);
        if (myIndex < 0) {
            return;
        }

        int n = ringOrder.size();
        for (int i = 1; i < n; i++) {
            int nextId = ringOrder.get((myIndex + i) % n);
            if (nextId == nodeId) {
                break;
            }

            Integer nextPort = nodePortMap.get(nextId);
            if (nextPort != null) {
                try {
                    Registry registry = LocateRegistry.getRegistry("localhost", nextPort);
                    ElectionNodeService successor = (ElectionNodeService) registry.lookup("election-node-" + nextId);

                    if (successor.isAlive()) {
                        successor.receiveRingMessage(msg);
                        return;
                    }
                } catch (Exception e) {
                    // Successor down, try next node in ring
                }
            }
        }
    }

    @Override
    public void receiveBullyElection(int senderId) {}

    @Override
    public void receiveBullyAnswer(int responderId) {}

    @Override
    public void receiveBullyCoordinator(int leaderId) {}

    public void shutdown() {
        asyncExecutor.shutdownNow();
    }
}
