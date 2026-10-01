package agentgrid.node;

import agentgrid.election.RingMessage;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Integrated election service of one cluster node.
 *
 * Holds the node's leader view and dispatches to the Bully or Ring protocol. Every
 * message sent is recorded as one ELECTION_MSG event whose Lamport time travels with the
 * message; every message received applies the Lamport receive rule before it is handled.
 * Handlers return immediately and do the protocol work on a background executor, so no
 * RMI call ever waits on a chain of nested calls.
 *
 * Exported with TimeoutSocketFactory so connects to a dead peer fail after
 * TimeoutSocketFactory.CONNECT_TIMEOUT_MS instead of stalling.
 */
public final class ElectionNode extends UnicastRemoteObject implements NodeElection {

    private static final long serialVersionUID = 1L;

    public static final String BULLY = "BULLY";
    public static final String RING = "RING";

    /** Invocation of one remote method on a peer's election stub. */
    interface PeerCall<T> {
        T call(NodeElection peer) throws Exception;
    }

    private final int nodeId;
    /** Member id -> port; replaced as a whole when a new membership epoch is applied. */
    private transient volatile TreeMap<Integer, Integer> ports;
    private final transient NodeEventBuffer events;
    private final transient LeaderLifecycleRegistry lifecycle;
    private final transient ExecutorService exec = Executors.newCachedThreadPool(daemon("election-" ));
    private final transient ScheduledExecutorService timers =
            Executors.newSingleThreadScheduledExecutor(daemon("election-timer-"));
    private final transient Map<Integer, NodeElection> stubs = new ConcurrentHashMap<>();
    private final transient BullyNode bully;
    private final transient RingNode ring;

    private final AtomicBoolean alive = new AtomicBoolean(true);
    private volatile String algorithm;

    private final transient Object leaderLock = new Object();
    private volatile int leaderId = -1;
    /** Last leader this node accepted, kept across elections to detect lifecycle transitions. */
    private int lastAccepted = -1;

    public ElectionNode(int nodeId, Map<Integer, Integer> nodePortMap, String algorithm,
                        NodeEventBuffer events, LeaderLifecycleRegistry lifecycle) throws RemoteException {
        super(0, TimeoutSocketFactory.INSTANCE, null);
        this.nodeId = nodeId;
        this.ports = new TreeMap<>(nodePortMap);
        this.events = events;
        this.lifecycle = lifecycle;
        this.algorithm = normalize(algorithm);
        this.bully = new BullyNode(this);
        this.ring = new RingNode(this);
    }

    // =========================================================================
    // Helpers used by BullyNode, RingNode and FailureDetector
    // =========================================================================

    int id() {
        return nodeId;
    }

    boolean alive() {
        return alive.get();
    }

    ExecutorService exec() {
        return exec;
    }

    ScheduledExecutorService timers() {
        return timers;
    }

    NodeEventBuffer events() {
        return events;
    }

    LeaderLifecycleRegistry lifecycle() {
        return lifecycle;
    }

    int leader() {
        return leaderId;
    }

    List<Integer> higherIds() {
        return new ArrayList<>(ports.tailMap(nodeId, false).keySet());
    }

    List<Integer> lowerIds() {
        return new ArrayList<>(ports.headMap(nodeId, false).descendingKeySet());
    }

    /** Other node ids in ring order (ascending id, wrapping) starting after this node. */
    List<Integer> successors() {
        TreeMap<Integer, Integer> p = ports;
        List<Integer> out = new ArrayList<>(p.tailMap(nodeId, false).keySet());
        out.addAll(p.headMap(nodeId, false).keySet());
        return out;
    }

    /**
     * Replaces the peer set with the members of a newly applied membership epoch. Bully's
     * higher/lower ids and the ring order (ascending ids) follow it; cached stubs of removed
     * members are dropped. A leader that is no longer a member stops answering pings, so the
     * failure detector elects a new one as usual.
     */
    public void updatePeers(Map<Integer, Integer> nodePortMap) {
        TreeMap<Integer, Integer> next = new TreeMap<>(nodePortMap);
        ports = next;
        stubs.keySet().removeIf(id -> !next.containsKey(id));
        System.out.println("[Node " + nodeId + "] election peers now " + next.keySet());
    }

    /** Starts an election with the currently selected algorithm. */
    void start(String reason) {
        if (!alive.get()) {
            return;
        }
        if (RING.equals(algorithm)) {
            ring.start(reason);
        } else {
            bully.start(reason);
        }
    }

    /** Records ELECTION_STARTED and drops the leader view: an electing node has no leader. */
    void beginElection(String alg, String reason) {
        synchronized (leaderLock) {
            leaderId = -1;
            Map<String, Object> f = new LinkedHashMap<>();
            f.put("algorithm", alg);
            f.put("reason", reason);
            events.record("ELECTION_STARTED", alg + " election started: " + reason, f);
        }
    }

    /** Sets the leader view, records LEADER_ACCEPTED, and reports lifecycle transitions. */
    void acceptLeader(int newLeader, String via) {
        int previousAccepted;
        synchronized (leaderLock) {
            previousAccepted = lastAccepted;
            int previousView = leaderId;
            leaderId = newLeader;
            lastAccepted = newLeader;
            Map<String, Object> f = new LinkedHashMap<>();
            f.put("leader", newLeader);
            f.put("previous", previousView < 0 ? null : previousView);
            f.put("via", via);
            events.record("LEADER_ACCEPTED", "accepted node " + newLeader + " as leader (" + via + ")", f);
        }
        if (newLeader == nodeId && previousAccepted != nodeId) {
            lifecycle.onElected(nodeId);
        } else if (previousAccepted == nodeId && newLeader != nodeId) {
            lifecycle.onDemoted(nodeId, newLeader);
        }
    }

    /** Drops the leader view if it still points at expected. */
    boolean clearLeaderIf(int expected) {
        synchronized (leaderLock) {
            if (leaderId == expected) {
                leaderId = -1;
                return true;
            }
            return false;
        }
    }

    /**
     * Sends one election message: records ELECTION_MSG (kind, from, to), then invokes the
     * peer with the event's Lamport time. Returns true if the RMI call succeeded.
     */
    boolean send(int to, String kind, String note, java.util.function.LongFunction<PeerCall<Void>> call) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("kind", kind);
        f.put("from", nodeId);
        f.put("to", to);
        NodeEvent ev = events.record("ELECTION_MSG", kind + " " + nodeId + " -> " + to
                + (note == null ? "" : " " + note), f);
        try {
            invoke(to, call.apply(ev.getLamport()));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** Failure-detector ping; the peer's own leader view, or null if it did not answer. */
    Integer ping(int to) {
        try {
            return invoke(to, peer -> peer.heartbeat(nodeId));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Calls a peer through a cached stub. A cached stub may belong to an earlier process
     * of a restarted peer, so one failure with a cached stub is retried with a fresh lookup.
     */
    private <T> T invoke(int to, PeerCall<T> call) throws Exception {
        NodeElection cached = stubs.get(to);
        if (cached != null) {
            try {
                return call.call(cached);
            } catch (Exception e) {
                stubs.remove(to, cached);
            }
        }
        NodeElection fresh = lookup(to);
        stubs.put(to, fresh);
        try {
            return call.call(fresh);
        } catch (Exception e) {
            stubs.remove(to, fresh);
            throw e;
        }
    }

    private NodeElection lookup(int to) throws Exception {
        Integer port = ports.get(to);
        if (port == null) {
            throw new IllegalArgumentException("unknown node " + to);
        }
        Registry registry = LocateRegistry.getRegistry("localhost", port, TimeoutSocketFactory.INSTANCE);
        return (NodeElection) registry.lookup("election-node-" + to);
    }

    // =========================================================================
    // NodeElection remote methods
    // =========================================================================

    @Override
    public void setAlgorithm(String name) throws RemoteException {
        String alg = normalize(name);
        if (!alg.equals(algorithm)) {
            System.out.println("[Node " + nodeId + "] election algorithm " + algorithm + " -> " + alg);
        }
        algorithm = alg;
    }

    @Override
    public String getAlgorithm() {
        return algorithm;
    }

    @Override
    public int getLeaderId() {
        return leaderId;
    }

    @Override
    public void startElection() {
        exec.submit(() -> start("requested via startElection()"));
    }

    @Override
    public boolean isElecting() {
        return bully.isRunning() || ring.isRunning();
    }

    @Override
    public int heartbeat(int fromId) throws RemoteException {
        if (!alive.get()) {
            throw new RemoteException("node " + nodeId + " is crashed (simulated)");
        }
        return leaderId;
    }

    @Override
    public void bullyElection(int fromId, long senderLamport) {
        if (!alive.get()) {
            return;
        }
        events.receive(senderLamport);
        exec.submit(() -> bully.onElection(fromId));
    }

    @Override
    public void bullyAnswer(int fromId, long senderLamport) {
        if (!alive.get()) {
            return;
        }
        events.receive(senderLamport);
        bully.onAnswer(fromId);
    }

    @Override
    public void bullyCoordinator(int leader, long senderLamport) {
        if (!alive.get()) {
            return;
        }
        events.receive(senderLamport);
        exec.submit(() -> bully.onCoordinator(leader));
    }

    @Override
    public void ringToken(RingToken token, long senderLamport) {
        if (!alive.get()) {
            return;
        }
        events.receive(senderLamport);
        exec.submit(() -> ring.onToken(token));
    }

    // =========================================================================
    // Submitted ElectionNodeService methods (no Lamport time carried)
    // =========================================================================

    @Override
    public void receiveBullyElection(int senderId) {
        bullyElection(senderId, 0L);
    }

    @Override
    public void receiveBullyAnswer(int responderId) {
        bullyAnswer(responderId, 0L);
    }

    @Override
    public void receiveBullyCoordinator(int leader) {
        bullyCoordinator(leader, 0L);
    }

    @Override
    public void receiveRingMessage(RingMessage message) {
        RingToken.Kind kind = message.getType() == RingMessage.Type.COORDINATOR
                ? RingToken.Kind.COORDINATOR : RingToken.Kind.ELECTION;
        ringToken(new RingToken(kind, message.getInitiatorId(), message.getCandidateId(), 0L,
                message.getHopCount()), 0L);
    }

    @Override
    public int getNodeId() {
        return nodeId;
    }

    @Override
    public boolean isLeader() {
        return alive.get() && leaderId == nodeId;
    }

    @Override
    public boolean isAlive() {
        return alive.get();
    }

    @Override
    public void simulateCrash() {
        alive.set(false);
        leaderId = -1;
        System.out.println("[Node " + nodeId + "] election service CRASHED (simulated)");
    }

    @Override
    public void recover() {
        alive.set(true);
        System.out.println("[Node " + nodeId + "] election service RECOVERED");
        exec.submit(() -> start("recovered from simulated crash"));
    }

    public void shutdown() {
        exec.shutdownNow();
        timers.shutdownNow();
    }

    private static String normalize(String name) {
        String n = name == null ? "" : name.trim().toUpperCase();
        if (!BULLY.equals(n) && !RING.equals(n)) {
            throw new IllegalArgumentException("algorithm must be BULLY or RING, got: " + name);
        }
        return n;
    }

    static java.util.concurrent.ThreadFactory daemon(String prefix) {
        java.util.concurrent.atomic.AtomicInteger n = new java.util.concurrent.atomic.AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
