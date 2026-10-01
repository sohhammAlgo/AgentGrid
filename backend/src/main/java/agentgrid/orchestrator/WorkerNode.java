package agentgrid.orchestrator;

import agentgrid.common.Result;
import agentgrid.common.Subtask;
import agentgrid.node.NodeAgent;
import agentgrid.rmi.AgentService;

import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A live cluster node as the SUBMITTED balancers see it. The submitted policies
 * (agentgrid.balancer) take a List of AgentService and call getAgentId() or
 * getQueueDepth() on them; this adapter answers those calls from the live node, so the
 * submitted classes are used unmodified.
 *
 * Fix G lives here, not in LeastLoadedBalancer: with LoadView.NORMALIZED_IN_FLIGHT,
 * getQueueDepth() reports load relative to capacity instead of the raw queue length.
 */
public final class WorkerNode implements AgentService {

    /** What getQueueDepth() reports to a balancer. */
    public enum LoadView {
        /** The node's raw queue depth: the submitted LeastLoadedBalancer's view. */
        RAW,
        /** Queue depth divided by pool size (x1000, as the submitted class compares ints). */
        NORMALIZED,
        /**
         * As NORMALIZED, but the depth is at least the number of subtasks this orchestrator has
         * dispatched to the node and not yet got back. A dispatch reaches the node's queue a
         * moment after select() returns, so without this a burst of selections reads stale
         * depths and piles onto one node.
         */
        NORMALIZED_IN_FLIGHT
    }

    private final int nodeId;
    private final NodeAgent stub;
    private final int poolSize;
    private final int weight;
    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile LoadView loadView = LoadView.RAW;

    public WorkerNode(int nodeId, NodeAgent stub, int poolSize) {
        this(nodeId, stub, poolSize, poolSize);
    }

    public WorkerNode(int nodeId, NodeAgent stub, int poolSize, int weight) {
        this.nodeId = nodeId;
        this.stub = stub;
        this.poolSize = Math.max(1, poolSize);
        this.weight = Math.max(1, weight);
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getPoolSize() {
        return poolSize;
    }

    /** WEIGHTED routing weight: the member's configured weight (by default its pool size). */
    public int getWeight() {
        return weight;
    }

    public NodeAgent stub() {
        return stub;
    }

    void setLoadView(LoadView view) {
        this.loadView = view;
    }

    void dispatchStarted() {
        inFlight.incrementAndGet();
    }

    void dispatchEnded() {
        inFlight.decrementAndGet();
    }

    @Override
    public int getQueueDepth() throws RemoteException {
        switch (loadView) {
            case NORMALIZED:
                return stub.getQueueDepth() * 1000 / poolSize;
            case NORMALIZED_IN_FLIGHT:
                return Math.max(stub.getQueueDepth(), inFlight.get()) * 1000 / poolSize;
            case RAW:
            default:
                return stub.getQueueDepth();
        }
    }

    /** Local, no RMI: WeightedBalancer keys its weights by this id. */
    @Override
    public String getAgentId() {
        return "node-" + nodeId;
    }

    @Override
    public Result execute(Subtask subtask) throws RemoteException {
        return stub.execute(subtask);
    }

    @Override
    public List<Result> executeBatch(List<Subtask> subtasks) throws RemoteException {
        return stub.executeBatch(subtasks);
    }

    @Override
    public boolean ping() throws RemoteException {
        return stub.ping();
    }

    @Override
    public String toString() {
        return "node-" + nodeId + "(pool=" + poolSize + ")";
    }
}
