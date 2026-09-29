package agentgrid.node;

import agentgrid.clock.LamportClock;
import agentgrid.common.Result;
import agentgrid.common.Subtask;
import agentgrid.orchestrator.Corpus;
import agentgrid.orchestrator.StrategyRegistry;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integrated Agent node implementation extending UnicastRemoteObject and implementing NodeAgent.
 *
 * Fixes two verified concurrency bugs present in the baseline AgentServiceImpl:
 *
 * BUG A FIX: In AgentServiceImpl, execute() executed synchronously on RMI dispatch threads
 * rather than dispatching tasks through the managed worker thread pool, rendering poolSize
 * ineffective under concurrent external RMI calls. In NodeAgentService, execute() submits
 * work to the fixed thread pool and blocks on the resulting Future, enforcing real pool-bound
 * concurrency limits.
 *
 * BUG B FIX: In AgentServiceImpl, executeBatch() incremented queueDepth during submission and
 * then delegated each subtask to execute(), which incremented queueDepth a second time.
 * In NodeAgentService, each subtask is counted exactly once from submission until worker completion.
 *
 * The work itself is done by the StageStrategy registered for the subtask's type.
 */
public class NodeAgentService extends UnicastRemoteObject implements NodeAgent {

    private static final long serialVersionUID = 1L;
    private static final int DEFAULT_POOL_SIZE = 4;

    private final String agentId;
    private final int poolSize;
    private final long simulatedWorkMs;
    private final transient AtomicInteger queueDepth = new AtomicInteger(0);
    private final transient AtomicInteger peakQueueDepth = new AtomicInteger(0);
    private final transient LamportClock lamportClock;
    private final transient StrategyRegistry strategies;
    private final transient ExecutorService threadPool;
    private transient volatile Runnable syncListener;

    public NodeAgentService(String agentId) throws RemoteException {
        this(agentId, DEFAULT_POOL_SIZE);
    }

    public NodeAgentService(String agentId, int poolSize) throws RemoteException {
        this(agentId, poolSize, new LamportClock());
    }

    public NodeAgentService(String agentId, int poolSize, LamportClock lamportClock) throws RemoteException {
        this(agentId, poolSize, lamportClock, StrategyRegistry.defaults(Corpus.load()),
                ClusterConfig.DEFAULT_SIMULATED_WORK_MS);
    }

    /**
     * @param lamportClock    the node's single Lamport clock, shared with its telemetry buffer
     *                        and election service
     * @param strategies      the work done per Subtask.Type
     * @param simulatedWorkMs delay added to every subtask; the real stage work is
     *                        sub-millisecond, so without it the thread-pool (Exp 2) and
     *                        load-balancing (Exp 6) effects would not be measurable
     */
    public NodeAgentService(String agentId, int poolSize, LamportClock lamportClock,
                            StrategyRegistry strategies, long simulatedWorkMs) throws RemoteException {
        // Exported with a connect timeout so an orchestrator dispatching to a killed node
        // fails fast instead of stalling on the Windows connect retry (see TimeoutSocketFactory).
        super(0, TimeoutSocketFactory.INSTANCE, null);
        if (agentId == null || agentId.isBlank()) {
            throw new IllegalArgumentException("agentId must be a non-empty string");
        }
        if (poolSize < 1) {
            throw new IllegalArgumentException("poolSize must be >= 1, got " + poolSize);
        }
        this.agentId = agentId;
        this.poolSize = poolSize;
        this.lamportClock = lamportClock;
        this.strategies = strategies;
        this.simulatedWorkMs = Math.max(0, simulatedWorkMs);
        this.threadPool = Executors.newFixedThreadPool(poolSize);
    }

    /**
     * Executes a single subtask on this agent's managed thread pool.
     * Enforces pool concurrency by submitting to threadPool and blocking on the Future.
     *
     * @param subtask the subtask to execute
     * @return the computation Result
     * @throws RemoteException if an RMI error or task execution failure occurs
     */
    @Override
    public Result execute(Subtask subtask) throws RemoteException {
        enqueued();
        Future<Result> future;
        try {
            future = threadPool.submit(() -> {
                try {
                    return executeSubtaskInternal(subtask);
                } finally {
                    queueDepth.decrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            queueDepth.decrementAndGet();
            throw new RemoteException("Failed to submit task to worker pool: " + e.getMessage(), e);
        }

        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Interrupted while awaiting subtask execution: " + subtask.getSubtaskId(), e);
        } catch (ExecutionException e) {
            throw new RemoteException("Subtask execution failed: " + subtask.getSubtaskId(), e.getCause());
        }
    }

    /**
     * Executes a batch of subtasks concurrently across this agent's worker thread pool.
     * Fixes Bug B: subtasks are submitted directly to the thread pool without double-incrementing queueDepth.
     *
     * @param subtasks list of subtasks to execute
     * @return list of results ordered corresponding to submission order
     * @throws RemoteException if an RMI error or task failure occurs
     */
    @Override
    public List<Result> executeBatch(List<Subtask> subtasks) throws RemoteException {
        List<Future<Result>> futures = new ArrayList<>(subtasks.size());

        for (Subtask subtask : subtasks) {
            enqueued();
            try {
                Future<Result> future = threadPool.submit(() -> {
                    try {
                        return executeSubtaskInternal(subtask);
                    } finally {
                        queueDepth.decrementAndGet();
                    }
                });
                futures.add(future);
            } catch (RuntimeException e) {
                queueDepth.decrementAndGet();
                throw new RemoteException("Failed to submit batch subtask to worker pool: " + e.getMessage(), e);
            }
        }

        List<Result> results = new ArrayList<>(futures.size());
        for (Future<Result> future : futures) {
            try {
                results.add(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RemoteException("Interrupted while awaiting batch subtask results", e);
            } catch (ExecutionException e) {
                throw new RemoteException("Batch subtask execution failed in thread pool", e.getCause());
            }
        }
        return results;
    }

    private void enqueued() {
        int depth = queueDepth.incrementAndGet();
        peakQueueDepth.accumulateAndGet(depth, Math::max);
    }

    /**
     * Internal subtask processor executed on a worker pool thread.
     * Receiving the subtask is a Lamport receive event; returning the Result is a send
     * event, so the Result carries a timestamp ticked after the work, greater than both
     * the sender's timestamp and anything this node did in between.
     */
    private Result executeSubtaskInternal(Subtask subtask) {
        long receiveTime;
        synchronized (lamportClock) {
            receiveTime = lamportClock.update(subtask.getLamportTimestamp());
            System.out.println("[" + agentId + "] executing "
                    + subtask + " on "
                    + Thread.currentThread().getName()
                    + " | lamport=" + receiveTime);
        }

        Result work = strategies.run(subtask);
        simulateWork();

        long sendTime;
        synchronized (lamportClock) {
            sendTime = lamportClock.tick();
        }
        return new Result(subtask.getSubtaskId(), agentId, work.getOutput(), sendTime, work.isSuccess());
    }

    private void simulateWork() {
        if (simulatedWorkMs <= 0) {
            return;
        }
        try {
            Thread.sleep(simulatedWorkMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public int getPoolSize() throws RemoteException {
        return poolSize;
    }

    @Override
    public boolean ping() throws RemoteException {
        return true;
    }

    @Override
    public String getAgentId() throws RemoteException {
        return agentId;
    }

    @Override
    public int getQueueDepth() throws RemoteException {
        return queueDepth.get();
    }

    @Override
    public int takePeakQueueDepth() throws RemoteException {
        return peakQueueDepth.getAndSet(queueDepth.get());
    }

    /**
     * Reads this agent's current logical clock timestamp over RMI.
     *
     * @return current Lamport logical timestamp
     * @throws RemoteException if an RMI error occurs
     */
    @Override
    public long getLamportTime() throws RemoteException {
        return lamportClock.getTime();
    }

    /**
     * Gracefully shuts down the worker thread pool.
     */
    public void shutdown() {
        threadPool.shutdown();
    }

    @Override
    public long getSimulatedWorkMs() throws RemoteException {
        return simulatedWorkMs;
    }

    /** Runs after every sync() call; the failure detector uses the first one as its boot gate. */
    public void setSyncListener(Runnable listener) {
        this.syncListener = listener;
    }

    @Override
    public long sync(long controlLamport) throws RemoteException {
        long time;
        synchronized (lamportClock) {
            time = lamportClock.update(controlLamport);
        }
        Runnable listener = syncListener;
        if (listener != null) {
            listener.run();
        }
        return time;
    }
}
