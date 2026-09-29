package agentgrid.node;

import agentgrid.clock.LamportClock;
import agentgrid.common.Result;
import agentgrid.common.Subtask;

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
 */
public class NodeAgentService extends UnicastRemoteObject implements NodeAgent {

    private static final long serialVersionUID = 1L;
    private static final int DEFAULT_POOL_SIZE = 4;

    private final String agentId;
    private final int poolSize;
    private final transient AtomicInteger queueDepth = new AtomicInteger(0);
    private final transient LamportClock lamportClock = new LamportClock();
    private final transient ExecutorService threadPool;

    public NodeAgentService(String agentId) throws RemoteException {
        this(agentId, DEFAULT_POOL_SIZE);
    }

    public NodeAgentService(String agentId, int poolSize) throws RemoteException {
        super();
        if (agentId == null || agentId.isBlank()) {
            throw new IllegalArgumentException("agentId must be a non-empty string");
        }
        if (poolSize < 1) {
            throw new IllegalArgumentException("poolSize must be >= 1, got " + poolSize);
        }
        this.agentId = agentId;
        this.poolSize = poolSize;
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
        queueDepth.incrementAndGet();
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
            queueDepth.incrementAndGet();
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

    /**
     * Internal subtask processor executed on a worker pool thread.
     * Updates Lamport logical time, logs execution, and computes result.
     * Synchronizes clock update and printout to guarantee strictly increasing log order.
     */
    private Result executeSubtaskInternal(Subtask subtask) {
        long eventTime;
        synchronized (lamportClock) {
            eventTime = lamportClock.update(subtask.getLamportTimestamp());
            System.out.println("[" + agentId + "] executing "
                    + subtask + " on "
                    + Thread.currentThread().getName()
                    + " | lamport=" + eventTime);
        }

        String output = simpleAgentLogic(subtask);

        return new Result(
                subtask.getSubtaskId(),
                agentId,
                output,
                eventTime,
                true
        );
    }

    /**
     * Simulated agent NLP logic for mock retrieval, ranking, summarization, and synthesis.
     */
    private String simpleAgentLogic(Subtask subtask) {
        simulateIoLatency();

        switch (subtask.getType()) {
            case RETRIEVE:
                return "retrieved[" + subtask.getPayload() + "]";
            case RANK:
                return "ranked[" + subtask.getPayload() + "]";
            case SUMMARIZE:
                return "summary of: " + subtask.getPayload();
            case SYNTHESIZE:
                return "synthesized: " + subtask.getPayload();
            default:
                return "unknown-op";
        }
    }

    /**
     * Simulates 150 ms of I/O-bound agent work.
     */
    private void simulateIoLatency() {
        try {
            Thread.sleep(getSimulatedWorkMs());
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
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
        return 150;
    }

    @Override
    public long sync(long controlLamport) throws RemoteException {
        synchronized (lamportClock) {
            return lamportClock.update(controlLamport);
        }
    }
}

