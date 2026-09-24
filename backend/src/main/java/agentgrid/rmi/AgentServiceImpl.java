package agentgrid.rmi;

import agentgrid.clock.LamportClock;
import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class AgentServiceImpl extends UnicastRemoteObject implements AgentService {

    private static final long serialVersionUID = 1L;
    private static final int DEFAULT_POOL_SIZE = 4;

    private final String agentId;
    private final AtomicInteger queueDepth = new AtomicInteger(0);

    /*
     * Experiment 3:
     * Logical clock ordering this agent's events against the rest of the grid.
     * Shared by every RMI dispatch thread and every thread-pool worker, which is
     * why LamportClock synchronizes internally.
     */
    private final LamportClock lamportClock = new LamportClock();

    /*
     * Experiment 2:
     * Fixed thread pool shared by the agent.
     */
    private final ExecutorService threadPool;

    public AgentServiceImpl(String agentId) throws RemoteException {
        this(agentId, DEFAULT_POOL_SIZE);
    }

    public AgentServiceImpl(String agentId, int poolSize) throws RemoteException {
        super();
        if (poolSize < 1) {
            throw new IllegalArgumentException("poolSize must be >= 1, got " + poolSize);
        }
        this.agentId = agentId;
        this.threadPool = Executors.newFixedThreadPool(poolSize);
    }

    @Override
    public Result execute(Subtask subtask) throws RemoteException {
        queueDepth.incrementAndGet();
        try {
            /*
             * Experiment 3:
             * Receiving a Subtask is a Lamport receive event, so the clock jumps to
             * max(local, sender) + 1. The old code stamped the Result with
             * subtask.getLamportTimestamp() + 1, which ignored everything else this
             * agent had already done and let concurrent subtasks collide on the
             * same timestamp.
             */
            long eventTime = lamportClock.update(subtask.getLamportTimestamp());

            System.out.println("[" + agentId + "] executing "
                    + subtask + " on "
                    + Thread.currentThread().getName()
                    + " | lamport=" + eventTime);

            String output = simpleAgentLogic(subtask);

            return new Result(
                    subtask.getSubtaskId(),
                    agentId,
                    output,
                    eventTime,
                    true
            );
        } finally {
            queueDepth.decrementAndGet();
        }
    }

    @Override
    public List<Result> executeBatch(List<Subtask> subtasks) throws RemoteException {
        List<Future<Result>> futures = new ArrayList<>();

        /*
         * Submit every subtask to the thread pool.
         */
        for (Subtask subtask : subtasks) {
             queueDepth.incrementAndGet(); // counted while waiting in the pool queue
 Callable<Result> job = () -> {
 try {
 return execute(subtask);
 } finally {
 queueDepth.decrementAndGet();
 }
 };
            futures.add(threadPool.submit(job));
        }

        /*
         * Collect results in submission order.
         */
        List<Result> results = new ArrayList<>();
        for (Future<Result> future : futures) {
            try {
                results.add(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RemoteException("Interrupted while awaiting subtask results", e);
            } catch (Exception e) {
                throw new RemoteException("Subtask execution failed in thread pool", e);
            }
        }
        return results;
    }

    private String simpleAgentLogic(Subtask subtask) {
        /*
         * Simulated I/O latency.
         */
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

    /*
     * Simulate 150 ms of I/O-bound work.
     */
    private void simulateIoLatency() {
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Shutdown the worker thread pool.
     */
    public void shutdown() {
        threadPool.shutdown();
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
     * Current logical time of this agent, for trace inspection.
     * Node-local and not part of the AgentService remote interface — reading a
     * Lamport clock is not an event and must not advance it.
     *
     * @return this agent's current Lamport timestamp
     */
    public long getLamportTime() {
        return lamportClock.getTime();
    }
}
