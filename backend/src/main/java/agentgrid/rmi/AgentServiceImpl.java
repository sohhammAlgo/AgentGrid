package agentgrid.rmi;

import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
<<<<<<< HEAD
=======

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

>>>>>>> bf3b5a39342d188269adf8991709e660f7025aee
import java.util.concurrent.atomic.AtomicInteger;

public class AgentServiceImpl
        extends UnicastRemoteObject
        implements AgentService {

    private static final long serialVersionUID = 1L;

    private final String agentId;
<<<<<<< HEAD
    private final AtomicInteger queueDepth =
            new AtomicInteger(0);

    public AgentServiceImpl(String agentId)
            throws RemoteException {

        super();
        this.agentId = agentId;
=======

    private final AtomicInteger queueDepth =
            new AtomicInteger(0);

    /*
     * Experiment 2:
     * Fixed thread pool shared by the agent.
     */
    private final ExecutorService threadPool;

    public AgentServiceImpl(String agentId)
            throws RemoteException {

        this(agentId, 4);
    }

    public AgentServiceImpl(String agentId, int poolSize)
            throws RemoteException {

        super();

        this.agentId = agentId;

        this.threadPool =
                Executors.newFixedThreadPool(poolSize);
>>>>>>> bf3b5a39342d188269adf8991709e660f7025aee
    }

    @Override
    public Result execute(Subtask subtask)
            throws RemoteException {

        queueDepth.incrementAndGet();

        try {
<<<<<<< HEAD
            System.out.println(
                    "[" + agentId + "] executing " + subtask
            );

            String output = simpleAgentLogic(subtask);
=======

            System.out.println(
                    "[" + agentId + "] executing "
                    + subtask
                    + " on "
                    + Thread.currentThread().getName()
            );

            String output =
                    simpleAgentLogic(subtask);
>>>>>>> bf3b5a39342d188269adf8991709e660f7025aee

            return new Result(
                    subtask.getSubtaskId(),
                    agentId,
                    output,
                    subtask.getLamportTimestamp() + 1,
                    true
            );

        } finally {
<<<<<<< HEAD
=======

>>>>>>> bf3b5a39342d188269adf8991709e660f7025aee
            queueDepth.decrementAndGet();
        }
    }

<<<<<<< HEAD
    private String simpleAgentLogic(Subtask subtask) {
=======
    @Override
    public List<Result> executeBatch(
            List<Subtask> subtasks)
            throws RemoteException {

        List<Future<Result>> futures =
                new ArrayList<>();

        /*
         * Submit every subtask to the thread pool.
         */
        for (Subtask subtask : subtasks) {

            Callable<Result> job =
                    () -> execute(subtask);

            futures.add(
                    threadPool.submit(job)
            );
        }

        /*
         * Collect results in submission order.
         */
        List<Result> results =
                new ArrayList<>();

        for (Future<Result> future : futures) {

            try {

                results.add(
                        future.get()
                );

            } catch (Exception e) {

                throw new RemoteException(
                        "Subtask execution failed in thread pool",
                        e
                );
            }
        }

        return results;
    }

    private String simpleAgentLogic(
            Subtask subtask) {

        /*
         * Simulated I/O latency.
         */
        simulateIoLatency();
>>>>>>> bf3b5a39342d188269adf8991709e660f7025aee

        switch (subtask.getType()) {

            case RETRIEVE:
<<<<<<< HEAD
                return "retrieved[" +
                        subtask.getPayload() + "]";

            case RANK:
                return "ranked[" +
                        subtask.getPayload() + "]";

            case SUMMARIZE:
                return "summary of: " +
                        subtask.getPayload();

            case SYNTHESIZE:
                return "synthesized: " +
                        subtask.getPayload();
=======
                return "retrieved["
                        + subtask.getPayload()
                        + "]";

            case RANK:
                return "ranked["
                        + subtask.getPayload()
                        + "]";

            case SUMMARIZE:
                return "summary of: "
                        + subtask.getPayload();

            case SYNTHESIZE:
                return "synthesized: "
                        + subtask.getPayload();
>>>>>>> bf3b5a39342d188269adf8991709e660f7025aee

            default:
                return "unknown-op";
        }
    }

<<<<<<< HEAD
    @Override
    public boolean ping() throws RemoteException {
=======
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
    public boolean ping()
            throws RemoteException {

>>>>>>> bf3b5a39342d188269adf8991709e660f7025aee
        return true;
    }

    @Override
    public String getAgentId()
            throws RemoteException {

        return agentId;
    }

    @Override
    public int getQueueDepth()
            throws RemoteException {

        return queueDepth.get();
    }
}