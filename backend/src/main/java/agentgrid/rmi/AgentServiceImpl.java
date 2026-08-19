package agentgrid.rmi;

import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.atomic.AtomicInteger;

public class AgentServiceImpl
        extends UnicastRemoteObject
        implements AgentService {

    private static final long serialVersionUID = 1L;

    private final String agentId;
    private final AtomicInteger queueDepth =
            new AtomicInteger(0);

    public AgentServiceImpl(String agentId)
            throws RemoteException {

        super();
        this.agentId = agentId;
    }

    @Override
    public Result execute(Subtask subtask)
            throws RemoteException {

        queueDepth.incrementAndGet();

        try {
            System.out.println(
                    "[" + agentId + "] executing " + subtask
            );

            String output = simpleAgentLogic(subtask);

            return new Result(
                    subtask.getSubtaskId(),
                    agentId,
                    output,
                    subtask.getLamportTimestamp() + 1,
                    true
            );

        } finally {
            queueDepth.decrementAndGet();
        }
    }

    private String simpleAgentLogic(Subtask subtask) {

        switch (subtask.getType()) {

            case RETRIEVE:
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

            default:
                return "unknown-op";
        }
    }

    @Override
    public boolean ping() throws RemoteException {
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