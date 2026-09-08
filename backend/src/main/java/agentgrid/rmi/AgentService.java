package agentgrid.rmi;

import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface AgentService extends Remote {

    Result execute(Subtask subtask) throws RemoteException;

    /**
     * Execute several subtasks concurrently on this node's thread pool
     * and return all results once every subtask has completed.
     *
     * Experiment 2:
     * proves multithreaded execution inside one agent,
     * as distinct from Experiment 1's single-subtask call path.
     */
    List<Result> executeBatch(List<Subtask> subtasks)
            throws RemoteException;

    boolean ping() throws RemoteException;

    String getAgentId() throws RemoteException;

    int getQueueDepth() throws RemoteException;
}
