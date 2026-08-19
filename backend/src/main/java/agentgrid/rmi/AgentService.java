package agentgrid.rmi;

import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface AgentService extends Remote {

    Result execute(Subtask subtask) throws RemoteException;

    boolean ping() throws RemoteException;

    String getAgentId() throws RemoteException;

    int getQueueDepth() throws RemoteException;
}