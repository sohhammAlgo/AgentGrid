package agentgrid.node;

import agentgrid.rmi.AgentService;

import java.rmi.RemoteException;

/**
 * Remote interface extending the base AgentService with cluster node operations,
 * including introspection of the worker thread pool capacity.
 */
public interface NodeAgent extends AgentService {

    /**
     * Returns the worker thread pool size configured on this node.
     *
     * @return worker thread pool capacity
     * @throws RemoteException if an RMI communication failure occurs
     */
    int getPoolSize() throws RemoteException;
}
