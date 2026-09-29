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

    /**
     * Reads this agent's current logical clock timestamp over RMI.
     *
     * @return current Lamport logical timestamp
     * @throws RemoteException if an RMI communication failure occurs
     */
    long getLamportTime() throws RemoteException;

    /**
     * Returns the simulated work duration in ms for this node.
     */
    long getSimulatedWorkMs() throws RemoteException;

    /**
     * Syncs lamport clock with control plane.
     */
    long sync(long controlLamport) throws RemoteException;
}

