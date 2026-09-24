package agentgrid.clock;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Remote interface exposing one AgentGrid-Lite node's physical clock so a
 * coordinator can read it and correct it.
 *
 * Why this exists in AgentGrid-Lite:
 * Lamport clocks order events causally, but say nothing about real elapsed time.
 * Components like the replicated blackboard in Experiment 5 need wall-clock numbers
 * that are comparable across nodes to resolve competing writes and timestamps.
 *
 * Berkeley's algorithm is used because AgentGrid-Lite is offline and peer-to-peer:
 * there is no external NTP authority needed. Nodes agree with each other by averaging
 * their clocks.
 */
public interface TimeService extends Remote {

    /**
     * Reads this node's current physical time.
     *
     * @return milliseconds since epoch as this node believes them to be
     * @throws RemoteException if the node is unreachable
     */
    long getTime() throws RemoteException;

    /**
     * Applies a correction handed down by the Berkeley coordinator.
     *
     * @param deltaMillis signed correction in milliseconds
     * @throws RemoteException if the node is unreachable
     */
    void adjustTime(long deltaMillis) throws RemoteException;

    /**
     * Identity of this node, matching the name it is bound under in the RMI registry.
     *
     * @return this node's identifier
     * @throws RemoteException if the node is unreachable
     */
    String getNodeId() throws RemoteException;
}
