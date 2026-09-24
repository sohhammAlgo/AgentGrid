package agentgrid.election;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Remote interface exposing leader election operations over RMI.
 * Supports both Bully and Ring election protocols.
 */
public interface ElectionNodeService extends Remote {

    /**
     * Bully: Received an ELECTION message from a lower-ID node.
     * @param senderId ID of the node initiating the election
     */
    void receiveBullyElection(int senderId) throws RemoteException;

    /**
     * Bully: Received an ANSWER/OK message from a higher-ID node.
     * @param responderId ID of the higher node responding
     */
    void receiveBullyAnswer(int responderId) throws RemoteException;

    /**
     * Bully: Received a COORDINATOR message declaring the winner.
     * @param leaderId ID of the newly elected leader node
     */
    void receiveBullyCoordinator(int leaderId) throws RemoteException;

    /**
     * Ring: Received a message circulating around the logical ring.
     * @param message RingMessage containing type, candidate ID, and hop count
     */
    void receiveRingMessage(RingMessage message) throws RemoteException;

    /**
     * Node ID (integer identifier, higher number = higher priority).
     */
    int getNodeId() throws RemoteException;

    /**
     * Currently recognized leader ID.
     */
    int getLeaderId() throws RemoteException;

    /**
     * Checks if this node currently considers itself the leader.
     */
    boolean isLeader() throws RemoteException;

    /**
     * Checks if this node is active (not crashed).
     */
    boolean isAlive() throws RemoteException;

    /**
     * Simulates a crash-stop failure on this node.
     */
    void simulateCrash() throws RemoteException;

    /**
     * Recovers a crashed node.
     */
    void recover() throws RemoteException;

    /**
     * Triggers leader election on this node.
     */
    void startElection() throws RemoteException;
}
