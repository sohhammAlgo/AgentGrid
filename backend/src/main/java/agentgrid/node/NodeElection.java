package agentgrid.node;

import agentgrid.election.ElectionNodeService;

import java.rmi.RemoteException;

/**
 * Election service of an integrated cluster node, bound under the same name as the
 * submitted service ("election-node-<id>") so the Phase 1 health check still passes.
 *
 * Adds runtime algorithm selection and message methods that carry the sender's Lamport
 * time; the receiver applies the Lamport receive rule before handling the message.
 */
public interface NodeElection extends ElectionNodeService {

    /** Selects the algorithm used by elections this node starts: "BULLY" or "RING". */
    void setAlgorithm(String name) throws RemoteException;

    String getAlgorithm() throws RemoteException;

    /** Leader this node currently recognises, or -1 when it has none. */
    @Override
    int getLeaderId() throws RemoteException;

    /** Starts an election with the current algorithm; returns immediately. */
    @Override
    void startElection() throws RemoteException;

    /** True while this node runs its own election (Bully loop, or Ring token in flight). */
    boolean isElecting() throws RemoteException;

    /** Failure-detector ping; returns this node's current leader view. */
    int heartbeat(int fromId) throws RemoteException;

    void bullyElection(int fromId, long senderLamport) throws RemoteException;

    void bullyAnswer(int fromId, long senderLamport) throws RemoteException;

    void bullyCoordinator(int leaderId, long senderLamport) throws RemoteException;

    void ringToken(RingToken token, long senderLamport) throws RemoteException;
}
