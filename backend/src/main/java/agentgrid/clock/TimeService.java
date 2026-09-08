package agentgrid.clock;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Remote interface exposing one AgentGrid-Lite node's physical clock so a
 * coordinator can read it and correct it.
 *
 * <p><b>Why this exists in AgentGrid-Lite.</b> Lamport clocks
 * ({@link LamportClock}) order events causally, but they say nothing about real
 * elapsed time and cannot be compared against anything outside the message chain
 * that produced them. Several parts of this system need wall-clock numbers that
 * are comparable <i>across</i> nodes:
 *
 * <ul>
 *   <li>Experiment 5's blackboard has multiple agents writing partial findings to
 *       shared state. Deciding whether agent B's write superseded agent A's, or
 *       aging out stale entries, means comparing write timestamps produced on
 *       different nodes. If those clocks disagree by seconds, a newer finding can
 *       look older than the one it replaced.</li>
 *   <li>Experiment 8 mirrors the task graph across nodes. Reconciling two replicas
 *       after a failure uses timestamps to decide which copy of a node's state is
 *       fresher, so skewed clocks resolve the merge the wrong way.</li>
 * </ul>
 *
 * <p>Berkeley's algorithm is the right fit here because AgentGrid-Lite is
 * zero-cost and offline: there is no NTP server or external time authority to sync
 * against. Berkeley needs none — it makes the nodes agree with <i>each other</i> by
 * averaging their clocks, which is all the comparisons above actually require.
 *
 * <p>Kept deliberately separate from {@code AgentService}: time sync is
 * infrastructure that must keep working regardless of whether a node is busy
 * executing subtasks, and a node may be sync'd before any agent work is bound to it.
 */
public interface TimeService extends Remote {

    /**
     * Reads this node's current physical time.
     *
     * @return milliseconds since the epoch as this node believes them to be,
     *         i.e. the machine clock plus this node's accumulated offset
     * @throws RemoteException if the node is unreachable
     */
    long getTime() throws RemoteException;

    /**
     * Applies a correction handed down by the Berkeley coordinator.
     *
     * <p>Shifts the node's notion of time by {@code deltaMillis}; positive moves it
     * forward, negative moves it back. The correction adjusts an offset rather than
     * the underlying system clock, so the node never needs elevated privileges and
     * the machine clock stays untouched for every other process on the laptop.
     *
     * @param deltaMillis signed correction in milliseconds
     * @throws RemoteException if the node is unreachable
     */
    void adjustTime(long deltaMillis) throws RemoteException;

    /**
     * Identity of this node, matching the name it is bound under in the RMI
     * registry, so coordinator output names the node a correction was sent to.
     *
     * @return this node's identifier
     * @throws RemoteException if the node is unreachable
     */
    String getNodeId() throws RemoteException;
}
