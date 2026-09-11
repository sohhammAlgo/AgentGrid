package agentgrid.clock;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * A node's physical clock, exported over RMI for Berkeley synchronization.
 *
 * Why the artificial drift:
 * JVM processes on one machine read the same hardware clock, so their System.currentTimeMillis()
 * values are identical. Each node is constructed with a fixed drift offset to simulate
 * clock skew across physical nodes in a distributed system.
 *
 * Why an offset instead of setting system clock:
 * adjustTime() only mutates offsetMillis, keeping the process unprivileged and leaving
 * system time untouched for other applications.
 */
public class TimeServiceImpl extends UnicastRemoteObject implements TimeService {

    private static final long serialVersionUID = 1L;

    private final String nodeId;
    private volatile long offsetMillis;
    private final long initialDriftMillis;

    /**
     * Creates a node clock skewed by an artificial drift.
     *
     * @param nodeId      registry name of this node
     * @param driftMillis simulated skew; positive runs fast, negative runs slow
     * @throws RemoteException if the object cannot be exported
     */
    public TimeServiceImpl(String nodeId, long driftMillis) throws RemoteException {
        super();
        if (nodeId == null || nodeId.isEmpty()) {
            throw new IllegalArgumentException("nodeId must be a non-empty name");
        }
        this.nodeId = nodeId;
        this.offsetMillis = driftMillis;
        this.initialDriftMillis = driftMillis;
    }

    @Override
    public long getTime() {
        return System.currentTimeMillis() + offsetMillis;
    }

    @Override
    public synchronized void adjustTime(long deltaMillis) {
        long previousOffset = offsetMillis;
        offsetMillis = previousOffset + deltaMillis;

        System.out.println(
                "[" + nodeId + "] correction "
                + formatSigned(deltaMillis)
                + " ms | offset "
                + formatSigned(previousOffset)
                + " -> "
                + formatSigned(offsetMillis)
                + " ms (initial drift "
                + formatSigned(initialDriftMillis)
                + " ms)"
        );
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Current offset from the machine clock, for local inspection.
     *
     * @return this node's accumulated offset in milliseconds
     */
    public long getOffsetMillis() {
        return offsetMillis;
    }

    /**
     * Renders a signed millisecond value with an explicit '+' so drift direction is clear.
     */
    private static String formatSigned(long millis) {
        return (millis >= 0 ? "+" : "") + millis;
    }
}
