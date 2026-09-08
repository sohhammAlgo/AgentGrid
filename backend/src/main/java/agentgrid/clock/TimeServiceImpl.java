package agentgrid.clock;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * A node's physical clock, exported over RMI for Berkeley synchronization.
 *
 * <p><b>Why the artificial drift.</b> AgentGrid-Lite simulates a distributed grid
 * as several JVM processes on one laptop. Those processes all read the same
 * hardware clock, so their {@code System.currentTimeMillis()} readings are
 * identical to the millisecond — there is no real skew for Berkeley to correct,
 * and running the algorithm against them would prove nothing. Each node is
 * therefore constructed with a fixed drift that is added to every reading,
 * manufacturing the disagreement that separate physical machines would exhibit
 * naturally. Experiment 3 then shows the algorithm collapsing that disagreement.
 *
 * <p><b>Why an offset instead of setting the clock.</b> {@code adjustTime()} only
 * mutates {@link #offsetMillis}; it never touches the system clock. That keeps the
 * node unprivileged, leaves every other process on the machine alone, and keeps
 * this node's time monotonic under forward corrections. Backward corrections do
 * step the node's reported time — the standard Berkeley caveat — which is
 * acceptable here because the timestamps consumed downstream (Exp 5's blackboard,
 * Exp 8's task-graph mirroring) are compared across nodes rather than used to
 * measure local elapsed intervals.
 *
 * <p><b>Concurrency.</b> {@link #offsetMillis} is {@code volatile} so agent threads
 * reading the clock see a correction as soon as the coordinator applies it, without
 * paying for a lock on the read path. {@link #adjustTime(long)} is synchronized
 * because {@code offset += delta} is a read-modify-write that {@code volatile}
 * alone does not make atomic; corrections normally arrive from a single
 * coordinator, but RMI dispatches on arbitrary threads and a lost update here would
 * silently leave the node unsynchronized.
 */
public class TimeServiceImpl
        extends UnicastRemoteObject
        implements TimeService {

    private static final long serialVersionUID = 1L;

    private final String nodeId;

    /**
     * Signed offset added to the machine clock on every reading.
     * Seeded with the simulated drift, then moved by Berkeley corrections.
     */
    private volatile long offsetMillis;

    /**
     * Drift this node started with, retained purely so logs can show how far the
     * node has been pulled from where it began.
     */
    private final long initialDriftMillis;

    /**
     * Creates a node clock skewed by an artificial drift.
     *
     * @param nodeId       registry name of this node
     * @param driftMillis  simulated skew; positive runs fast, negative runs slow
     * @throws RemoteException if the object cannot be exported
     */
    public TimeServiceImpl(String nodeId, long driftMillis)
            throws RemoteException {

        super();

        if (nodeId == null || nodeId.isEmpty()) {

            throw new IllegalArgumentException(
                    "nodeId must be a non-empty name"
            );
        }

        this.nodeId = nodeId;
        this.offsetMillis = driftMillis;
        this.initialDriftMillis = driftMillis;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The machine clock plus this node's offset. Every AgentGrid-Lite component
     * that needs a comparable wall-clock reading must go through here rather than
     * calling {@code System.currentTimeMillis()} directly, or it will bypass the
     * correction and reintroduce the skew.
     */
    @Override
    public long getTime() {

        return System.currentTimeMillis() + offsetMillis;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Logs the correction so the lab report can show each node being pulled
     * toward the group average.
     */
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
     * Renders a signed millisecond value with an explicit {@code +} so drift
     * direction is readable at a glance in the logs.
     */
    private static String formatSigned(long millis) {

        return (millis >= 0 ? "+" : "") + millis;
    }
}
