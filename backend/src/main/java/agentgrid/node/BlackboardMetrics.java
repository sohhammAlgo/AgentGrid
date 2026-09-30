package agentgrid.node;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Write counters of one replica and a bounded window (last WINDOW samples per mode) of write
 * latencies, from which medians are computed. Thread-safe; copy() gives the serializable view.
 */
public final class BlackboardMetrics implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final int WINDOW = 200;

    private long strongStored;
    private long strongDegraded;
    private long strongRefused;
    private long strongFailedPartial;
    private long eventualAccepted;
    private long eventualRetries;
    private long eventualDelivered;
    private long antiEntropyApplied;
    private int eventualPending;
    private final ArrayDeque<Long> strongLatency = new ArrayDeque<>();
    private final ArrayDeque<Long> eventualLatency = new ArrayDeque<>();

    synchronized void recordWrite(BlackboardWriteOutcome outcome) {
        switch (outcome.getStatus()) {
            case STORED: strongStored++; break;
            case STORED_DEGRADED: strongDegraded++; break;
            case REFUSED: strongRefused++; break;
            case FAILED_PARTIAL: strongFailedPartial++; break;
            case ACCEPTED: eventualAccepted++; break;
            default: break;
        }
        ArrayDeque<Long> window = "STRONG".equals(outcome.getMode()) ? strongLatency : eventualLatency;
        window.addLast(outcome.getLatencyMs());
        while (window.size() > WINDOW) {
            window.removeFirst();
        }
    }

    synchronized void retried() { eventualRetries++; }
    synchronized void delivered() { eventualDelivered++; }
    synchronized void antiEntropyApplied(int n) { antiEntropyApplied += n; }

    synchronized BlackboardMetrics copy(int pending) {
        BlackboardMetrics c = new BlackboardMetrics();
        c.strongStored = strongStored;
        c.strongDegraded = strongDegraded;
        c.strongRefused = strongRefused;
        c.strongFailedPartial = strongFailedPartial;
        c.eventualAccepted = eventualAccepted;
        c.eventualRetries = eventualRetries;
        c.eventualDelivered = eventualDelivered;
        c.antiEntropyApplied = antiEntropyApplied;
        c.eventualPending = pending;
        c.strongLatency.addAll(strongLatency);
        c.eventualLatency.addAll(eventualLatency);
        return c;
    }

    public long getStrongStored() { return strongStored; }
    public long getStrongDegraded() { return strongDegraded; }
    public long getStrongRefused() { return strongRefused; }
    public long getStrongFailedPartial() { return strongFailedPartial; }
    public long getEventualAccepted() { return eventualAccepted; }
    public long getEventualRetries() { return eventualRetries; }
    public long getEventualDelivered() { return eventualDelivered; }
    public long getAntiEntropyApplied() { return antiEntropyApplied; }
    public int getEventualPending() { return eventualPending; }
    public List<Long> strongLatencySamples() { return new ArrayList<>(strongLatency); }
    public List<Long> eventualLatencySamples() { return new ArrayList<>(eventualLatency); }
}
