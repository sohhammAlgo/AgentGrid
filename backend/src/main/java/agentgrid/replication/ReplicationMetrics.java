package agentgrid.replication;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Empirical metrics collector for quantitative comparison of Strong vs Eventual consistency modes.
 */
public class ReplicationMetrics {

    private final AtomicLong totalWriteLatencyMs = new AtomicLong(0);
    private final AtomicInteger writeCount = new AtomicInteger(0);
    private final AtomicInteger staleReadCount = new AtomicInteger(0);
    private final AtomicInteger totalReadCount = new AtomicInteger(0);

    public void reset() {
        totalWriteLatencyMs.set(0);
        writeCount.set(0);
        staleReadCount.set(0);
        totalReadCount.set(0);
    }

    public void recordWriteLatency(long latencyMs) {
        totalWriteLatencyMs.addAndGet(latencyMs);
        writeCount.incrementAndGet();
    }

    public void recordRead(boolean isStale) {
        totalReadCount.incrementAndGet();
        if (isStale) {
            staleReadCount.incrementAndGet();
        }
    }

    public double getAverageWriteLatencyMs() {
        int count = writeCount.get();
        if (count == 0) return 0.0;
        return (double) totalWriteLatencyMs.get() / count;
    }

    public int getStaleReadCount() {
        return staleReadCount.get();
    }

    public int getTotalReadCount() {
        return totalReadCount.get();
    }

    public double getStalenessPercentage() {
        int total = totalReadCount.get();
        if (total == 0) return 0.0;
        return ((double) staleReadCount.get() / total) * 100.0;
    }
}
