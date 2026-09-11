package agentgrid.election;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Empirical metrics tracker for leader election benchmark comparisons.
 * Tracks total messages sent across RMI and convergence latency.
 */
public class ElectionMetrics {

    private final AtomicInteger messageCount = new AtomicInteger(0);
    private final AtomicLong startTime = new AtomicLong(0);
    private final AtomicLong endTime = new AtomicLong(0);
    private volatile int electedLeaderId = -1;

    public void reset() {
        messageCount.set(0);
        startTime.set(0);
        endTime.set(0);
        electedLeaderId = -1;
    }

    public void recordStart() {
        startTime.set(System.currentTimeMillis());
    }

    public void incrementMessages() {
        messageCount.incrementAndGet();
    }

    public void addMessages(int count) {
        messageCount.addAndGet(count);
    }

    public void recordCompletion(int leaderId) {
        this.electedLeaderId = leaderId;
        this.endTime.set(System.currentTimeMillis());
    }

    public int getMessageCount() {
        return messageCount.get();
    }

    public long getConvergenceTimeMs() {
        long start = startTime.get();
        long end = endTime.get();
        if (start == 0 || end == 0) {
            return 0;
        }
        return Math.max(0, end - start);
    }

    public int getElectedLeaderId() {
        return electedLeaderId;
    }
}
