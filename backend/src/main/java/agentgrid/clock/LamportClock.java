package agentgrid.clock;

/**
 * Lamport logical clock for a single AgentGrid-Lite node.
 *
 * Why this exists in AgentGrid-Lite:
 * Agents in this system are separate JVM processes that never share memory:
 * a client decomposes a research query, ships Subtasks to agents over RMI,
 * and each agent ships a Result back. There is no global wall clock those processes
 * can trust, so "which agent event happened first" cannot be answered by comparing
 * System.currentTimeMillis() readings — they drift across nodes.
 *
 * What we actually need is causal order, not real time: a RETRIEVE that
 * fed a RANK must be orderable before that RANK in the shared trace log.
 * Lamport's rules give exactly that guarantee (if event a causally precedes event b,
 * then L(a) < L(b)) with zero overhead beyond a counter in messages.
 *
 * The timestamp travels in Subtask.lamportTimestamp on the way out and
 * Result.lamportTimestamp on the way back.
 *
 * Why every method is synchronized:
 * Agent execution uses thread pools and RMI dispatches on worker threads.
 * A single agent runs several execute() calls concurrently, all hitting this clock.
 * The read-modify-write in update(long) must be atomic to prevent concurrent
 * subtasks from receiving duplicate logical timestamps.
 */
public class LamportClock {

    /** Monotonically non-decreasing logical time for this node. Guarded by this. */
    private long counter;

    /**
     * Creates a clock starting at logical time 0.
     */
    public LamportClock() {
        this.counter = 0L;
    }

    /**
     * Creates a clock starting at an explicit logical time.
     *
     * @param initialTime starting counter value, must not be negative
     */
    public LamportClock(long initialTime) {
        if (initialTime < 0L) {
            throw new IllegalArgumentException("initialTime must be >= 0, got " + initialTime);
        }
        this.counter = initialTime;
    }

    /**
     * Lamport rule for a local or send event: advance the clock, then stamp the event.
     *
     * @return the logical time assigned to this event
     */
    public synchronized long tick() {
        return ++counter;
    }

    /**
     * Lamport rule for a receive event: counter = max(counter, received) + 1.
     *
     * @param receivedTimestamp logical time carried by the inbound message
     * @return the logical time assigned to this receive event
     */
    public synchronized long update(long receivedTimestamp) {
        long safeReceived = Math.max(receivedTimestamp, 0L);
        counter = Math.max(counter, safeReceived) + 1L;
        return counter;
    }

    /**
     * Reads the current logical time without advancing it.
     *
     * @return the current counter value
     */
    public synchronized long getTime() {
        return counter;
    }

    @Override
    public synchronized String toString() {
        return "LamportClock{" + counter + "}";
    }
}
