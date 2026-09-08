package agentgrid.clock;

/**
 * Lamport logical clock for a single AgentGrid-Lite node.
 *
 * <p><b>Why this exists in AgentGrid-Lite.</b> Agents in this system are separate
 * JVM processes that never share memory: a client decomposes a research query,
 * ships {@code Subtask}s to agents over RMI, and each agent ships a {@code Result}
 * back. There is no global wall clock those processes can trust, so "which agent
 * event happened first" cannot be answered by comparing
 * {@code System.currentTimeMillis()} readings — they drift, and on a laptop
 * simulating N nodes they are only accidentally close.
 *
 * <p>What we actually need is <i>causal</i> order, not real time: a RETRIEVE that
 * fed a RANK must be orderable before that RANK in the shared trace log, no matter
 * which node ran either one or how their wall clocks compare. Lamport's rules give
 * exactly that guarantee — if event <i>a</i> causally precedes event <i>b</i>, then
 * {@code L(a) < L(b)} — and cost nothing but a counter piggybacked on the
 * {@code Subtask}/{@code Result} messages already crossing the wire.
 *
 * <p>The timestamp travels in {@code Subtask.lamportTimestamp} on the way out and
 * {@code Result.lamportTimestamp} on the way back, which is why both data models
 * carry that field. Sorting the merged trace log by it reconstructs a legal
 * execution order for the whole grid.
 *
 * <p><b>Why every method is synchronized.</b> Experiment 2 gave each agent a real
 * {@code ExecutorService} thread pool, and RMI dispatches incoming calls on its own
 * threads on top of that. So a single agent genuinely runs several
 * {@code execute()} calls at once, all hitting this one clock. The read-modify-write
 * in {@link #update(long)} is not atomic and cannot be made so with a plain
 * {@code AtomicLong} increment, since the new value depends on both the current
 * counter and the received timestamp. Without synchronization two concurrent
 * subtasks could be issued the same logical time, which breaks the ordering the
 * whole mechanism exists to provide.
 *
 * <p>This class deliberately does not implement {@code Remote}: a logical clock is
 * node-local state, advanced only as a side effect of messages the node actually
 * sends or receives.
 */
public class LamportClock {

    /**
     * Monotonically non-decreasing logical time for this node.
     * Guarded by {@code this}.
     */
    private long counter;

    /**
     * Creates a clock starting at logical time 0, so the node's first
     * event is stamped 1.
     */
    public LamportClock() {

        this.counter = 0L;
    }

    /**
     * Creates a clock starting at an explicit logical time.
     * Useful when a node is restarted and must not reissue timestamps it
     * already handed out before the crash.
     *
     * @param initialTime starting counter value, must not be negative
     */
    public LamportClock(long initialTime) {

        if (initialTime < 0L) {

            throw new IllegalArgumentException(
                    "initialTime must be >= 0, got " + initialTime
            );
        }

        this.counter = initialTime;
    }

    /**
     * Lamport rule for a local or send event: advance the clock, then stamp
     * the event with the new value.
     *
     * <p>Called before an agent originates a message rather than reacting to
     * one — for example when a coordinator emits the subtasks of a freshly
     * decomposed query.
     *
     * @return the logical time assigned to this event
     */
    public synchronized long tick() {

        return ++counter;
    }

    /**
     * Lamport rule for a receive event:
     * {@code counter = max(counter, received) + 1}.
     *
     * <p>Taking the max absorbs the sender's knowledge of time, so this node can
     * never stamp the receipt of a message at or below the timestamp the message
     * carried; the {@code + 1} then makes the receive strictly later than the send.
     * Together those give the happens-before guarantee across nodes.
     *
     * <p>In AgentGrid-Lite this is what {@code AgentServiceImpl.execute()} calls
     * with the incoming {@code Subtask}'s timestamp, and the value returned here is
     * the one stamped onto the outgoing {@code Result}.
     *
     * @param receivedTimestamp logical time carried by the inbound message;
     *                          negative values are treated as 0, so a subtask
     *                          built without a timestamp cannot drag the clock back
     * @return the logical time assigned to this receive event
     */
    public synchronized long update(long receivedTimestamp) {

        long safeReceived =
                Math.max(receivedTimestamp, 0L);

        counter = Math.max(counter, safeReceived) + 1L;

        return counter;
    }

    /**
     * Reads the current logical time without advancing it.
     *
     * <p>Strictly for observation — trace logs, queue-depth reports, assertions in
     * the experiment clients. Reading is not an event, so it must not tick.
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
