package agentgrid.node;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bully election for the integrated cluster.
 *
 * Differences from the submitted BullyElectionNode:
 *  - A node that got an ANSWER waits up to COORDINATOR_TIMEOUT_MS for COORDINATOR and,
 *    if none arrives, RESTARTS its election. It never claims leadership after that timeout
 *    (submitted code: a lower node claimed while a higher node was still electing).
 *  - COORDINATOR from a lower id is ignored and triggers this node's own election.
 *  - No convergence timing here; the control plane derives it from the events.
 */
final class BullyNode {

    static final long ANSWER_TIMEOUT_MS = 500L;
    static final long COORDINATOR_TIMEOUT_MS = 2000L;

    private final ElectionNode node;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final Object lock = new Object();
    private boolean answered;
    private boolean coordinatorSeen;

    BullyNode(ElectionNode node) {
        this.node = node;
    }

    boolean isRunning() {
        return running.get();
    }

    /** Starts this node's election unless one is already running. */
    void start(String reason) {
        if (!node.alive() || !running.compareAndSet(false, true)) {
            return;
        }
        node.exec().submit(() -> {
            try {
                run(reason);
            } catch (RuntimeException e) {
                System.err.println("[Node " + node.id() + "] bully election failed: " + e);
            } finally {
                running.set(false);
            }
        });
    }

    private void run(String firstReason) throws RuntimeException {
        String reason = firstReason;
        while (node.alive()) {
            // Reset and ELECTION_STARTED under the lock, so a COORDINATOR accepted by
            // onCoordinator is never overwritten by this round's "no leader" view.
            synchronized (lock) {
                answered = false;
                coordinatorSeen = false;
                node.beginElection(ElectionNode.BULLY, reason);
            }

            List<Integer> higher = node.higherIds();
            int me = node.id();
            for (int peer : higher) {
                node.exec().submit(() -> node.send(peer, "ELECTION", null,
                        lamport -> p -> {
                            p.bullyElection(me, lamport);
                            return null;
                        }));
            }

            synchronized (lock) {
                if (!higher.isEmpty()) {
                    awaitFlag(ANSWER_TIMEOUT_MS, true);
                }
                if (coordinatorSeen) {
                    return;
                }
                if (!answered) {
                    // Decided and applied under the lock: a COORDINATOR from a higher
                    // node cannot slip in between the check and the claim.
                    claim();
                    return;
                }
            }

            synchronized (lock) {
                awaitFlag(COORDINATOR_TIMEOUT_MS, false);
                if (coordinatorSeen) {
                    return;
                }
            }
            reason = "no COORDINATOR within " + COORDINATOR_TIMEOUT_MS + " ms after ANSWER; restarting";
        }
    }

    /** Waits on lock until an ANSWER (if orAnswer) or a COORDINATOR arrives, or timeout. */
    private void awaitFlag(long timeoutMs, boolean orAnswer) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!coordinatorSeen && !(orAnswer && answered)) {
            long left = deadline - System.currentTimeMillis();
            if (left <= 0) {
                return;
            }
            try {
                lock.wait(left);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** No higher node answered: become leader and tell every lower node. */
    private void claim() {
        int me = node.id();
        node.acceptLeader(me, "bully: no ANSWER from a higher node");
        for (int peer : node.lowerIds()) {
            node.exec().submit(() -> node.send(peer, "COORDINATOR", null,
                    lamport -> p -> {
                        p.bullyCoordinator(me, lamport);
                        return null;
                    }));
        }
    }

    void onElection(int fromId) {
        int me = node.id();
        if (fromId >= me) {
            return;
        }
        node.send(fromId, "ANSWER", null, lamport -> p -> {
            p.bullyAnswer(me, lamport);
            return null;
        });
        start("ELECTION received from node " + fromId);
    }

    void onAnswer(int fromId) {
        synchronized (lock) {
            answered = true;
            lock.notifyAll();
        }
    }

    void onCoordinator(int leader) {
        if (leader < node.id()) {
            System.out.println("[Node " + node.id() + "] ignoring COORDINATOR from lower node " + leader);
            start("COORDINATOR from lower node " + leader + " ignored");
            return;
        }
        synchronized (lock) {
            node.acceptLeader(leader, "bully: COORDINATOR from node " + leader);
            coordinatorSeen = true;
            lock.notifyAll();
        }
    }
}
