package agentgrid.node;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Ring election for the integrated cluster.
 *
 * The initiator sends an ELECTION token carrying a candidate id. Each node replaces the
 * candidate with its own id if higher and forwards the token to the next alive successor
 * by id, skipping nodes whose RMI call fails. When the token returns to the initiator the
 * candidate (the highest id on the ring) wins, and the initiator starts one COORDINATOR
 * pass, which stops before it would reach the announcer again.
 *
 * With several initiators, a token is swallowed at an active initiator with a higher id,
 * because that node's own token will elect the same winner. An initiator whose token has
 * not returned after TOKEN_TIMEOUT_MS restarts its election.
 */
final class RingNode {

    static final long TOKEN_TIMEOUT_MS = 2000L;

    private final ElectionNode node;
    private long roundGen = 0;
    /** Round of this node's in-flight ELECTION token, or 0 when not initiating. Guarded by this. */
    private long activeRound = 0;

    RingNode(ElectionNode node) {
        this.node = node;
    }

    synchronized boolean isRunning() {
        return activeRound != 0;
    }

    void start(String reason) {
        if (!node.alive()) {
            return;
        }
        long round;
        synchronized (this) {
            if (activeRound != 0) {
                return;
            }
            round = ++roundGen;
            activeRound = round;
            node.beginElection(ElectionNode.RING, reason);
        }
        int me = node.id();
        node.timers().schedule(() -> onTimeout(round), TOKEN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        node.exec().submit(() -> forward(new RingToken(RingToken.Kind.ELECTION, me, me, round, 0)));
    }

    private void onTimeout(long round) {
        synchronized (this) {
            if (activeRound != round) {
                return;
            }
            activeRound = 0;
        }
        node.exec().submit(() -> start("token did not return within " + TOKEN_TIMEOUT_MS + " ms; restarting"));
    }

    void onToken(RingToken token) {
        if (token.getKind() == RingToken.Kind.ELECTION) {
            onElectionToken(token);
        } else {
            onCoordinatorToken(token);
        }
    }

    private void onElectionToken(RingToken token) {
        int me = node.id();
        if (token.getInitiator() == me) {
            synchronized (this) {
                if (token.getRound() != activeRound) {
                    return;  // stale token of a cancelled or restarted round
                }
                activeRound = 0;
                int winner = Math.max(token.getCandidate(), me);
                node.acceptLeader(winner, "ring: own token returned, highest id " + winner);
            }
            int winner = Math.max(token.getCandidate(), me);
            forward(new RingToken(RingToken.Kind.COORDINATOR, me, winner, token.getRound(), 0));
            return;
        }
        synchronized (this) {
            if (activeRound != 0 && token.getInitiator() < me) {
                System.out.println("[Node " + me + "] swallowing ring token of lower initiator "
                        + token.getInitiator() + " (own token in flight)");
                return;
            }
        }
        forward(token.next(Math.max(token.getCandidate(), me)));
    }

    private void onCoordinatorToken(RingToken token) {
        int me = node.id();
        int winner = token.getCandidate();
        if (winner < me) {
            System.out.println("[Node " + me + "] ignoring ring COORDINATOR for lower node " + winner);
            start("COORDINATOR for lower node " + winner + " ignored");
            return;
        }
        synchronized (this) {
            activeRound = 0;  // a winner is known; cancel this node's own token if any
            node.acceptLeader(winner, "ring: COORDINATOR from node " + token.getInitiator());
        }
        forward(token.next(winner));
    }

    /**
     * Sends the token to the next alive successor. ELECTION tokens are dropped if their
     * initiator is unreachable (it would circulate forever); COORDINATOR passes stop
     * before reaching their announcer. If every other node is down, an ELECTION token
     * is handed back to this node.
     */
    private void forward(RingToken token) {
        int me = node.id();
        List<Integer> successors = node.successors();
        if (token.getHops() > 2 * (successors.size() + 1)) {
            return;
        }
        String kind = token.getKind().name();
        for (int next : successors) {
            if (token.getKind() == RingToken.Kind.COORDINATOR && next == token.getInitiator()) {
                return;
            }
            boolean delivered = node.send(next, kind, "(initiator " + token.getInitiator()
                    + ", candidate " + token.getCandidate() + ")",
                    lamport -> p -> {
                        p.ringToken(token, lamport);
                        return null;
                    });
            if (delivered) {
                return;
            }
            if (token.getKind() == RingToken.Kind.ELECTION && next == token.getInitiator()) {
                return;
            }
        }
        if (token.getKind() == RingToken.Kind.ELECTION && token.getInitiator() == me) {
            onElectionToken(token);
        }
    }
}
