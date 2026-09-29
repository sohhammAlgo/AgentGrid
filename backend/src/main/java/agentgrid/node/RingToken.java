package agentgrid.node;

import java.io.Serializable;

/**
 * Ring election token. ELECTION tokens carry the highest candidate id seen so far and
 * return to their initiator; COORDINATOR tokens announce the winner for one pass.
 */
public final class RingToken implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Kind { ELECTION, COORDINATOR }

    private final Kind kind;
    private final int initiator;
    private final int candidate;
    private final long round;
    private final int hops;

    public RingToken(Kind kind, int initiator, int candidate, long round, int hops) {
        this.kind = kind;
        this.initiator = initiator;
        this.candidate = candidate;
        this.round = round;
        this.hops = hops;
    }

    public Kind getKind() { return kind; }
    /** ELECTION: the node that started the token. COORDINATOR: the node announcing. */
    public int getInitiator() { return initiator; }
    public int getCandidate() { return candidate; }
    public long getRound() { return round; }
    public int getHops() { return hops; }

    public RingToken next(int newCandidate) {
        return new RingToken(kind, initiator, newCandidate, round, hops + 1);
    }

    @Override
    public String toString() {
        return kind + "{initiator=" + initiator + ", candidate=" + candidate + ", hops=" + hops + "}";
    }
}
