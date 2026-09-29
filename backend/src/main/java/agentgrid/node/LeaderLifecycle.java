package agentgrid.node;

/**
 * Leadership transitions seen by one node. Election code reports transitions to a
 * LeaderLifecycleRegistry; later phases register listeners there to react to them.
 */
public interface LeaderLifecycle {

    /** This node has become leader. */
    void onElected(int nodeId);

    /** This node was leader and has accepted another node as leader. */
    void onDemoted(int nodeId, int newLeaderId);

    /** This node's failure detector declared its leader dead. */
    void onLeaderLost(int nodeId, int lostLeaderId);
}
