package agentgrid.replication;

/**
 * Runtime-selectable consistency protocol for replicated blackboard writes.
 */
public enum ConsistencyLevel {
    /**
     * Synchronous write replication: Write blocks until 100% of peer nodes acknowledge
     * receipt of the research finding. Guarantees zero stale reads across all nodes.
     */
    STRONG,

    /**
     * Asynchronous write replication: Write returns immediately after local update;
     * background daemon thread propagates findings to peers using Last-Write-Wins (LWW).
     * Provides low write response latency with a short temporary staleness window.
     */
    EVENTUAL
}
