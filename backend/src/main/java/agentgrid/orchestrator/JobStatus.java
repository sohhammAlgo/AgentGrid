package agentgrid.orchestrator;

/**
 * Lifecycle of a job. ORPHANED: its leader stopped (killed or demoted) before it finished and
 * nothing took it over. RECOVERED: its leader stopped, the backup took over, and the job then
 * finished (primary-backup replication, Exp 8).
 */
public enum JobStatus {
    QUEUED, RUNNING, COMPLETE, FAILED, ORPHANED, RECOVERED;

    public boolean isActive() {
        return this == QUEUED || this == RUNNING;
    }
}
