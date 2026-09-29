package agentgrid.orchestrator;

/** Lifecycle of a job. ORPHANED: its leader stopped (killed or demoted) before it finished. */
public enum JobStatus {
    QUEUED, RUNNING, COMPLETE, FAILED, ORPHANED;

    public boolean isActive() {
        return this == QUEUED || this == RUNNING;
    }
}
