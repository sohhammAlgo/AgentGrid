package agentgrid.orchestrator;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Remote interface of a node's orchestrator, bound as "orchestrator" on every node.
 * Only the elected leader's orchestrator is active and accepts jobs.
 */
public interface OrchestratorService extends Remote {

    /** Queues a job; throws if this node's orchestrator is not active (not the leader). */
    String submit(String query, String policy) throws RemoteException;

    /** A snapshot of the job, or null if this orchestrator does not know it. */
    Job getJob(String jobId) throws RemoteException;

    /** Snapshots of the jobs this orchestrator knows, most recent first. */
    List<Job> listJobs() throws RemoteException;

    /**
     * Takes over the record of a job orphaned by a leader that died: keeps it as ORPHANED
     * with its completed subtasks and records JOB_ORPHANED. Does not resume it.
     * Returns the stored snapshot.
     */
    Job adoptOrphan(Job job) throws RemoteException;

    boolean isActive() throws RemoteException;
}
