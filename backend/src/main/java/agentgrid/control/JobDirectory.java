package agentgrid.control;

import agentgrid.node.ClusterConfig;
import agentgrid.orchestrator.Job;
import agentgrid.orchestrator.JobStatus;
import agentgrid.orchestrator.OrchestratorService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The control plane's record of every job submitted through it.
 *
 * Jobs run on the leader's orchestrator; this keeps the latest snapshot of each, refreshed
 * every monitor cycle while it is QUEUED or RUNNING. A leader that is killed cannot mark its
 * own job, so when the monitor sees a job's leader node DOWN, the job is marked ORPHANED here
 * (its completed subtasks stay as last seen) and, once a new leader is agreed, handed to that
 * leader's orchestrator, which records JOB_ORPHANED. A demoted leader orphans its own jobs.
 */
public class JobDirectory {

    /** An API error with the HTTP status to report. */
    public static class ApiException extends Exception {
        private static final long serialVersionUID = 1L;
        private final int status;

        ApiException(int status, String message) {
            super(message);
            this.status = status;
        }

        public int getStatus() {
            return status;
        }
    }

    private static final int MAX_JOBS = 200;

    private final ClusterConfig config;
    private final ClusterMonitor monitor;
    private final LinkedHashMap<String, Job> jobs = new LinkedHashMap<>();
    private final Set<String> adopted = new HashSet<>();
    private volatile boolean active;

    public JobDirectory(ClusterConfig config, ClusterMonitor monitor) {
        this.config = config;
        this.monitor = monitor;
    }

    /** True while any known job is QUEUED or RUNNING (the monitor then polls fast). */
    public boolean hasActive() {
        return active;
    }

    OrchestratorService orchestratorOf(int nodeId) throws Exception {
        int port = config.getNode(nodeId).getPort();
        Registry registry = monitor.call(() -> LocateRegistry.getRegistry("localhost", port));
        return monitor.call(() -> (OrchestratorService) registry.lookup("orchestrator"));
    }

    /** Submits a job to the agreed leader's orchestrator. */
    public Map<String, Object> submit(String query, String policy) throws ApiException {
        Object leader = monitor.getElectionTracker().toMap().get("leaderId");
        if (!(leader instanceof Integer)) {
            throw new ApiException(503, "no agreed leader; an election may be in progress");
        }
        int leaderId = (Integer) leader;
        String jobId;
        Job snapshot;
        try {
            OrchestratorService orchestrator = orchestratorOf(leaderId);
            jobId = monitor.call(() -> orchestrator.submit(query, policy));
            snapshot = monitor.call(() -> orchestrator.getJob(jobId));
        } catch (Exception e) {
            throw new ApiException(503, "leader node " + leaderId + " did not accept the job: " + rootMessage(e));
        }
        synchronized (this) {
            if (snapshot != null) {
                store(snapshot);
            }
            active = true;
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("jobId", jobId);
        resp.put("leader", leaderId);
        resp.put("status", snapshot == null ? null : snapshot.getStatus().name());
        return resp;
    }

    /** The latest snapshot of a job (refreshed from its leader if it is still running), or null. */
    public Job get(String jobId) {
        Job cached;
        synchronized (this) {
            cached = jobs.get(jobId);
        }
        if (cached == null) {
            return null;
        }
        if (cached.getStatus().isActive()) {
            refresh(cached);
        }
        synchronized (this) {
            Job j = jobs.get(jobId);
            return j == null ? null : j.copy();
        }
    }

    /** Known jobs, most recent first. */
    public synchronized List<Job> list(int limit) {
        List<Job> all = new ArrayList<>(jobs.values());
        List<Job> out = new ArrayList<>();
        for (int i = all.size() - 1; i >= 0 && out.size() < limit; i--) {
            out.add(all.get(i).copy());
        }
        return out;
    }

    /** Called once per monitor cycle, after node statuses and the election state are updated. */
    public void onCycle(List<ClusterMonitor.NodeStatus> statuses, Integer agreedLeader) {
        List<Job> snapshot;
        synchronized (this) {
            snapshot = new ArrayList<>(jobs.values());
        }
        boolean anyActive = false;
        for (Job job : snapshot) {
            int owner = job.getLeaderNode();
            boolean ownerUp = isUp(statuses, owner);
            if (job.getStatus().isActive()) {
                if (ownerUp) {
                    refresh(job);
                } else {
                    synchronized (this) {
                        Job current = jobs.get(job.getJobId());
                        if (current != null) {
                            current.orphan(System.currentTimeMillis());
                        }
                    }
                }
            }
            Job current;
            synchronized (this) {
                current = jobs.get(job.getJobId());
            }
            if (current == null) {
                continue;
            }
            if (current.getStatus().isActive()) {
                anyActive = true;
            }
            if (current.getStatus() == JobStatus.ORPHANED && !ownerUp
                    && agreedLeader != null && agreedLeader != owner) {
                boolean todo;
                synchronized (this) {
                    todo = adopted.add(current.getJobId());
                }
                if (todo) {
                    adopt(current, agreedLeader);
                }
            }
        }
        active = anyActive;
    }

    private void adopt(Job job, int newLeader) {
        try {
            OrchestratorService orchestrator = orchestratorOf(newLeader);
            Job copy = job.copy();
            Job stored = monitor.call(() -> orchestrator.adoptOrphan(copy));
            synchronized (this) {
                store(stored);
            }
        } catch (Exception e) {
            synchronized (this) {
                adopted.remove(job.getJobId());
            }
        }
    }

    private void refresh(Job cached) {
        try {
            OrchestratorService orchestrator = orchestratorOf(cached.getLeaderNode());
            Job fresh = monitor.call(() -> orchestrator.getJob(cached.getJobId()));
            if (fresh != null) {
                synchronized (this) {
                    Job current = jobs.get(cached.getJobId());
                    // Never overwrite a job this directory already marked ORPHANED.
                    if (current == null || current.getStatus().isActive()) {
                        store(fresh);
                    }
                }
            }
        } catch (Exception ignored) {
            // the leader may just have died; the next cycle decides
        }
    }

    private void store(Job job) {
        jobs.put(job.getJobId(), job);
        while (jobs.size() > MAX_JOBS) {
            jobs.remove(jobs.keySet().iterator().next());
        }
    }

    private static boolean isUp(List<ClusterMonitor.NodeStatus> statuses, int nodeId) {
        for (ClusterMonitor.NodeStatus s : statuses) {
            if (s.getId() == nodeId) {
                return s.isUp();
            }
        }
        return false;
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getMessage() == null ? c.toString() : c.getMessage();
    }
}
