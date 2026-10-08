package agentgrid.orchestrator;

import agentgrid.node.ClusterConfig;
import agentgrid.node.LeaderLifecycle;
import agentgrid.node.NodeEventBuffer;
import agentgrid.node.TimeoutSocketFactory;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The job orchestrator of one node. Every node has one, but it is active only while the
 * node is the elected leader: it starts on LeaderLifecycle.onElected and stops on onDemoted.
 * Jobs run one at a time, in submission order, so each job's makespan is its own.
 *
 * Its Lamport clock is the node's clock: job events go into the node's telemetry buffer,
 * which ticks that clock, and a second clock feeding the same buffer would break the
 * strictly increasing order of the node's events.
 *
 * On demotion the running and queued jobs are marked ORPHANED here. A leader that is killed
 * cannot do that; the control plane detects it and hands the job to the new leader through
 * adoptOrphan(). Orphaned jobs are not resumed.
 *
 * With primary-backup replication ON (Exp 8, the default) this orchestrator is the PRIMARY while
 * it is the leader: every job state change goes through the Replicator to the backup's log before
 * the job moves on. A newly elected leader first promotes itself (catches up, starts a new term,
 * replays its log) and resumes the jobs the log shows as unfinished; those end RECOVERED, not
 * ORPHANED. With replication OFF none of this runs and the behaviour is the one described above.
 */
public final class Orchestrator extends UnicastRemoteObject implements OrchestratorService, LeaderLifecycle {

    private static final long serialVersionUID = 1L;
    private static final int MAX_JOBS = 100;

    private final int nodeId;
    private final transient ClusterView cluster;
    private final transient NodeEventBuffer events;
    private final transient JobRunner.EventSink sink;
    private final transient Map<String, Job> jobs = new LinkedHashMap<>();
    private final transient LinkedBlockingQueue<Job> queue = new LinkedBlockingQueue<>();
    private final transient AtomicInteger sequence = new AtomicInteger();
    private final long incarnation = System.currentTimeMillis() % 100000;

    private volatile boolean active;
    /** False while a newly elected primary is still promoting itself (catching up and replaying its log). */
    private transient volatile boolean ready = true;
    private transient Thread worker;
    private transient volatile JobRunner currentRunner;
    private transient volatile Job currentJob;
    private final transient Replicator replicator;
    private final transient java.util.concurrent.ExecutorService control = java.util.concurrent.Executors
            .newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "orchestrator-control");
                t.setDaemon(true);
                return t;
            });

    public Orchestrator(int nodeId, ClusterConfig config, NodeEventBuffer events) throws RemoteException {
        super(0, TimeoutSocketFactory.INSTANCE, null);
        this.nodeId = nodeId;
        this.cluster = new ClusterView(config);
        this.events = events;
        this.replicator = new Replicator(nodeId, config, events, new Replicator.Host() {
            @Override
            public void superseded(String reason) {
                // Called with the replicator's lock held: stop on another thread.
                control.submit(() -> stop("replaced as primary: " + reason));
            }

            @Override
            public boolean isActive() {
                return active;
            }
        });
        this.sink = new JobRunner.EventSink() {
            @Override
            public long record(String type, String details, Map<String, Object> fields) {
                return events.record(type, details, fields).getLamport();
            }

            @Override
            public void receive(long lamport) {
                events.receive(lamport);
            }
        };
    }

    // =========================================================================
    // LeaderLifecycle
    // =========================================================================

    @Override
    public synchronized void onElected(int electedNodeId) {
        active = true;
        boolean startsWorker = worker == null || !worker.isAlive();
        // With replication on, a new worker thread promotes the node before it is ready for jobs.
        ready = !startsWorker || !replicator.isEnabled();
        if (startsWorker) {
            worker = new Thread(this::runJobs, "orchestrator-" + nodeId);
            worker.setDaemon(true);
            worker.start();
        }
        System.out.println("[Node " + nodeId + "] orchestrator started (elected leader)");
    }

    @Override
    public void onDemoted(int demotedNodeId, int newLeaderId) {
        stop("node " + nodeId + " demoted; node " + newLeaderId + " is leader");
    }

    @Override
    public void onLeaderLost(int nodeId, int lostLeaderId) {
        // Not this orchestrator's concern: a follower has nothing to stop.
    }

    private void stop(String reason) {
        Thread w;
        synchronized (this) {
            if (!active) {
                return;
            }
            active = false;
            w = worker;
            worker = null;
        }
        JobRunner runner = currentRunner;
        if (runner != null) {
            runner.abort();
        }
        if (w != null) {
            w.interrupt();
        }
        replicator.demote();
        List<Job> unfinished = new ArrayList<>();
        Job running = currentJob;
        if (running != null) {
            unfinished.add(running);
        }
        queue.drainTo(unfinished);
        for (Job job : unfinished) {
            if (replicator.isEnabled() && job.isRecoverable()) {
                // The backup has this job's state: it is left for the new primary to resume, not orphaned.
                Map<String, Object> f = new LinkedHashMap<>();
                f.put("jobId", job.getJobId());
                f.put("formerLeader", nodeId);
                f.put("completedSubtasks", job.getGraph().completedCount());
                events.record("JOB_HANDED_OVER", job.getJobId() + " left to the new primary: " + reason + " ("
                        + job.getGraph().completedCount() + " completed subtasks are in the replicated log)", f);
                continue;
            }
            if (job.orphan(System.currentTimeMillis())) {
                recordOrphan(job, reason);
            }
        }
        System.out.println("[Node " + nodeId + "] orchestrator stopped: " + reason);
    }

    private void recordOrphan(Job job, String reason) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("jobId", job.getJobId());
        f.put("formerLeader", job.getLeaderNode());
        f.put("completedSubtasks", job.getGraph().completedCount());
        f.put("subtasks", job.getGraph().subtaskCount());
        events.record("JOB_ORPHANED", job.getJobId() + " orphaned: " + reason + " ("
                + job.getGraph().completedCount() + " completed subtasks retained)", f);
    }

    /**
     * Primary-backup failover: this node was just elected. It catches up from the peer with the
     * longest log, starts a new term, rebuilds the jobs from the log and queues the unfinished ones
     * (marked recovered) ahead of anything submitted since. Replication OFF skips all of it.
     */
    private void promoteAndLoad() {
        replicator.setReady(false);
        try {
            Map<String, Job> rebuilt = replicator.promote();
            List<Job> resume = new ArrayList<>();
            // Marked before they become visible: the control plane must never see a replayed job that
            // still names the dead leader as its owner (it would orphan it).
            for (Job j : rebuilt.values()) {
                if (j.getStatus().isActive()) {
                    j.markRecovered(nodeId);
                    resume.add(j);
                }
            }
            synchronized (jobs) {
                for (Job j : rebuilt.values()) {
                    if (j.getStatus().isActive()) {
                        jobs.put(j.getJobId(), j);
                    } else {
                        jobs.putIfAbsent(j.getJobId(), j);
                    }
                }
                trim();
            }
            replicator.maintainNow();   // pick the backup and announce the new term
            boolean backed = replicator.state().getBackupId() != null;
            for (Job j : resume) {
                j.setRecoverable(backed);
                Map<String, Object> f = new LinkedHashMap<>();
                f.put("jobId", j.getJobId());
                f.put("formerLeader", j.getLeaderNode());
                f.put("completedSubtasks", j.getGraph().completedCount());
                f.put("term", replicator.state().getTerm());
                events.record("JOB_RECOVERING", j.getJobId() + " recovered from the replicated log by node " + nodeId
                        + " (term " + replicator.state().getTerm() + "): " + j.getGraph().completedCount()
                        + " subtasks already complete, the rest are re-dispatched", f);
                queue.add(j);
            }
            if (!resume.isEmpty()) {
                replicator.noteRecovered(resume.size());
            }
        } catch (RuntimeException e) {
            System.err.println("[Node " + nodeId + "] promotion failed: " + e);
        } finally {
            replicator.setReady(true);
            ready = true;
        }
    }

    private void runJobs() {
        if (replicator.isEnabled()) {
            promoteAndLoad();
        }
        ready = true;
        while (active) {
            Job job;
            try {
                job = queue.take();
            } catch (InterruptedException e) {
                return;
            }
            if (!active) {
                if (job.orphan(System.currentTimeMillis())) {
                    recordOrphan(job, "orchestrator stopped before the job started");
                }
                return;
            }
            JobRunner runner = new JobRunner(cluster, sink, WorkerNode.LoadView.NORMALIZED_IN_FLIGHT, false);
            runner.setReplication(replicator);
            currentRunner = runner;
            currentJob = job;
            try {
                JobStatus status = runner.run(job);
                Map<String, Object> f = new LinkedHashMap<>();
                f.put("jobId", job.getJobId());
                f.put("status", status.name());
                f.put("makespanMs", job.makespanMs());
                f.put("policy", job.getPolicy());
                Map<String, Object> retrieval = job.retrievalMap();
                f.put("retrieval", retrieval.get("mode"));
                f.put("docsTotal", retrieval.get("docsTotal"));
                f.put("docsTouched", retrieval.get("docsTouched"));
                f.put("retrieveSubtasks", retrieval.get("retrieveSubtasks"));
                f.put("retrievalMs", retrieval.get("retrievalMs"));
                if (status == JobStatus.COMPLETE || status == JobStatus.RECOVERED) {
                    events.record("JOB_COMPLETED", job.getJobId() + " " + status.name() + " in " + job.makespanMs()
                            + " ms (" + job.getPolicy() + ") [" + job.retrievalText() + "]", f);
                } else if (status == JobStatus.FAILED) {
                    events.record("JOB_FAILED", job.getJobId() + " FAILED: " + job.getError(), f);
                }
            } finally {
                runner.shutdown();
                currentRunner = null;
                currentJob = null;
            }
        }
    }

    // =========================================================================
    // OrchestratorService
    // =========================================================================

    @Override
    public String submit(String query, String policy, String consistency) throws RemoteException {
        return submit(query, policy, consistency, RetrievalPlan.scan());
    }

    @Override
    public String submit(String query, String policy, String consistency, RetrievalPlan retrieval) throws RemoteException {
        RetrievalPlan plan = retrieval == null ? RetrievalPlan.scan() : retrieval;
        if (!active) {
            throw new RemoteException("node " + nodeId + " is not the leader; its orchestrator is inactive");
        }
        // A newly elected primary is catching up and replaying its log: wait briefly for it.
        long until = System.currentTimeMillis() + 5000;
        while (!ready && active && System.currentTimeMillis() < until) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (!ready) {
            throw new RemoteException("node " + nodeId + " is still promoting itself (catching up from the replicated log)");
        }
        BalancingPolicy p = BalancingPolicy.parse(policy);
        if (p == null) {
            throw new RemoteException("unknown policy: " + policy);
        }
        String mode = consistency == null ? "EVENTUAL" : consistency.trim().toUpperCase(java.util.Locale.ROOT);
        if (!mode.equals("STRONG") && !mode.equals("EVENTUAL")) {
            throw new RemoteException("unknown consistency: " + consistency);
        }
        String jobId = "job-" + nodeId + "-" + incarnation + "-" + sequence.incrementAndGet();
        long submittedMs = System.currentTimeMillis();
        Job job = new Job(jobId, query, p.name(), mode, nodeId, submittedMs, plan);
        synchronized (jobs) {
            jobs.put(jobId, job);
            trim();
        }
        // Replicate the submission before the job is queued: the backup must know the job exists.
        if (replicator.isEnabled()) {
            Map<String, Object> ev = new LinkedHashMap<>();
            ev.put("query", query);
            ev.put("policy", p.name());
            ev.put("consistency", mode);
            ev.put("leaderNode", nodeId);
            ev.put("submittedMs", submittedMs);
            ev.put("retrieval", plan.getMode());
            if (plan.isIndex()) {
                ev.put("indexDocs", String.join(",", plan.getDocIds()));
                ev.put("indexBuiltAt", plan.getIndexBuiltAt());
                ev.put("queryTerms", String.join(",", plan.getQueryTerms()));
            }
            replicator.log(ReplEvent.Type.JOB_SUBMITTED, job, ev);
        }
        String shown = query.length() <= 80 ? query : query.substring(0, 80) + "...";
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("jobId", jobId);
        f.put("policy", p.name());
        f.put("consistency", mode);
        f.put("query", shown);
        f.put("retrieval", plan.getMode());
        if (plan.isIndex()) {
            f.put("indexDocs", plan.getDocIds().size());
        }
        events.record("JOB_SUBMITTED", jobId + " submitted (" + p.name() + ", " + mode + "): " + shown
                + " [" + job.retrievalText() + "]", f);
        queue.add(job);
        return jobId;
    }

    @Override
    public Job getJob(String jobId) {
        Job job;
        synchronized (jobs) {
            job = jobs.get(jobId);
        }
        return job == null ? null : job.copy();
    }

    @Override
    public List<Job> listJobs() {
        List<Job> list = new ArrayList<>();
        synchronized (jobs) {
            for (Job job : jobs.values()) {
                list.add(0, job.copy());
            }
        }
        return list;
    }

    @Override
    public Job adoptOrphan(Job job) {
        job.orphan(System.currentTimeMillis());
        job.adoptedBy(nodeId);
        synchronized (jobs) {
            jobs.put(job.getJobId(), job);
            trim();
        }
        recordOrphan(job, "leader node " + job.getLeaderNode() + " died; recorded by new leader node " + nodeId);
        return job.copy();
    }

    @Override
    public boolean isActive() {
        return active;
    }

    @Override
    public ReplAck replicate(ReplEvent event) {
        return replicator.onReplicate(event);
    }

    @Override
    public ReplAck installSnapshot(long term, List<ReplEvent> log) {
        return replicator.onSnapshot(term, log);
    }

    @Override
    public List<ReplEvent> fetchLog() {
        return replicator.fetchLog();
    }

    @Override
    public ReplState replicationState() {
        return replicator.state();
    }

    @Override
    public void setPrimaryBackup(boolean enabled) {
        replicator.setEnabled(enabled);
    }

    private void trim() {
        while (jobs.size() > MAX_JOBS) {
            String oldest = jobs.keySet().iterator().next();
            jobs.remove(oldest);
        }
    }
}
