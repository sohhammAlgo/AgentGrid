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
 * adoptOrphan(). Orphaned jobs are not resumed (recovery is Exp 8).
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
    private transient Thread worker;
    private transient volatile JobRunner currentRunner;
    private transient volatile Job currentJob;

    public Orchestrator(int nodeId, ClusterConfig config, NodeEventBuffer events) throws RemoteException {
        super(0, TimeoutSocketFactory.INSTANCE, null);
        this.nodeId = nodeId;
        this.cluster = new ClusterView(config);
        this.events = events;
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
        if (worker == null || !worker.isAlive()) {
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
        List<Job> unfinished = new ArrayList<>();
        Job running = currentJob;
        if (running != null) {
            unfinished.add(running);
        }
        queue.drainTo(unfinished);
        for (Job job : unfinished) {
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

    private void runJobs() {
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
                if (status == JobStatus.COMPLETE) {
                    events.record("JOB_COMPLETED", job.getJobId() + " COMPLETE in " + job.makespanMs()
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
        BalancingPolicy p = BalancingPolicy.parse(policy);
        if (p == null) {
            throw new RemoteException("unknown policy: " + policy);
        }
        String mode = consistency == null ? "EVENTUAL" : consistency.trim().toUpperCase(java.util.Locale.ROOT);
        if (!mode.equals("STRONG") && !mode.equals("EVENTUAL")) {
            throw new RemoteException("unknown consistency: " + consistency);
        }
        String jobId = "job-" + nodeId + "-" + incarnation + "-" + sequence.incrementAndGet();
        Job job = new Job(jobId, query, p.name(), mode, nodeId, System.currentTimeMillis(), plan);
        synchronized (jobs) {
            jobs.put(jobId, job);
            trim();
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

    private void trim() {
        while (jobs.size() > MAX_JOBS) {
            String oldest = jobs.keySet().iterator().next();
            jobs.remove(oldest);
        }
    }
}
