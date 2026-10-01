package agentgrid.orchestrator;

import agentgrid.balancer.LoadBalancer;
import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs one job through RETRIEVE (fan out over corpus chunks) -> RANK -> SUMMARIZE (fan out
 * over the top-ranked documents) -> SYNTHESIZE. Each stage starts only after every subtask of
 * the previous stage has completed; its payloads are built from those outputs.
 *
 * Lamport chain (fix H): every dispatch is an event whose Lamport time travels in the
 * Subtask; the worker returns a Result stamped after its own receive and work; the runner
 * applies the receive rule to that time before recording the completion. The next stage's
 * dispatches therefore carry times greater than every result they depend on.
 */
public final class JobRunner {

    /** Where the runner records its events and applies the Lamport receive rule. */
    public interface EventSink {
        /** Records a local event and returns its Lamport time. */
        long record(String type, String details, Map<String, Object> fields);

        /** Lamport receive rule for a Result's timestamp. */
        void receive(long lamport);
    }

    public static final int RETRIEVE_FANOUT = 20;
    public static final int SUMMARIZE_FANOUT = 20;
    public static final int MAX_ATTEMPTS = 3;

    private final ClusterView cluster;
    private final EventSink sink;
    private final WorkerNode.LoadView leastLoadedView;
    private final boolean loopIndexTimestamps;
    private final ExecutorService pool;
    private volatile boolean aborted;
    private volatile Thread runningThread;

    /**
     * @param leastLoadedView     load view for LEAST_LOADED; the orchestrator passes NORMALIZED_IN_FLIGHT
     * @param loopIndexTimestamps baseline mode for fix H only: carry the subtask's index as its
     *                            timestamp and ignore Result times, as the submitted demo did
     */
    public JobRunner(ClusterView cluster, EventSink sink, WorkerNode.LoadView leastLoadedView,
                     boolean loopIndexTimestamps) {
        this.cluster = cluster;
        this.sink = sink;
        this.leastLoadedView = leastLoadedView;
        this.loopIndexTimestamps = loopIndexTimestamps;
        AtomicInteger n = new AtomicInteger();
        this.pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "job-dispatch-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    /** Stops the running job: no more dispatches, late results are ignored. */
    public void abort() {
        aborted = true;
        Thread t = runningThread;
        if (t != null) {
            t.interrupt();
        }
    }

    public void shutdown() {
        pool.shutdownNow();
    }

    /** Runs the job on the calling thread and returns its final status. */
    public JobStatus run(Job job) {
        runningThread = Thread.currentThread();
        BalancingPolicy policy = BalancingPolicy.parse(job.getPolicy());
        job.started(System.currentTimeMillis());
        try {
            if (policy == null) {
                return fail(job, "unknown policy " + job.getPolicy());
            }
            List<WorkerNode> initial = cluster.liveWorkers();
            if (initial.isEmpty()) {
                return fail(job, "no live worker node");
            }
            for (WorkerNode w : initial) {
                try {
                    w.stub().takePeakQueueDepth();
                } catch (RemoteException ignored) {
                    // the node's peak simply is not reset
                }
            }
            String query = job.getQuery();

            // SCAN dispatches every chunk; INDEX only the chunks holding a document the
            // MapReduce index lists for the query, and each such subtask reads only those.
            RetrievalPlan plan = job.getRetrieval();
            Set<String> indexed = plan.docIdSet();
            Corpus corpus = Corpus.load();
            int docsTouched = 0;
            List<String[]> inputs = new ArrayList<>();
            for (int i = 0; i < RETRIEVE_FANOUT; i++) {
                Payload p = new Payload().put("query", query).put("chunk", i).put("chunks", RETRIEVE_FANOUT);
                String label = "chunk " + (i + 1) + "/" + RETRIEVE_FANOUT;
                List<Corpus.Doc> chunkDocs = corpus.chunk(i, RETRIEVE_FANOUT);
                if (plan.isIndex()) {
                    List<String> docs = new ArrayList<>();
                    for (Corpus.Doc d : chunkDocs) {
                        if (indexed.contains(d.getId())) {
                            docs.add(d.getId());
                        }
                    }
                    if (docs.isEmpty()) {
                        continue;
                    }
                    p.put("docs", String.join(",", docs));
                    label += " (index: " + String.join(", ", docs) + ")";
                    docsTouched += docs.size();
                } else {
                    docsTouched += chunkDocs.size();
                }
                inputs.add(new String[] {label, p.encode()});
            }
            long retrieveStart = System.currentTimeMillis();
            boolean retrieved = runStage(job, Subtask.Type.RETRIEVE, inputs, policy);
            job.retrievalMetrics(corpus.size(), docsTouched, inputs.size(), System.currentTimeMillis() - retrieveStart);
            if (!retrieved) {
                return fail(job, "RETRIEVE failed");
            }
            Set<String> candidates = new LinkedHashSet<>();
            for (JobStage.SubtaskState s : job.getGraph().stage(Subtask.Type.RETRIEVE).getSubtasks()) {
                candidates.addAll(RetrieveStrategy.parseDocIds(s.fullOutput()));
            }

            inputs = new ArrayList<>();
            inputs.add(new String[] {candidates.size() + " candidate documents",
                    new Payload().put("query", query).put("candidates", String.join(",", candidates))
                            .put("topK", SUMMARIZE_FANOUT).encode()});
            if (!runStage(job, Subtask.Type.RANK, inputs, policy)) {
                return fail(job, "RANK failed");
            }
            List<String[]> ranked = RankStrategy.parse(
                    job.getGraph().stage(Subtask.Type.RANK).getSubtasks().get(0).fullOutput());

            inputs = new ArrayList<>();
            for (int i = 0; i < ranked.size(); i++) {
                Payload p = new Payload().put("query", query).put("doc", ranked.get(i)[0]).put("rank", i + 1)
                        .put("sentences", 2);
                blackboardKey(p, job, "finding/summarize-" + (i + 1));
                inputs.add(new String[] {ranked.get(i)[0] + " (rank " + (i + 1) + ", tf-idf " + ranked.get(i)[1] + ")",
                        p.encode()});
            }
            if (!runStage(job, Subtask.Type.SUMMARIZE, inputs, policy)) {
                return fail(job, "SUMMARIZE failed");
            }

            Payload synth = new Payload().put("query", query).put("answerSentences", 4);
            List<JobStage.SubtaskState> summaries = job.getGraph().stage(Subtask.Type.SUMMARIZE).getSubtasks();
            for (int i = 0; i < summaries.size(); i++) {
                synth.put("p" + i, summaries.get(i).fullOutput());
            }
            blackboardKey(synth, job, "answer");
            inputs = new ArrayList<>();
            inputs.add(new String[] {summaries.size() + " partial summaries", synth.encode()});
            if (!runStage(job, Subtask.Type.SYNTHESIZE, inputs, policy)) {
                return fail(job, "SYNTHESIZE failed");
            }
            String answer = job.getGraph().stage(Subtask.Type.SYNTHESIZE).getSubtasks().get(0).fullOutput();
            recordBlackboardSummary(job);

            Map<Integer, Integer> peaks = new TreeMap<>();
            for (WorkerNode w : initial) {
                try {
                    peaks.put(w.getNodeId(), w.stub().takePeakQueueDepth());
                } catch (RemoteException ignored) {
                    // a node that died during the job has no peak to report
                }
            }
            job.peakQueueDepth(peaks);
            job.finished(JobStatus.COMPLETE, answer, null, System.currentTimeMillis());
            return job.getStatus();
        } catch (InterruptedException e) {
            return job.getStatus();
        } catch (RemoteException e) {
            return fail(job, "routing failed: " + e.getMessage());
        } finally {
            runningThread = null;
        }
    }

    /**
     * Asks the worker to post its output to the blackboard as job/<jobId>/<suffix> under the
     * job's consistency mode (jobs without a mode post nothing).
     */
    private static void blackboardKey(Payload p, Job job, String suffix) {
        if (job.getConsistency() != null) {
            p.put("bbKey", "job/" + job.getJobId() + "/" + suffix).put("consistency", job.getConsistency());
        }
    }

    /** One BLACKBOARD_JOB_FINDINGS event per job: how the workers' writes went, never one per write. */
    private void recordBlackboardSummary(Job job) {
        Map<String, Object> summary = job.blackboardSummary();
        if (summary == null || ((Integer) summary.get("posted")) == 0) {
            return;
        }
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("jobId", job.getJobId());
        f.putAll(summary);
        f.put("byStatus", String.valueOf(summary.get("byStatus")));
        sink.record("BLACKBOARD_JOB_FINDINGS", job.getJobId() + " blackboard (" + summary.get("consistency") + "): "
                + summary.get("stored") + "/" + summary.get("posted") + " findings and answer stored "
                + summary.get("byStatus") + ", median write " + summary.get("medianLatencyMs") + " ms", f);
    }

    private JobStatus fail(Job job, String reason) {
        recordBlackboardSummary(job);
        job.finished(JobStatus.FAILED, null, reason, System.currentTimeMillis());
        return job.getStatus();
    }

    private boolean runStage(Job job, Subtask.Type type, List<String[]> inputs, BalancingPolicy policy)
            throws InterruptedException, RemoteException {
        if (aborted) {
            throw new InterruptedException("aborted");
        }
        JobStage stage = job.getGraph().stage(type);
        List<JobStage.SubtaskState> states = new ArrayList<>();
        long start = System.currentTimeMillis();
        synchronized (job) {
            stage.started(start);
            for (int i = 0; i < inputs.size(); i++) {
                states.add(stage.add(type.name().toLowerCase(Locale.ROOT) + "-" + (i + 1), inputs.get(i)[0]));
            }
        }
        Map<String, Object> f = fields(job, type);
        f.put("subtasks", inputs.size());
        sink.record("STAGE_STARTED", job.getJobId() + " " + type + " started with " + inputs.size() + " subtasks", f);

        if (!inputs.isEmpty()) {
            StageDispatch dispatch = new StageDispatch(job, type, cluster.liveWorkers(), policy, inputs.size());
            for (int i = 0; i < inputs.size(); i++) {
                dispatch.dispatch(states.get(i), inputs.get(i)[1], i, 1);
            }
            dispatch.await();
        }

        boolean ok;
        long completed;
        long end = System.currentTimeMillis();
        synchronized (job) {
            completed = stage.completedCount();
            ok = completed == states.size();
            stage.finished(ok, end);
        }
        f = fields(job, type);
        f.put("subtasks", inputs.size());
        f.put("completed", completed);
        f.put("durationMs", end - start);
        sink.record("STAGE_COMPLETED", job.getJobId() + " " + type + " " + (ok ? "completed" : "FAILED")
                + " (" + completed + "/" + inputs.size() + ") in " + (end - start) + " ms", f);
        return ok;
    }

    private static Integer intOrNull(String s) {
        Long v = longOrNull(s);
        return v == null ? null : v.intValue();
    }

    private static Long longOrNull(String s) {
        try {
            return s == null ? null : Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Map<String, Object> fields(Job job, Subtask.Type type) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("jobId", job.getJobId());
        f.put("stage", type.name());
        return f;
    }

    /** Routing and dispatch of one stage's subtasks. */
    private final class StageDispatch {
        private final Job job;
        private final Subtask.Type type;
        private final List<WorkerNode> workers;
        private final BalancingPolicy policy;
        private final CountDownLatch done;
        private final Set<Integer> down = ConcurrentHashMap.newKeySet();
        private final Object lock = new Object();
        private LoadBalancer balancer;
        private List<WorkerNode> balancerWorkers;

        StageDispatch(Job job, Subtask.Type type, List<WorkerNode> workers, BalancingPolicy policy, int count) {
            this.job = job;
            this.type = type;
            this.workers = workers;
            this.policy = policy;
            this.done = new CountDownLatch(count);
        }

        /** Picks a live worker with the policy's submitted balancer; null if none is left. */
        private WorkerNode pick() throws RemoteException {
            for (int tries = 0; tries <= workers.size(); tries++) {
                List<WorkerNode> alive = new ArrayList<>();
                for (WorkerNode w : workers) {
                    if (!down.contains(w.getNodeId())) {
                        alive.add(w);
                    }
                }
                if (alive.isEmpty()) {
                    return null;
                }
                if (balancer == null || !alive.equals(balancerWorkers)) {
                    balancer = policy.create(alive, leastLoadedView);
                    balancerWorkers = alive;
                }
                try {
                    return (WorkerNode) balancer.select(BalancingPolicy.asServices(alive));
                } catch (RemoteException e) {
                    // LEAST_LOADED queried a node that just died: find it and retry without it.
                    for (WorkerNode w : alive) {
                        try {
                            w.ping();
                        } catch (RemoteException dead) {
                            down.add(w.getNodeId());
                            cluster.forget(w.getNodeId());
                        }
                    }
                }
            }
            return null;
        }

        void dispatch(JobStage.SubtaskState state, String payload, int index, int attempt) throws RemoteException {
            WorkerNode worker;
            long carried;
            synchronized (lock) {
                if (aborted) {
                    done.countDown();
                    return;
                }
                worker = pick();
                if (worker == null) {
                    synchronized (job) {
                        state.failed("no live worker node");
                    }
                    done.countDown();
                    return;
                }
                worker.dispatchStarted();
                Map<String, Object> f = fields(job, type);
                f.put("subtaskId", state.getSubtaskId());
                f.put("node", worker.getNodeId());
                f.put("attempt", attempt);
                long lamport = sink.record("SUBTASK_DISPATCHED", job.getJobId() + " " + state.getSubtaskId()
                        + " -> node " + worker.getNodeId() + (attempt > 1 ? " (attempt " + attempt + ")" : ""), f);
                carried = loopIndexTimestamps ? index : lamport;
                synchronized (job) {
                    state.dispatched(worker.getNodeId(), carried, System.currentTimeMillis());
                }
            }
            WorkerNode w = worker;
            long sent = carried;
            boolean postedFinding = Payload.decode(payload).get("bbKey") != null;
            pool.submit(() -> {
                try {
                    Result r = w.execute(new Subtask(job.getJobId(), state.getSubtaskId(), type, payload, sent));
                    if (aborted) {
                        done.countDown();
                        return;
                    }
                    if (!loopIndexTimestamps) {
                        sink.receive(r.getLamportTimestamp());
                    }
                    Map<String, Object> f = fields(job, type);
                    f.put("subtaskId", state.getSubtaskId());
                    f.put("node", w.getNodeId());
                    f.put("dispatchLamport", sent);
                    f.put("resultLamport", r.getLamportTimestamp());
                    f.put("ok", r.isSuccess());
                    long complete = sink.record("SUBTASK_COMPLETED", job.getJobId() + " " + state.getSubtaskId()
                            + " <- node " + w.getNodeId() + (r.isSuccess() ? "" : " FAILED"), f);
                    // A worker that posted to the blackboard wraps its output: result (unchanged)
                    // plus the write's outcome. The job is built from result exactly as before.
                    String output = r.getOutput();
                    Payload envelope = postedFinding ? Payload.decode(output) : null;
                    boolean wrapped = envelope != null && envelope.get("bbStatus") != null && envelope.get("result") != null;
                    if (wrapped) {
                        output = envelope.get("result");
                    }
                    synchronized (job) {
                        state.completed(r.getLamportTimestamp(), complete, System.currentTimeMillis(),
                                output, r.isSuccess());
                        if (wrapped) {
                            state.blackboard(envelope.get("bbKey"), envelope.get("bbStatus"),
                                    longOrNull(envelope.get("bbLatencyMs")), longOrNull(envelope.get("bbStamp")),
                                    intOrNull(envelope.get("bbWriter")),
                                    envelope.get("bbMessage"));
                        } else if (postedFinding && r.isSuccess()) {
                            state.blackboard(Payload.decode(payload).get("bbKey"), "FAILED", null, null, null,
                                    "the worker returned no blackboard outcome");
                        }
                    }
                    done.countDown();
                } catch (RemoteException e) {
                    down.add(w.getNodeId());
                    cluster.forget(w.getNodeId());
                    if (!aborted && attempt < MAX_ATTEMPTS) {
                        try {
                            dispatch(state, payload, index, attempt + 1);
                        } catch (RemoteException again) {
                            synchronized (job) {
                                state.failed("dispatch failed: " + again.getMessage());
                            }
                            done.countDown();
                        }
                    } else {
                        synchronized (job) {
                            state.failed("node " + w.getNodeId() + " failed: " + e.getMessage());
                        }
                        done.countDown();
                    }
                } finally {
                    w.dispatchEnded();
                }
            });
        }

        void await() throws InterruptedException {
            while (!done.await(100, TimeUnit.MILLISECONDS)) {
                if (aborted) {
                    throw new InterruptedException("aborted");
                }
            }
        }
    }
}
