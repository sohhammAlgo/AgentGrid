package agentgrid.orchestrator;

import agentgrid.node.ClusterConfig;
import agentgrid.node.MemberSpec;
import agentgrid.node.NodeEventBuffer;
import agentgrid.node.TimeoutSocketFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Primary-backup replication of the orchestrator's job state (Exp 8), one per node.
 *
 * The elected leader's orchestrator is the PRIMARY. Every job state change is appended to its log
 * as {seq, term, jobId, type, payload} and sent over RMI to the BACKUP, the live member with the
 * next highest id below the primary's; the primary waits for the ack before the job moves on. With
 * no live backup it carries on unreplicated and reports it ("no backup"); it never blocks.
 *
 * Terms fence stale primaries. Every election result starts a term: a new primary takes the
 * highest term any peer reports, plus one. An event or snapshot whose term is below the receiver's
 * is refused (STALE_TERM) and its sender steps down; workers refuse dispatches the same way.
 *
 * Failover: a node elected leader first catches up (it takes the longest log among its peers, by
 * last term then last seq, so a node that restarted with an empty log copies the current primary's),
 * then starts its new term, replays the log into jobs and hands the unfinished ones back to the
 * orchestrator to resume. Backups only append; they do not interpret the log until promoted.
 *
 * The log is in memory: it survives the death of the primary or the backup, not of both.
 */
public final class Replicator implements JobRunner.Replication {

    /** What the orchestrator provides. */
    public interface Host {
        /** This node's primary role was superseded: stop running jobs; they belong to the new primary. */
        void superseded(String reason);

        /** True while this node's orchestrator is the active (elected) one. */
        boolean isActive();
    }

    private static final long CALL_TIMEOUT_MS = 1500;
    private static final long SNAPSHOT_TIMEOUT_MS = 6000;
    private static final long MAINTAIN_MS = 400;
    private static final long BACKOFF_MS = 2000;

    private final int nodeId;
    private final ClusterConfig config;
    private final NodeEventBuffer events;
    private final Host host;

    private final ReentrantLock lock = new ReentrantLock();
    private final ArrayList<ReplEvent> log = new ArrayList<>();
    /** Jobs whose JOB_SUBMITTED is in the log and that have not finished: only these are replicated. */
    private final Set<String> loggedJobs = new HashSet<>();
    private final Map<Integer, OrchestratorService> stubs = new ConcurrentHashMap<>();
    private final ExecutorService rmi = Executors.newCachedThreadPool(daemon("repl-rmi-"));
    private final ScheduledExecutorService maintenance = Executors.newSingleThreadScheduledExecutor(daemon("repl-maint-"));

    private volatile boolean enabled;
    // Guarded by lock; mirrored in view for lock-free reads.
    private long term;
    private long lastSeq;
    private long lastLogTerm;
    /** When the last search for a backup found none: events then go unreplicated without re-probing every time. */
    private long lastChooseFailMs;
    private boolean primary;
    private boolean ready = true;
    private Integer backupId;
    private long backupLastSeq;
    private boolean noBackup;
    private final AtomicLong staleRejected = new AtomicLong();
    private final AtomicLong recoveredJobs = new AtomicLong();
    private volatile long activeTerm;   // term while this node is an enabled primary, else 0
    private volatile ReplState view;

    public Replicator(int nodeId, ClusterConfig config, NodeEventBuffer events, Host host) {
        this.nodeId = nodeId;
        this.config = config;
        this.events = events;
        this.host = host;
        this.enabled = Boolean.parseBoolean(System.getProperty("agentgrid.primaryBackup", "true"));
        publish();
        maintenance.scheduleWithFixedDelay(this::maintainSafely, MAINTAIN_MS, MAINTAIN_MS, TimeUnit.MILLISECONDS);
    }

    // =========================================================================
    // State
    // =========================================================================

    public boolean isEnabled() {
        return enabled;
    }

    public ReplState state() {
        return view;
    }

    public void shutdown() {
        maintenance.shutdownNow();
        rmi.shutdownNow();
    }

    public void noteRecovered(int jobs) {
        recoveredJobs.addAndGet(jobs);
        publishSafely();
    }

    public void setReady(boolean ready) {
        lock.lock();
        try {
            this.ready = ready;
            publishLocked();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Turns replication on or off. Off clears the log (a log that skips the jobs run while off would
     * resurrect finished jobs when it is replayed) and stops fencing; on, an active leader becomes a
     * primary of the current term at once and picks a backup.
     */
    public void setEnabled(boolean on) {
        lock.lock();
        try {
            if (enabled == on) {
                return;
            }
            enabled = on;
            log.clear();
            loggedJobs.clear();
            lastSeqReset();
            backupId = null;
            backupLastSeq = 0;
            noBackup = false;
            if (on && host.isActive()) {
                primary = true;
                if (term == 0) {
                    term = 1;
                }
            } else if (!on) {
                primary = false;
            }
            record("REPLICATION_TOGGLED", "primary-backup replication " + (on ? "ON" : "OFF") + " on node " + nodeId
                    + (on ? "" : " (log cleared, jobs are orphaned again if their leader dies)"),
                    fields("enabled", on, "term", term));
            publishLocked();
        } finally {
            lock.unlock();
        }
        if (on) {
            maintainSafely();
        }
    }

    private void lastSeqReset() {
        lastSeq = 0;
        lastLogTerm = 0;
    }

    private void publishSafely() {
        lock.lock();
        try {
            publishLocked();
        } finally {
            lock.unlock();
        }
    }

    private void publishLocked() {
        activeTerm = enabled && primary ? term : 0L;
        view = new ReplState(nodeId, enabled, primary, ready, term, lastSeq, lastLogTerm, backupId, backupLastSeq,
                noBackup, staleRejected.get(), recoveredJobs.get(), log.size());
    }

    private void publish() {
        publishSafely();
    }

    // =========================================================================
    // Primary: log and replicate (JobRunner.Replication)
    // =========================================================================

    @Override
    public long term() {
        return activeTerm;
    }

    @Override
    public void log(ReplEvent.Type type, Job job, Map<String, Object> payload) {
        if (!enabled) {
            return;
        }
        lock.lock();
        try {
            if (!enabled || !primary) {
                return;
            }
            if (type != ReplEvent.Type.JOB_SUBMITTED && !loggedJobs.contains(job.getJobId())) {
                return;   // the job started while replication was off: it is not replicated
            }
            ReplEvent ev = new ReplEvent(lastSeq + 1, term, job.getJobId(), type, payload, System.currentTimeMillis());
            log.add(ev);
            lastSeq = ev.getSeq();
            lastLogTerm = ev.getTerm();
            if (type == ReplEvent.Type.JOB_SUBMITTED) {
                loggedJobs.add(job.getJobId());
            } else if (type == ReplEvent.Type.JOB_DONE || type == ReplEvent.Type.JOB_FAILED) {
                loggedJobs.remove(job.getJobId());
            }
            boolean replicated = replicateLocked(ev);
            job.setRecoverable(replicated);
            publishLocked();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fenced(long currentTerm) {
        lock.lock();
        try {
            supersededLocked(currentTerm, "a worker refused this primary's dispatch: term " + term
                    + " is below " + currentTerm);
        } finally {
            lock.unlock();
        }
    }

    /** True if the backup holds event ev afterwards. Called with the lock held. */
    private boolean replicateLocked(ReplEvent ev) {
        for (int attempt = 0; attempt < 3; attempt++) {
            if (!primary) {
                return false;
            }
            if (backupId == null) {
                // The maintenance thread keeps looking for a backup outside the lock; until it finds
                // one, events go unreplicated rather than re-probing dead nodes for every event.
                if (System.currentTimeMillis() - lastChooseFailMs < BACKOFF_MS) {
                    markNoBackupLocked();
                    return false;
                }
                // chooseBackupLocked ships the whole log, which already contains ev
                if (chooseBackupLocked()) {
                    return true;
                }
                markNoBackupLocked();
                return false;
            }
            int target = backupId;
            try {
                OrchestratorService stub = stub(target);
                ReplAck ack = call(() -> stub.replicate(ev), CALL_TIMEOUT_MS);
                switch (ack.getStatus()) {
                    case OK:
                        backupLastSeq = ev.getSeq();
                        noBackup = false;
                        return true;
                    case STALE_TERM:
                        supersededLocked(ack.getTerm(), "backup node " + target + " is at term " + ack.getTerm());
                        return false;
                    case NEED_SYNC:
                    default:
                        if (syncBackupLocked(target)) {
                            return true;
                        }
                        dropBackupLocked("could not be synced");
                }
            } catch (Exception e) {
                dropBackupLocked("did not answer: " + root(e));
            }
        }
        markNoBackupLocked();
        return false;
    }

    private void markNoBackupLocked() {
        if (!noBackup) {
            noBackup = true;
            record("REPLICATION_NO_BACKUP", "primary node " + nodeId + " has no live backup: jobs run unreplicated until one is chosen",
                    fields("term", term));
        }
    }

    private void dropBackupLocked(String why) {
        if (backupId != null) {
            record("REPLICATION_BACKUP_LOST", "backup node " + backupId + " of primary " + nodeId + " " + why,
                    fields("backup", backupId, "term", term));
            stubs.remove(backupId);
        }
        backupId = null;
        backupLastSeq = 0;
    }

    /** Candidates in preference order: ids below this node, highest first, then ids above it. */
    private List<Integer> candidates() {
        List<Integer> ids = config.membership().ids();
        List<Integer> out = new ArrayList<>();
        for (int i = ids.size() - 1; i >= 0; i--) {
            if (ids.get(i) < nodeId) {
                out.add(ids.get(i));
            }
        }
        for (int id : ids) {
            if (id > nodeId) {
                out.add(id);
            }
        }
        return out;
    }

    /** Picks the first reachable candidate and ships it the whole log. Called with the lock held. */
    private boolean chooseBackupLocked() {
        for (int id : candidates()) {
            try {
                OrchestratorService stub = stub(id);
                call(stub::replicationState, CALL_TIMEOUT_MS);   // reachable?
            } catch (Exception e) {
                stubs.remove(id);
                continue;
            }
            if (syncBackupLocked(id)) {
                return true;
            }
            if (!primary) {
                return false;
            }
        }
        lastChooseFailMs = System.currentTimeMillis();
        return false;
    }

    /** Installs the primary's whole log on node id and makes it the backup. Called with the lock held. */
    private boolean syncBackupLocked(int id) {
        try {
            OrchestratorService stub = stub(id);
            List<ReplEvent> copy = new ArrayList<>(log);
            long sentTerm = term;
            ReplAck ack = call(() -> stub.installSnapshot(sentTerm, copy), SNAPSHOT_TIMEOUT_MS);
            if (ack.getStatus() == ReplAck.Status.STALE_TERM) {
                supersededLocked(ack.getTerm(), "node " + id + " is at term " + ack.getTerm());
                return false;
            }
            if (!ack.isOk()) {
                return false;
            }
            boolean changed = backupId == null || backupId != id;
            backupId = id;
            backupLastSeq = lastSeq;
            noBackup = false;
            if (changed) {
                record("REPLICATION_BACKUP_SET", "node " + id + " is now the backup of primary " + nodeId + " (term " + term
                        + ", log synced to seq " + lastSeq + ")", fields("backup", id, "term", term, "lastSeq", lastSeq));
            }
            return true;
        } catch (Exception e) {
            stubs.remove(id);
            return false;
        }
    }

    private void supersededLocked(long theirTerm, String why) {
        if (theirTerm > term) {
            term = theirTerm;
        }
        boolean was = primary;
        primary = false;
        backupId = null;
        backupLastSeq = 0;
        publishLocked();
        if (was) {
            record("REPLICATION_STEPPED_DOWN", "node " + nodeId + " steps down as primary: " + why, fields("term", term));
            host.superseded(why);
        }
    }

    // =========================================================================
    // Primary: keep the backup right (membership changes, a dead or returning backup)
    // =========================================================================

    private void maintainSafely() {
        try {
            maintain();
        } catch (RuntimeException e) {
            System.err.println("[Node " + nodeId + "] replication maintenance error: " + e);
        }
    }

    /** Re-checks the backup now (the orchestrator calls it right after promotion). */
    public void maintainNow() {
        maintainSafely();
    }

    private void maintain() {
        if (!enabled) {
            return;
        }
        // Phase 1, without the lock: probe the current backup and every candidate above it.
        Integer current;
        long myTerm;
        long ackedBefore;
        lock.lock();
        try {
            if (!primary) {
                return;
            }
            current = backupId;
            myTerm = term;
            ackedBefore = backupLastSeq;
        } finally {
            lock.unlock();
        }
        List<Integer> order = candidates();
        boolean currentGone = current != null && !order.contains(current);
        ReplState currentState = null;
        if (current != null && !currentGone) {
            try {
                currentState = call(stub(current)::replicationState, CALL_TIMEOUT_MS);
            } catch (Exception e) {
                stubs.remove(current);
                currentGone = true;
            }
        }
        // The first live candidate that ranks above the current backup (or any live one if there is none).
        Integer better = null;
        for (int id : order) {
            if (current != null && !currentGone && id == current) {
                break;
            }
            try {
                call(stub(id)::replicationState, CALL_TIMEOUT_MS);
                better = id;
                break;
            } catch (Exception e) {
                stubs.remove(id);
            }
        }
        // Phase 2, with the lock: act on what was found.
        lock.lock();
        try {
            if (!primary || !enabled || term != myTerm) {
                return;
            }
            if (backupId != null && !backupId.equals(current)) {
                return;   // changed meanwhile (an event dropped or replaced it)
            }
            if (currentGone) {
                dropBackupLocked(order.contains(current) ? "is unreachable" : "is no longer a member");
            } else if (current != null && currentState != null) {
                // The backup reports less than it had already acked before this probe (or an older
                // term): it lost its log (it restarted). Events sent meanwhile do not count against it.
                if (currentState.getLastSeq() < ackedBefore || currentState.getTerm() < myTerm) {
                    syncBackupLocked(current);
                }
            }
            if (better != null) {
                syncBackupLocked(better);   // a higher-preference candidate is back (or joined), or the first backup
            }
            if (backupId == null) {
                lastChooseFailMs = System.currentTimeMillis();
                markNoBackupLocked();
            } else {
                lastChooseFailMs = 0;
            }
            publishLocked();
        } finally {
            lock.unlock();
        }
    }

    // =========================================================================
    // Receiver (backup or any member)
    // =========================================================================

    public ReplAck onReplicate(ReplEvent ev) {
        lock.lock();
        try {
            if (ev.getTerm() < term) {
                return stale("event " + ev, ev.getTerm());
            }
            if (primary && ev.getTerm() > term) {
                supersededLocked(ev.getTerm(), "an event of term " + ev.getTerm() + " arrived while this node was primary at term " + term);
            }
            term = Math.max(term, ev.getTerm());
            if (ev.getSeq() == lastSeq + 1) {
                log.add(ev);
                lastSeq = ev.getSeq();
                lastLogTerm = ev.getTerm();
                publishLocked();
                return new ReplAck(ReplAck.Status.OK, term, lastSeq, null);
            }
            if (ev.getSeq() <= lastSeq && ev.getSeq() >= 1 && ev.same(log.get((int) ev.getSeq() - 1))) {
                return new ReplAck(ReplAck.Status.OK, term, lastSeq, "duplicate");
            }
            return new ReplAck(ReplAck.Status.NEED_SYNC, term, lastSeq, "event " + ev.getSeq() + " does not continue the log (last seq " + lastSeq + ")");
        } finally {
            lock.unlock();
        }
    }

    public ReplAck onSnapshot(long fromTerm, List<ReplEvent> events) {
        lock.lock();
        try {
            if (fromTerm < term) {
                return stale("snapshot", fromTerm);
            }
            if (primary && fromTerm > term) {
                supersededLocked(fromTerm, "a snapshot of term " + fromTerm + " arrived while this node was primary at term " + term);
            }
            term = Math.max(term, fromTerm);
            log.clear();
            log.addAll(events);
            lastSeq = events.isEmpty() ? 0 : events.get(events.size() - 1).getSeq();
            lastLogTerm = events.isEmpty() ? 0 : events.get(events.size() - 1).getTerm();
            rebuildLoggedJobs();
            record("REPLICATION_SYNCED", "node " + nodeId + " installed the primary's log: " + events.size()
                    + " events, last seq " + lastSeq + ", term " + term, fields("lastSeq", lastSeq, "term", term));
            publishLocked();
            return new ReplAck(ReplAck.Status.OK, term, lastSeq, null);
        } finally {
            lock.unlock();
        }
    }

    public List<ReplEvent> fetchLog() {
        lock.lock();
        try {
            return new ArrayList<>(log);
        } finally {
            lock.unlock();
        }
    }

    private ReplAck stale(String what, long theirTerm) {
        staleRejected.incrementAndGet();
        record("REPLICATION_STALE_REJECTED", "node " + nodeId + " refused " + what + ": term " + theirTerm + " < " + term,
                fields("staleTerm", theirTerm, "term", term));
        publishLocked();
        return new ReplAck(ReplAck.Status.STALE_TERM, term, lastSeq, "term " + theirTerm + " < " + term);
    }

    private void rebuildLoggedJobs() {
        loggedJobs.clear();
        for (Map.Entry<String, Job> e : JobReplay.rebuild(log).entrySet()) {
            if (e.getValue().getStatus().isActive()) {
                loggedJobs.add(e.getKey());
            }
        }
    }

    // =========================================================================
    // Promotion
    // =========================================================================

    /**
     * This node was elected: catch up from the peer with the longest log, start a new term, and
     * return the jobs the log describes (the unfinished ones are the orchestrator's to resume).
     */
    public Map<String, Job> promote() {
        // Ask every other member for its state, in parallel and without the lock.
        List<Integer> others = new ArrayList<>(config.membership().ids());
        others.remove(Integer.valueOf(nodeId));
        Map<Integer, Future<ReplState>> asked = new LinkedHashMap<>();
        for (int id : others) {
            asked.put(id, rmi.submit(() -> stub(id).replicationState()));
        }
        Map<Integer, ReplState> peers = new LinkedHashMap<>();
        for (Map.Entry<Integer, Future<ReplState>> e : asked.entrySet()) {
            try {
                peers.put(e.getKey(), e.getValue().get(CALL_TIMEOUT_MS + 300, TimeUnit.MILLISECONDS));
            } catch (Exception ex) {
                e.getValue().cancel(true);
                stubs.remove(e.getKey());
            }
        }
        long peerMaxTerm = 0;
        ReplState best = null;
        lock.lock();
        long mineTerm;
        long mineSeq;
        try {
            mineTerm = lastLogTerm;
            mineSeq = lastSeq;
        } finally {
            lock.unlock();
        }
        for (ReplState p : peers.values()) {
            peerMaxTerm = Math.max(peerMaxTerm, p.getTerm());
            boolean longer = p.getLastLogTerm() > mineTerm || (p.getLastLogTerm() == mineTerm && p.getLastSeq() > mineSeq);
            if (longer && (best == null || p.getLastLogTerm() > best.getLastLogTerm()
                    || (p.getLastLogTerm() == best.getLastLogTerm() && p.getLastSeq() > best.getLastSeq()))) {
                best = p;
            }
        }
        List<ReplEvent> theirs = null;
        if (best != null) {
            try {
                OrchestratorService stub = stub(best.getNodeId());
                theirs = call(stub::fetchLog, SNAPSHOT_TIMEOUT_MS);
            } catch (Exception e) {
                stubs.remove(best.getNodeId());
                System.err.println("[Node " + nodeId + "] could not fetch node " + best.getNodeId() + "'s log: " + root(e));
            }
        }
        lock.lock();
        try {
            if (theirs != null && !theirs.isEmpty()) {
                long theirTerm = theirs.get(theirs.size() - 1).getTerm();
                long theirSeq = theirs.get(theirs.size() - 1).getSeq();
                if (theirTerm > lastLogTerm || (theirTerm == lastLogTerm && theirSeq > lastSeq)) {
                    log.clear();
                    log.addAll(theirs);
                    lastSeq = theirSeq;
                    lastLogTerm = theirTerm;
                    record("REPLICATION_CATCH_UP", "node " + nodeId + " caught up from node " + best.getNodeId() + ": "
                            + theirs.size() + " events, last seq " + lastSeq, fields("from", best.getNodeId(), "lastSeq", lastSeq));
                }
            }
            term = Math.max(term, peerMaxTerm) + 1;
            primary = true;
            backupId = null;
            backupLastSeq = 0;
            noBackup = false;
            rebuildLoggedJobs();
            Map<String, Job> jobs = JobReplay.rebuild(log);
            int unfinished = 0;
            for (Job j : jobs.values()) {
                if (j.getStatus().isActive()) {
                    unfinished++;
                }
            }
            record("REPLICATION_PROMOTED", "node " + nodeId + " is primary at term " + term + " (log last seq " + lastSeq
                    + ", " + jobs.size() + " jobs in the log, " + unfinished + " unfinished to resume)",
                    fields("term", term, "lastSeq", lastSeq, "unfinished", unfinished));
            publishLocked();
            return jobs;
        } finally {
            lock.unlock();
        }
    }

    /** This node is no longer the leader. */
    public void demote() {
        lock.lock();
        try {
            if (primary) {
                primary = false;
                backupId = null;
                backupLastSeq = 0;
                publishLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** The log as the API shows it: one summary per event with seq above from. */
    public List<Map<String, Object>> logSummary(long from) {
        List<ReplEvent> copy = fetchLog();
        List<Map<String, Object>> out = new ArrayList<>();
        for (ReplEvent e : copy) {
            if (e.getSeq() > from) {
                out.add(e.toSummaryMap());
            }
        }
        return out;
    }

    private OrchestratorService stub(int id) throws Exception {
        OrchestratorService cached = stubs.get(id);
        if (cached != null) {
            return cached;
        }
        MemberSpec spec = config.membership().get(id);
        if (spec == null) {
            throw new IllegalArgumentException("node " + id + " is not a member");
        }
        Registry registry = LocateRegistry.getRegistry("localhost", spec.getPort(), TimeoutSocketFactory.INSTANCE);
        OrchestratorService fresh = (OrchestratorService) registry.lookup("orchestrator");
        stubs.put(id, fresh);
        return fresh;
    }

    private <T> T call(Callable<T> c, long timeoutMs) throws Exception {
        Future<T> f = rmi.submit(c);
        try {
            return f.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            f.cancel(true);
            throw new RuntimeException("RMI call timed out after " + timeoutMs + " ms", e);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            throw cause instanceof Exception ? (Exception) cause : e;
        }
    }

    private void record(String type, String details, Map<String, Object> fields) {
        events.record(type, details, fields);
    }

    private static Map<String, Object> fields(Object... kv) {
        Map<String, Object> f = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            f.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return f;
    }

    private static String root(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getMessage() == null ? c.getClass().getSimpleName() : c.getMessage();
    }

    private static java.util.concurrent.ThreadFactory daemon(String prefix) {
        java.util.concurrent.atomic.AtomicInteger n = new java.util.concurrent.atomic.AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
