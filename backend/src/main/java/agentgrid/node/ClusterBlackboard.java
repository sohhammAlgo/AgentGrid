package agentgrid.node;

import agentgrid.clock.TimeServiceImpl;
import agentgrid.replication.BlackboardEntry;
import agentgrid.replication.ConsistencyLevel;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

/**
 * The replicated blackboard of one node (Exp 5 in the live cluster), bound as
 * "blackboard-node-node-<id>".
 *
 * Every replica keeps {key, value, timestamp, writerNodeId, version} and merges with
 * BlackboardRecord.merge (higher timestamp wins, ties go to the higher writer id).
 * Timestamps come from this node's Berkeley-corrected clock, made monotonic per node.
 *
 * STRONG: probe which nodes are live (in parallel, capped at PROBE_CAP_MS). With fewer than a
 * majority of the configured cluster live, refuse and apply nothing anywhere. Otherwise apply
 * locally, replicate to every live node and return after they acknowledge (each call bounded
 * by PEER_TIMEOUT_MS). This is quorum-gated replication, not consensus: a node can die between
 * the check and the apply (then fewer acks come back: STORED_DEGRADED or FAILED_PARTIAL), and
 * concurrent writers of one key are ordered only by LWW.
 *
 * EVENTUAL: apply locally and acknowledge at once; push to each peer after the configured
 * (simulated) lag, retrying peers that are down; anti-entropy pulls newer records from a
 * random peer periodically, so replicas converge without operator action.
 *
 * Boot: the replica is not ready until it has pulled the records its live peers hold and,
 * with auto clock sync on, until the leader has synced its clock (or CLOCK_WAIT_MS passed).
 */
public final class ClusterBlackboard extends UnicastRemoteObject implements ClusterBlackboardService {

    private static final long serialVersionUID = 1L;

    public static final int MAX_KEY_CHARS = 128;
    public static final int MAX_VALUE_CHARS = 1024;
    static final long PEER_TIMEOUT_MS = 1500L;
    static final long PROBE_CAP_MS = 500L;
    static final long SNAPSHOT_WAIT_MS = 2000L;
    static final long CLOCK_WAIT_MS = 3000L;
    static final long RETRY_MS = 1000L;
    static final long REJOIN_RETRY_MIN_MS = 250L;
    static final long REJOIN_RETRY_MAX_MS = 2000L;
    static final long REJOIN_RETRY_TIMEOUT_MS = 5000L;
    static final long GIVE_UP_MS = 60000L;
    private static final int DETAIL_CHARS = 80;

    /** Invocation of one remote method on a peer replica. */
    private interface PeerCall<T> {
        T call(ClusterBlackboardService peer) throws Exception;
    }

    private final int nodeId;
    private final transient ClusterConfig config;
    private final transient TimeServiceImpl clock;
    private final transient NodeEventBuffer events;
    private final transient ClockCoordinator clockCoordinator;
    private final transient IntSupplier leaderView;
    private final transient Map<String, BlackboardRecord> store = new ConcurrentHashMap<>();
    private final transient Map<Integer, ClusterBlackboardService> stubs = new ConcurrentHashMap<>();
    private final transient ExecutorService calls = Executors.newCachedThreadPool(ElectionNode.daemon("bb-call-"));
    private final transient ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(2, ElectionNode.daemon("bb-sched-"));
    private final transient AtomicInteger pending = new AtomicInteger();
    private final transient BlackboardMetrics metrics = new BlackboardMetrics();
    private final transient Object stampLock = new Object();

    private long lastStamp;
    private long version;
    private volatile boolean ready;
    private volatile boolean joining;
    private volatile String snapshotSource = "not yet pulled";

    public ClusterBlackboard(int nodeId, ClusterConfig config, TimeServiceImpl clock, NodeEventBuffer events,
                             ClockCoordinator clockCoordinator, IntSupplier leaderView) throws RemoteException {
        super(0, TimeoutSocketFactory.INSTANCE, null);
        this.nodeId = nodeId;
        this.config = config;
        this.clock = clock;
        this.events = events;
        this.clockCoordinator = clockCoordinator;
        this.leaderView = leaderView;
    }

    /** Marks this node as joining the cluster (set before start()). */
    public void setJoining(boolean joining) {
        this.joining = joining;
    }

    /** Starts the boot catch-up and the periodic anti-entropy. */
    public void start() {
        Thread boot = new Thread(this::catchUp, "bb-boot-" + nodeId);
        boot.setDaemon(true);
        boot.start();
        long ae = config.getAntiEntropyMs();
        if (ae > 0) {
            scheduler.scheduleWithFixedDelay(this::antiEntropy, ae, ae, TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown() {
        calls.shutdownNow();
        scheduler.shutdownNow();
    }

    // =========================================================================
    // Writes
    // =========================================================================

    @Override
    public BlackboardWriteOutcome write(String key, String value, String mode) throws RemoteException {
        String m = mode == null ? "" : mode.trim().toUpperCase(Locale.ROOT);
        if (!m.equals("STRONG") && !m.equals("EVENTUAL")) {
            throw new RemoteException("mode must be STRONG or EVENTUAL, got: " + mode);
        }
        if (key == null || key.isEmpty() || key.length() > MAX_KEY_CHARS) {
            throw new RemoteException("key must be 1-" + MAX_KEY_CHARS + " characters");
        }
        if (value == null || value.length() > MAX_VALUE_CHARS) {
            throw new RemoteException("value must be at most " + MAX_VALUE_CHARS + " characters");
        }
        BlackboardWriteOutcome outcome = m.equals("STRONG") ? writeStrong(key, value) : writeEventual(key, value);
        metrics.recordWrite(outcome);
        return outcome;
    }

    private BlackboardWriteOutcome writeStrong(String key, String value) {
        long t0 = System.nanoTime();
        boolean quiet = isJobKey(key);
        // One membership snapshot for the whole decision: quorum = floor(n / 2) + 1 of the n
        // members of this node's current epoch. (No epoch fencing on replication: a replica
        // applies the record whatever epoch it holds; see README, limitations.)
        Membership members = config.membership();
        List<Integer> live = probeLive(members);
        int majority = members.quorum();
        if (live.size() < majority) {
            long latency = elapsedMs(t0);
            String msg = "only " + live.size() + " of " + members.size() + " members live (" + live
                    + "); a STRONG write needs " + majority + " (quorum of " + members.size() + " members, epoch "
                    + members.getEpoch() + "); nothing was written";
            if (!quiet) {
                Map<String, Object> f = fields(key, "STRONG");
                f.put("live", live.toString());
                events.record("BLACKBOARD_REFUSED", "STRONG write of " + clip(key) + " refused: " + msg, f);
            }
            return new BlackboardWriteOutcome(BlackboardWriteOutcome.Status.REFUSED, "STRONG", key, nodeId, null,
                    live, List.of(), List.of(), latency, msg);
        }

        BlackboardRecord record = newRecord(key, value);
        long lamport = quiet ? 0 : recordWrite(record, "STRONG", "live " + live);
        applyLocal(record, quiet);

        List<Integer> acked = new ArrayList<>();
        List<Integer> failed = new ArrayList<>();
        acked.add(nodeId);
        Map<Integer, Future<Boolean>> sends = new LinkedHashMap<>();
        for (int peer : live) {
            if (peer != nodeId) {
                sends.put(peer, calls.submit(() -> invoke(peer, p -> p.replicaApply(record, lamport))));
            }
        }
        for (Map.Entry<Integer, Future<Boolean>> e : sends.entrySet()) {
            Boolean ok = waitFor(e.getValue(), PEER_TIMEOUT_MS);
            if (Boolean.TRUE.equals(ok)) {
                acked.add(e.getKey());
            } else {
                failed.add(e.getKey());
            }
        }
        Collections.sort(acked);
        BlackboardWriteOutcome.Status status;
        String msg;
        if (failed.isEmpty()) {
            status = BlackboardWriteOutcome.Status.STORED;
            msg = "acknowledged by all " + acked.size() + " live nodes " + acked;
        } else if (acked.size() >= majority) {
            status = BlackboardWriteOutcome.Status.STORED_DEGRADED;
            msg = "acknowledged by " + acked + "; " + failed + " failed after the live check and will catch up later";
        } else {
            status = BlackboardWriteOutcome.Status.FAILED_PARTIAL;
            msg = "only " + acked + " acknowledged (fewer than " + majority + "); " + failed
                    + " failed after the live check; the nodes that acknowledged keep the record";
        }
        return new BlackboardWriteOutcome(status, "STRONG", key, nodeId, record, live, acked, failed,
                elapsedMs(t0), msg);
    }

    private BlackboardWriteOutcome writeEventual(String key, String value) {
        long t0 = System.nanoTime();
        boolean quiet = isJobKey(key);
        BlackboardRecord record = newRecord(key, value);
        long lamport = quiet ? 0 : recordWrite(record, "EVENTUAL", "propagation after " + config.getEventualLagMs() + " ms");
        applyLocal(record, quiet);
        long firstAttempt = System.currentTimeMillis() + config.getEventualLagMs();
        for (int peer : config.getNodeIds()) {
            if (peer != nodeId) {
                pending.incrementAndGet();
                scheduler.schedule(() -> deliver(peer, record, lamport, firstAttempt),
                        config.getEventualLagMs(), TimeUnit.MILLISECONDS);
            }
        }
        return new BlackboardWriteOutcome(BlackboardWriteOutcome.Status.ACCEPTED, "EVENTUAL", key, nodeId, record,
                List.of(nodeId), List.of(nodeId), List.of(), elapsedMs(t0),
                "applied locally; pushed to peers after " + config.getEventualLagMs() + " ms (simulated lag)");
    }

    /** One EVENTUAL delivery attempt; a peer that is down is retried every RETRY_MS for GIVE_UP_MS. */
    private void deliver(int peer, BlackboardRecord record, long lamport, long firstAttempt) {
        if (!config.membership().contains(peer)) {
            pending.decrementAndGet();   // the peer was removed from the cluster
            return;
        }
        Boolean ok = callPeer(peer, p -> p.replicaApply(record, lamport), PEER_TIMEOUT_MS);
        if (Boolean.TRUE.equals(ok)) {
            pending.decrementAndGet();
            metrics.delivered();
            return;
        }
        if (System.currentTimeMillis() - firstAttempt >= GIVE_UP_MS) {
            pending.decrementAndGet();  // anti-entropy or the peer's restart catch-up covers it
            return;
        }
        metrics.retried();
        scheduler.schedule(() -> deliver(peer, record, lamport, firstAttempt), RETRY_MS, TimeUnit.MILLISECONDS);
    }

    private BlackboardRecord newRecord(String key, String value) {
        synchronized (stampLock) {
            // Monotonic per node: a backward Berkeley correction never makes this node's
            // later write look older than its earlier one.
            lastStamp = Math.max(clock.getTime(), lastStamp + 1);
            version++;
            return new BlackboardRecord(key, value, lastStamp, nodeId, version);
        }
    }

    private long recordWrite(BlackboardRecord r, String mode, String note) {
        Map<String, Object> f = fields(r.getKey(), mode);
        f.put("timestamp", r.getTimestamp());
        f.put("writer", r.getWriterNodeId());
        return events.record("BLACKBOARD_WRITE", mode + " write of " + clip(r.getKey()) + " = \"" + clip(r.getValue())
                + "\" by node " + nodeId + " at ts " + r.getTimestamp() + " (" + note + ")", f).getLamport();
    }

    // =========================================================================
    // Applying records
    // =========================================================================

    /** Merges record into this replica; returns true if the replica now holds it or a newer one. */
    private boolean applyLocal(BlackboardRecord incoming, boolean quiet) {
        BlackboardRecord[] before = new BlackboardRecord[1];
        BlackboardRecord after = store.compute(incoming.getKey(), (k, current) -> {
            before[0] = current;
            return BlackboardRecord.merge(current, incoming);
        });
        BlackboardRecord current = before[0];
        boolean changed = after == incoming && (current == null || !current.sameStamp(incoming));
        boolean conflict = current != null && !current.sameStamp(incoming)
                && current.getWriterNodeId() != incoming.getWriterNodeId()
                && !String.valueOf(current.getValue()).equals(incoming.getValue());
        if (!quiet && conflict) {
            BlackboardRecord winner = after;
            BlackboardRecord loser = winner == incoming ? current : incoming;
            Map<String, Object> f = fields(incoming.getKey(), null);
            f.put("winnerTimestamp", winner.getTimestamp());
            f.put("winnerWriter", winner.getWriterNodeId());
            f.put("loserTimestamp", loser.getTimestamp());
            f.put("loserWriter", loser.getWriterNodeId());
            events.record("LWW_CONFLICT_RESOLVED", "LWW on node " + nodeId + " for " + clip(incoming.getKey())
                    + ": kept \"" + clip(winner.getValue()) + "\" (ts " + winner.getTimestamp() + ", node "
                    + winner.getWriterNodeId() + ") over \"" + clip(loser.getValue()) + "\" (ts "
                    + loser.getTimestamp() + ", node " + loser.getWriterNodeId() + ")", f);
        }
        return changed;
    }

    @Override
    public boolean replicaApply(BlackboardRecord record, long senderLamport) {
        if (senderLamport > 0) {
            events.receive(senderLamport);
        }
        boolean quiet = isJobKey(record.getKey());
        boolean changed = applyLocal(record, quiet);
        if (changed && !quiet) {
            Map<String, Object> f = fields(record.getKey(), null);
            f.put("timestamp", record.getTimestamp());
            f.put("writer", record.getWriterNodeId());
            events.record("BLACKBOARD_APPLIED", "node " + nodeId + " applied " + clip(record.getKey()) + " = \""
                    + clip(record.getValue()) + "\" (ts " + record.getTimestamp() + ", writer node "
                    + record.getWriterNodeId() + ")", f);
        }
        return true;  // holds this record or a newer one
    }

    @Override
    public List<BlackboardRecord> pullNewer(HashMap<String, String> digest) {
        List<BlackboardRecord> out = new ArrayList<>();
        for (BlackboardRecord r : store.values()) {
            String theirs = digest.get(r.getKey());
            if (theirs == null) {
                out.add(r);
                continue;
            }
            String[] parts = theirs.split(":");
            BlackboardRecord probe = new BlackboardRecord(r.getKey(), null, Long.parseLong(parts[0]),
                    Integer.parseInt(parts[1]), 0);
            if (BlackboardRecord.newer(r, probe)) {
                out.add(r);
            }
        }
        return out;
    }

    private HashMap<String, String> digest() {
        HashMap<String, String> d = new HashMap<>();
        for (BlackboardRecord r : store.values()) {
            d.put(r.getKey(), r.getTimestamp() + ":" + r.getWriterNodeId());
        }
        return d;
    }

    /** Pulls newer records from one peer; returns how many changed this replica, or -1 if unreachable. */
    private int pullFrom(int peer) {
        HashMap<String, String> d = digest();
        List<BlackboardRecord> newer = callPeer(peer, p -> p.pullNewer(d), PEER_TIMEOUT_MS);
        if (newer == null) {
            return -1;
        }
        int applied = 0;
        for (BlackboardRecord r : newer) {
            if (applyLocal(r, true)) {
                applied++;
            }
        }
        return applied;
    }

    private void antiEntropy() {
        try {
            List<Integer> peers = new ArrayList<>(config.getNodeIds());
            peers.remove(Integer.valueOf(nodeId));
            if (peers.isEmpty()) {
                return;
            }
            int peer = peers.get(ThreadLocalRandom.current().nextInt(peers.size()));
            int applied = pullFrom(peer);
            if (applied > 0) {
                metrics.antiEntropyApplied(applied);
                Map<String, Object> f = new LinkedHashMap<>();
                f.put("kind", "anti-entropy");
                f.put("from", peer);
                f.put("applied", applied);
                events.record("BLACKBOARD_SYNC", "anti-entropy: node " + nodeId + " pulled " + applied
                        + " newer record(s) from node " + peer, f);
            }
        } catch (RuntimeException e) {
            System.err.println("[Node " + nodeId + "] anti-entropy failed: " + e);
        }
    }

    // =========================================================================
    // Boot catch-up and the ready gate
    // =========================================================================

    private void catchUp() {
        long deadline = System.currentTimeMillis() + SNAPSHOT_WAIT_MS;
        List<Integer> pulledFrom = new ArrayList<>();
        int applied = 0;
        while (pulledFrom.isEmpty() && System.currentTimeMillis() < deadline) {
            for (int peer : config.getNodeIds()) {
                if (peer == nodeId) {
                    continue;
                }
                int n = pullFrom(peer);
                if (n >= 0) {
                    pulledFrom.add(peer);
                    applied += n;
                }
            }
            if (pulledFrom.isEmpty()) {
                sleep(300);
            }
        }
        snapshotSource = pulledFrom.isEmpty() ? "none (no live peer within " + SNAPSHOT_WAIT_MS + " ms)"
                : "nodes " + pulledFrom;
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("kind", "restart-snapshot");
        f.put("from", pulledFrom.toString());
        f.put("applied", applied);
        events.record("BLACKBOARD_SYNC", "snapshot pull: node " + nodeId + " pulled " + applied + " record(s) from "
                + snapshotSource, f);

        long clockStart = System.currentTimeMillis();
        int[] counts = new int[2];   // rejoin attempts, polls without a known leader
        // A joining node is not a member yet, so no round can include it: it reports ready
        // after its snapshot and gets synced (below, in the background) once it has joined.
        if (clockCoordinator.isAutoSync() && !joining) {
            long clockDeadline = clockStart + CLOCK_WAIT_MS;
            while (System.currentTimeMillis() < clockDeadline && !clockSynced()) {
                rejoinAttempt(Math.max(1, clockDeadline - System.currentTimeMillis()), counts, clockStart);
                if (!clockSynced()) {
                    sleep(200);
                }
            }
        }
        ready = true;
        System.out.println("[Node " + nodeId + "] blackboard ready (" + store.size() + " records from "
                + snapshotSource + ", clockSynced=" + clockSynced() + ")");

        // READY is capped at CLOCK_WAIT_MS, but a rejoin sync that has not happened is not dropped:
        // keep asking the leader (short backoff) until a round includes this node or auto-sync is off.
        if (clockCoordinator.isAutoSync() && !clockSynced()) {
            log("clock wait ended unsynced after " + (System.currentTimeMillis() - clockStart) + " ms ("
                    + counts[0] + " attempts, " + counts[1] + " polls without a leader); retrying in the background");
            long backoff = REJOIN_RETRY_MIN_MS;
            while (clockCoordinator.isAutoSync() && !clockSynced() && !Thread.currentThread().isInterrupted()) {
                sleep(backoff);
                if (clockCoordinator.isAutoSync() && !clockSynced()) {
                    rejoinAttempt(REJOIN_RETRY_TIMEOUT_MS, counts, clockStart);
                }
                backoff = Math.min(backoff * 2, REJOIN_RETRY_MAX_MS);
            }
            log(clockSynced() ? "clock synced " + (System.currentTimeMillis() - clockStart) + " ms after the clock wait began"
                    : "stopped retrying the rejoin sync: auto-sync is off");
        }
    }

    /** One rejoin-sync request to the current leader; a true answer means its round corrected this node. */
    private void rejoinAttempt(long timeoutMs, int[] counts, long clockStart) {
        int leader = leaderView.getAsInt();
        if (leader <= 0) {
            counts[1]++;
            return;
        }
        counts[0]++;
        long t0 = System.currentTimeMillis();
        String[] why = new String[1];
        Boolean ok = leader == nodeId ? localRejoin() : callClock(leader, timeoutMs, why);
        log("rejoin sync attempt " + counts[0] + " to leader " + leader + " (" + counts[1]
                + " polls without a leader so far): " + (ok == null ? why[0] : ok) + " after "
                + (System.currentTimeMillis() - t0) + " ms, " + (System.currentTimeMillis() - clockStart)
                + " ms after the clock wait began");
        if (Boolean.TRUE.equals(ok)) {
            // The leader's round included and corrected this node; its noteSynced() call is
            // asynchronous, so record it here too.
            clockCoordinator.noteSynced(0);
        }
    }

    private Boolean localRejoin() {
        try {
            return clockCoordinator.requestRejoinSync(nodeId);
        } catch (RemoteException e) {
            return false;
        }
    }

    private Boolean callClock(int leader, long timeoutMs, String[] why) {
        Future<Boolean> f = calls.submit(() -> {
            ClockCoordinatorService c = (ClockCoordinatorService) registry(leader).lookup("clock");
            return c.requestRejoinSync(nodeId);
        });
        try {
            return f.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            f.cancel(true);
            why[0] = "TIMEOUT (" + timeoutMs + " ms)";
        } catch (Exception e) {
            f.cancel(true);
            Throwable c = e.getCause() != null ? e.getCause() : e;
            why[0] = "EXCEPTION " + c;
        }
        return null;
    }

    private void log(String msg) {
        System.out.println(java.time.LocalTime.now() + " [Node " + nodeId + "] blackboard: " + msg);
    }

    private boolean clockSynced() {
        return clockCoordinator.lastSyncedTrueMs() >= 0;
    }

    // =========================================================================
    // Reads, snapshots, metrics
    // =========================================================================

    @Override
    public BlackboardState read(String key) {
        BlackboardRecord r = key == null ? null : store.get(key);
        return state(r == null ? List.of() : List.of(r));
    }

    @Override
    public BlackboardState snapshot(String prefix) {
        String p = prefix == null ? "" : prefix;
        List<BlackboardRecord> all = new ArrayList<>();
        for (BlackboardRecord r : store.values()) {
            if (r.getKey().startsWith(p)) {
                all.add(r);
            }
        }
        all.sort((a, b) -> a.getKey().compareTo(b.getKey()));
        return state(all);
    }

    private BlackboardState state(List<BlackboardRecord> records) {
        long stamp;
        synchronized (stampLock) {
            stamp = lastStamp;
        }
        return new BlackboardState(nodeId, ready, clockSynced(), snapshotSource, pending.get(), stamp, records);
    }

    @Override
    public BlackboardMetrics metrics() {
        return metrics.copy(pending.get());
    }

    @Override
    public boolean isReady() {
        return ready;
    }

    @Override
    public boolean alive() {
        return true;
    }

    // =========================================================================
    // Submitted ReplicationService methods
    // =========================================================================

    @Override
    public String getNodeId() {
        return "node-" + nodeId;
    }

    @Override
    public BlackboardEntry getFinding(String key) {
        return toEntry(store.get(key));
    }

    @Override
    public boolean publishFinding(BlackboardEntry entry, ConsistencyLevel level) throws RemoteException {
        return write(entry.getSubtaskId(), entry.getFindingPayload(), level.name()).getStatus().stored();
    }

    @Override
    public boolean receiveReplicate(BlackboardEntry entry) {
        int writer;
        try {
            writer = Integer.parseInt(entry.getAgentId().replaceAll("\\D", ""));
        } catch (NumberFormatException e) {
            writer = 0;
        }
        return replicaApply(new BlackboardRecord(entry.getSubtaskId(), entry.getFindingPayload(),
                entry.getTimestamp(), writer, entry.getVersion()), 0);
    }

    @Override
    public Map<String, BlackboardEntry> getAllFindings() {
        Map<String, BlackboardEntry> out = new ConcurrentHashMap<>();
        for (BlackboardRecord r : store.values()) {
            out.put(r.getKey(), toEntry(r));
        }
        return out;
    }

    private static BlackboardEntry toEntry(BlackboardRecord r) {
        if (r == null) {
            return null;
        }
        return new BlackboardEntry("", r.getKey(), r.getValue(), "node-" + r.getWriterNodeId(),
                r.getTimestamp(), r.getVersion());
    }

    // =========================================================================
    // Peers
    // =========================================================================

    /** Members that answer a probe within PROBE_CAP_MS overall (this node always counts). */
    private List<Integer> probeLive(Membership members) {
        Map<Integer, Future<Boolean>> probes = new LinkedHashMap<>();
        for (int peer : members.ids()) {
            if (peer != nodeId) {
                probes.put(peer, calls.submit(() -> invoke(peer, ClusterBlackboardService::alive)));
            }
        }
        long deadline = System.currentTimeMillis() + PROBE_CAP_MS;
        List<Integer> live = new ArrayList<>();
        live.add(nodeId);
        for (Map.Entry<Integer, Future<Boolean>> e : probes.entrySet()) {
            long left = Math.max(1, deadline - System.currentTimeMillis());
            if (Boolean.TRUE.equals(waitFor(e.getValue(), left))) {
                live.add(e.getKey());
            }
        }
        Collections.sort(live);
        return live;
    }

    /** Calls a peer with a hard timeout; null if it failed or timed out. */
    private <T> T callPeer(int peer, PeerCall<T> call, long timeoutMs) {
        return waitFor(calls.submit(() -> invoke(peer, call)), timeoutMs);
    }

    /** Through a cached stub; one failure with a cached stub is retried with a fresh lookup. */
    private <T> T invoke(int peer, PeerCall<T> call) throws Exception {
        ClusterBlackboardService cached = stubs.get(peer);
        if (cached != null) {
            try {
                return call.call(cached);
            } catch (Exception e) {
                stubs.remove(peer, cached);
            }
        }
        ClusterBlackboardService fresh = (ClusterBlackboardService) registry(peer).lookup("blackboard-node-node-" + peer);
        stubs.put(peer, fresh);
        try {
            return call.call(fresh);
        } catch (Exception e) {
            stubs.remove(peer, fresh);
            throw e;
        }
    }

    private Registry registry(int peer) throws RemoteException {
        return LocateRegistry.getRegistry("localhost", config.getNode(peer).getPort(), TimeoutSocketFactory.INSTANCE);
    }

    private static <T> T waitFor(Future<T> f, long timeoutMs) {
        try {
            return f.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            f.cancel(true);
            return null;
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Job findings and answers are summarised once per job by the leader; no per-write events. */
    static boolean isJobKey(String key) {
        return key != null && key.startsWith("job/");
    }

    private static Map<String, Object> fields(String key, String mode) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("key", clip(key));
        if (mode != null) {
            f.put("mode", mode);
        }
        return f;
    }

    /** Keys and values are user text; event details carry at most DETAIL_CHARS of them. */
    static String clip(String s) {
        if (s == null) {
            return "";
        }
        return s.length() <= DETAIL_CHARS ? s : s.substring(0, DETAIL_CHARS) + "...";
    }

    private static long elapsedMs(long t0) {
        return (System.nanoTime() - t0) / 1_000_000;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
