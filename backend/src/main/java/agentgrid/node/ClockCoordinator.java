package agentgrid.node;

import agentgrid.clock.TimeService;
import agentgrid.clock.TimeServiceImpl;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The cluster's single Berkeley clock-synchronization implementation. Every node has one;
 * only the elected leader's runs rounds (LeaderLifecycle starts it on onElected and stops it
 * on onDemoted).
 *
 * A round: the leader reads every node's clock in parallel (per-node timeout, round-trip
 * compensated against its own clock), includes itself with offset 0, averages the offsets
 * of the nodes that answered, and sends each of them the correction to that average. A node
 * that does not answer is skipped; the round does not abort. Berkeley makes the nodes agree
 * with each other, not with true time: they converge to the mean of their offsets.
 */
public final class ClockCoordinator extends UnicastRemoteObject implements ClockCoordinatorService, LeaderLifecycle {

    private static final long serialVersionUID = 1L;

    public static final long REJOIN_DEBOUNCE_MS = 2000L;
    static final long NODE_TIMEOUT_MS = 700L;

    private final int nodeId;
    private final transient ClusterConfig config;
    private final transient TimeServiceImpl localClock;
    private final transient NodeEventBuffer events;
    private final transient ExecutorService calls = Executors.newCachedThreadPool(ElectionNode.daemon("clock-call-"));
    private final transient ScheduledExecutorService timer =
            Executors.newSingleThreadScheduledExecutor(ElectionNode.daemon("clock-periodic-"));
    private final transient Object roundLock = new Object();

    private volatile boolean active;
    private volatile boolean autoSync;
    private volatile long lastSyncedTrueMs = -1;
    private transient ScheduledFuture<?> periodic;
    private long lastRoundStartMs = -1;
    private transient Set<Integer> lastRoundNodes = Collections.emptySet();

    public ClockCoordinator(int nodeId, ClusterConfig config, TimeServiceImpl localClock, NodeEventBuffer events,
                            boolean autoSync) throws RemoteException {
        super(0, TimeoutSocketFactory.INSTANCE, null);
        this.nodeId = nodeId;
        this.config = config;
        this.localClock = localClock;
        this.events = events;
        this.autoSync = autoSync;
    }

    /** Machine time of the last round that corrected this node's clock, or -1. */
    public long lastSyncedTrueMs() {
        return lastSyncedTrueMs;
    }

    // =========================================================================
    // LeaderLifecycle
    // =========================================================================

    @Override
    public synchronized void onElected(int electedNodeId) {
        active = true;
        long interval = config.getBerkeleyIntervalMs();
        if (interval > 0 && (periodic == null || periodic.isDone())) {
            periodic = timer.scheduleAtFixedRate(this::periodicRound, interval, interval, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized void onDemoted(int demotedNodeId, int newLeaderId) {
        active = false;
        if (periodic != null) {
            periodic.cancel(false);
            periodic = null;
        }
    }

    @Override
    public void onLeaderLost(int nodeId, int lostLeaderId) {
        // Another node's leadership ended; nothing of ours to stop.
    }

    private void periodicRound() {
        if (!active || !autoSync) {
            return;
        }
        try {
            synchronized (roundLock) {
                doRound("periodic");
            }
        } catch (RuntimeException e) {
            System.err.println("[Node " + nodeId + "] periodic Berkeley round failed: " + e);
        }
    }

    // =========================================================================
    // ClockCoordinatorService
    // =========================================================================

    @Override
    public LinkedHashMap<String, Object> runSyncRound(String reason) throws RemoteException {
        if (!active) {
            throw new RemoteException("node " + nodeId + " is not the leader; it does not coordinate clock sync");
        }
        synchronized (roundLock) {
            return doRound(reason);
        }
    }

    @Override
    public boolean requestRejoinSync(int requester) throws RemoteException {
        if (!active || !autoSync) {
            return false;
        }
        long requestedAt = System.currentTimeMillis();
        synchronized (roundLock) {
            if (lastRoundStartMs >= requestedAt && lastRoundNodes.contains(requester)) {
                return true;
            }
            long wait = lastRoundStartMs + REJOIN_DEBOUNCE_MS - System.currentTimeMillis();
            if (wait > 0) {
                try {
                    Thread.sleep(wait);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            if (!active || !autoSync) {
                return false;
            }
            doRound("rejoin of node " + requester);
            return lastRoundNodes.contains(requester);
        }
    }

    @Override
    public void setAutoSync(boolean enabled) {
        if (enabled != autoSync) {
            System.out.println("[Node " + nodeId + "] auto clock sync " + (enabled ? "enabled" : "disabled"));
        }
        autoSync = enabled;
    }

    @Override
    public boolean isAutoSync() {
        return autoSync;
    }

    @Override
    public void noteSynced(long coordinatorLamport) {
        if (coordinatorLamport > 0) {
            events.receive(coordinatorLamport);
        }
        lastSyncedTrueMs = System.currentTimeMillis();
    }

    // =========================================================================
    // The round (caller holds roundLock)
    // =========================================================================

    private LinkedHashMap<String, Object> doRound(String reason) {
        lastRoundStartMs = System.currentTimeMillis();
        Map<Integer, TimeService> clocks = new java.util.concurrent.ConcurrentHashMap<>();
        Map<Integer, Long> before = readOffsets(clocks, config.getNodeIds());

        long sum = 0;
        for (long o : before.values()) {
            sum += o;
        }
        long average = before.isEmpty() ? 0 : sum / before.size();

        Map<String, Object> corrections = new LinkedHashMap<>();
        Set<Integer> corrected = new TreeSet<>();
        List<Future<Boolean>> applied = new ArrayList<>();
        List<Integer> order = new ArrayList<>(before.keySet());
        for (int id : order) {
            long correction = average - before.get(id);
            corrections.put(String.valueOf(id), correction);
            if (id == nodeId) {
                localClock.adjustTime(correction);
                applied.add(null);
            } else {
                TimeService ts = clocks.get(id);
                applied.add(calls.submit(() -> {
                    ts.adjustTime(correction);
                    return true;
                }));
            }
        }
        for (int i = 0; i < order.size(); i++) {
            Future<Boolean> f = applied.get(i);
            if (f == null || waitFor(f, NODE_TIMEOUT_MS) != null) {
                corrected.add(order.get(i));
            }
        }

        Map<Integer, Long> after = readOffsets(clocks, new ArrayList<>(corrected));
        List<Integer> skipped = new ArrayList<>();
        for (int id : config.getNodeIds()) {
            if (!corrected.contains(id)) {
                skipped.add(id);
            }
        }
        lastRoundNodes = corrected;

        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        result.put("spreadBefore", spread(before));
        result.put("spreadAfter", spread(after));
        result.put("averageOffset", average);
        result.put("nodeCount", corrected.size());
        result.put("corrections", corrections);
        result.put("coordinator", nodeId);
        result.put("reason", reason);
        result.put("skipped", skipped);

        Map<String, Object> f = new LinkedHashMap<>();
        f.put("reason", reason);
        f.put("coordinator", nodeId);
        f.put("nodeCount", corrected.size());
        f.put("spreadBefore", result.get("spreadBefore"));
        f.put("spreadAfter", result.get("spreadAfter"));
        f.put("skipped", skipped.toString());
        long lamport = events.record("CLOCK_SYNC", "Berkeley round (" + reason + ") by leader node " + nodeId + " over "
                + corrected.size() + " nodes: spread " + result.get("spreadBefore") + " ms -> "
                + result.get("spreadAfter") + " ms" + (skipped.isEmpty() ? "" : ", skipped " + skipped), f).getLamport();

        for (int id : corrected) {
            if (id == nodeId) {
                noteSynced(0);
            } else {
                calls.submit(() -> {
                    try {
                        ClockCoordinatorService peer = (ClockCoordinatorService) registry(id).lookup("clock");
                        peer.noteSynced(lamport);
                    } catch (Exception ignored) {
                        // best effort: the node only loses its clockSynced flag
                    }
                    return null;
                });
            }
        }
        return result;
    }

    /** Offsets (node clock minus this node's clock, RTT-compensated) of the nodes that answered. */
    private Map<Integer, Long> readOffsets(Map<Integer, TimeService> clocks, List<Integer> ids) {
        Map<Integer, Future<Long>> reads = new LinkedHashMap<>();
        Map<Integer, Long> offsets = new TreeMap<>();
        for (int id : ids) {
            if (id == nodeId) {
                offsets.put(id, 0L);
                continue;
            }
            reads.put(id, calls.submit(() -> {
                TimeService ts = clocks.get(id);
                if (ts == null) {
                    ts = (TimeService) registry(id).lookup("time");
                    clocks.put(id, ts);
                }
                long c0 = localClock.getTime();
                long remote = ts.getTime();
                long c1 = localClock.getTime();
                return remote - (c0 + (c1 - c0) / 2);
            }));
        }
        for (Map.Entry<Integer, Future<Long>> e : reads.entrySet()) {
            Long offset = waitFor(e.getValue(), NODE_TIMEOUT_MS);
            if (offset != null) {
                offsets.put(e.getKey(), offset);
            }
        }
        return offsets;
    }

    private Registry registry(int id) throws RemoteException {
        return LocateRegistry.getRegistry("localhost", config.getNode(id).getPort(), TimeoutSocketFactory.INSTANCE);
    }

    private static <T> T waitFor(Future<T> f, long timeoutMs) {
        try {
            return f.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            f.cancel(true);
            return null;
        }
    }

    private static long spread(Map<Integer, Long> offsets) {
        if (offsets.isEmpty()) {
            return 0;
        }
        return Collections.max(offsets.values()) - Collections.min(offsets.values());
    }

    public void shutdown() {
        calls.shutdownNow();
        timer.shutdownNow();
    }
}
