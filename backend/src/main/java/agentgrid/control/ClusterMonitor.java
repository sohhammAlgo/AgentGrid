package agentgrid.control;

import agentgrid.clock.TimeService;
import agentgrid.node.ClockCoordinatorService;
import agentgrid.node.ClusterConfig;
import agentgrid.node.NodeAgent;
import agentgrid.node.NodeElection;
import agentgrid.node.NodeEvent;
import agentgrid.node.NodeTelemetry;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Periodically monitors cluster nodes over RMI.
 * Wraps every RMI call in a dedicated Future with a strict 700 ms timeout to prevent
 * thread blocking on dead or unresponsive remote processes.
 *
 * Each cycle polls every node's status and leader view, then pulls new telemetry events
 * from every UP node and merges them into the EventLog. Cycles run every 1 s, or every
 * 200 ms while an RMI burst or an election is active.
 */
public class ClusterMonitor {

    private static final long RMI_TIMEOUT_MS = 700L;
    private static final long FAST_POLL_MS = 200L;
    private static final long SLOW_POLL_MS = 1000L;

    public static class NodeStatus {
        private final int id;
        private final int port;
        private final boolean up;
        private final int poolSize;
        private final Integer queueDepth;
        private final Long lamport;
        private final Long clockOffsetMs;
        private final List<String> bindings;
        private final Integer leaderView;
        private final Boolean electing;
        private final String algorithm;

        public NodeStatus(
                int id,
                int port,
                boolean up,
                int poolSize,
                Integer queueDepth,
                Long lamport,
                Long clockOffsetMs,
                List<String> bindings) {
            this(id, port, up, poolSize, queueDepth, lamport, clockOffsetMs, bindings, null, null, null);
        }

        public NodeStatus(
                int id,
                int port,
                boolean up,
                int poolSize,
                Integer queueDepth,
                Long lamport,
                Long clockOffsetMs,
                List<String> bindings,
                Integer leaderView,
                Boolean electing,
                String algorithm) {
            this.id = id;
            this.port = port;
            this.up = up;
            this.poolSize = poolSize;
            this.queueDepth = queueDepth;
            this.lamport = lamport;
            this.clockOffsetMs = clockOffsetMs;
            this.bindings = bindings != null ? bindings : Collections.emptyList();
            this.leaderView = leaderView;
            this.electing = electing;
            this.algorithm = algorithm;
        }

        public int getId() { return id; }
        public int getPort() { return port; }
        public boolean isUp() { return up; }
        public int getPoolSize() { return poolSize; }
        public Integer getQueueDepth() { return queueDepth; }
        public Long getLamport() { return lamport; }
        public Long getClockOffsetMs() { return clockOffsetMs; }
        public List<String> getBindings() { return bindings; }
        public Integer getLeaderView() { return leaderView; }
        public Boolean getElecting() { return electing; }
        public String getAlgorithm() { return algorithm; }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", id);
            m.put("port", port);
            m.put("up", up);
            m.put("poolSize", poolSize);
            m.put("queueDepth", queueDepth);
            m.put("lamport", lamport);
            m.put("clockOffsetMs", clockOffsetMs);
            m.put("bindings", bindings);
            m.put("leaderView", up ? leaderView : null);
            m.put("isLeader", up && leaderView != null && leaderView == id);
            return m;
        }
    }

    private final ClusterConfig config;
    private final EventLog eventLog;
    private final ElectionTracker electionTracker = new ElectionTracker();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService rmiPool = Executors.newCachedThreadPool();

    private final Map<Integer, NodeStatus> latestStatuses = new ConcurrentHashMap<>();
    private final Map<Integer, Boolean> lastKnownState = new ConcurrentHashMap<>();
    /** Per node: {incarnation, last pulled seq}. */
    private final Map<Integer, long[]> telemetryCursors = new ConcurrentHashMap<>();

    private volatile boolean burstActive = false;
    private volatile long electionFastUntilMs = 0;
    private volatile String desiredAlgorithm = "BULLY";
    private volatile boolean desiredAutoSync = true;
    private volatile JobDirectory jobDirectory;
    private Thread pollingThread;
    private final Object burstLock = new Object();

    public void setBurstActive(boolean active) {
        this.burstActive = active;
        wake();
    }

    public boolean isBurstActive() {
        return burstActive || isElectionFast();
    }

    /** Switches to fast polling for the next windowMs (e.g. right after a kill or election request). */
    public void expectElectionActivity(long windowMs) {
        electionFastUntilMs = Math.max(electionFastUntilMs, System.currentTimeMillis() + windowMs);
        wake();
    }

    private boolean isElectionFast() {
        JobDirectory jd = jobDirectory;
        return electionTracker.isActive() || System.currentTimeMillis() < electionFastUntilMs
                || (jd != null && jd.hasActive());
    }

    /** Jobs are refreshed (and orphans detected) once per monitoring cycle. */
    public void setJobDirectory(JobDirectory jobDirectory) {
        this.jobDirectory = jobDirectory;
    }

    private void wake() {
        synchronized (burstLock) {
            burstLock.notifyAll();
        }
    }

    public void setDesiredAlgorithm(String algorithm) {
        this.desiredAlgorithm = algorithm;
    }

    public void setDesiredAutoSync(boolean enabled) {
        this.desiredAutoSync = enabled;
    }

    public boolean getDesiredAutoSync() {
        return desiredAutoSync;
    }

    public String getDesiredAlgorithm() {
        return desiredAlgorithm;
    }

    public ElectionTracker getElectionTracker() {
        return electionTracker;
    }

    public ClusterMonitor(ClusterConfig config, EventLog eventLog) {
        this.config = config;
        this.eventLog = eventLog;
    }

    public void start() {
        pollingThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                long start = System.currentTimeMillis();
                pollAll();
                long elapsed = System.currentTimeMillis() - start;
                long delay = (burstActive || isElectionFast()) ? FAST_POLL_MS : SLOW_POLL_MS;
                long sleep = Math.max(0, delay - elapsed);
                if (sleep > 0) {
                    try {
                        synchronized (burstLock) {
                            burstLock.wait(sleep);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }, "cluster-monitor");
        pollingThread.start();
    }

    public void stop() {
        if (pollingThread != null) {
            pollingThread.interrupt();
        }
        scheduler.shutdownNow();
        rmiPool.shutdownNow();
    }

    /**
     * One monitoring cycle: statuses and leader views first, then telemetry. Pulling after
     * the views are read guarantees that the LEADER_ACCEPTED behind every view is merged.
     */
    public synchronized void pollAll() {
        long cycleStartMs = System.currentTimeMillis();
        for (int id : config.getNodeIds()) {
            try {
                pollNode(id);
            } catch (Exception ignored) {
            }
        }
        List<EventLog.Event> merged = pullTelemetry();
        electionTracker.onCycle(getSnapshot(), merged, desiredAlgorithm, cycleStartMs);
        JobDirectory jd = jobDirectory;
        if (jd != null) {
            Object leader = electionTracker.toMap().get("leaderId");
            jd.onCycle(getSnapshot(), leader instanceof Integer ? (Integer) leader : null);
        }
    }

    public NodeStatus pollNode(int nodeId) {
        ClusterConfig.NodeConfig nc = config.getNode(nodeId);
        int port = nc.getPort();

        boolean up = false;
        int poolSize = nc.getPoolSize();
        Integer queueDepth = null;
        Long lamport = null;
        Long clockOffsetMs = null;
        List<String> bindings = new ArrayList<>();
        Integer leaderView = null;
        Boolean electing = null;
        String algorithm = null;
        NodeAgent agent = null;
        NodeElection election = null;

        try {
            // 1. Registry connection and listing
            Registry registry = callWithTimeout(() -> LocateRegistry.getRegistry("localhost", port));
            String[] list = callWithTimeout(registry::list);
            bindings = Arrays.asList(list);

            // 2. NodeAgent lookup & metrics
            NodeAgent a = callWithTimeout(() -> (NodeAgent) registry.lookup("agent"));
            agent = a;
            callWithTimeout(a::ping);
            poolSize = callWithTimeout(a::getPoolSize);
            queueDepth = callWithTimeout(a::getQueueDepth);
            lamport = callWithTimeout(a::getLamportTime);

            // 3. TimeService lookup with RTT midpoint calculation
            TimeService timeService = callWithTimeout(() -> (TimeService) registry.lookup("time"));
            long t0 = System.currentTimeMillis();
            long nodeTime = callWithTimeout(timeService::getTime);
            long t1 = System.currentTimeMillis();
            long rtt = t1 - t0;
            long coordinatorMid = t0 + (rtt / 2);
            clockOffsetMs = nodeTime - coordinatorMid;

            up = true;

            // 4. Election view (a failure here leaves the node UP with an unknown view)
            try {
                NodeElection el = callWithTimeout(() -> (NodeElection) registry.lookup("election-node-" + nodeId));
                election = el;
                int view = callWithTimeout(el::getLeaderId);
                leaderView = view < 0 ? null : view;
                electing = callWithTimeout(el::isElecting);
                algorithm = callWithTimeout(el::getAlgorithm);
            } catch (Exception ignored) {
            }
        } catch (Exception e) {
            up = false;
        }

        // State change detection
        Boolean previousState = lastKnownState.put(nodeId, up);
        if (previousState != null) {
            if (!previousState && up) {
                eventLog.record("NODE_UP_DETECTED", nodeId,
                        "Node " + nodeId + " (port " + port + ") is online and responding", System.currentTimeMillis());
            } else if (previousState && !up) {
                eventLog.record("NODE_DOWN_DETECTED", nodeId,
                        "Node " + nodeId + " (port " + port + ") is unreachable", System.currentTimeMillis());
            }
        }
        if (up && (previousState == null || !previousState)) {
            // A node seen for the first time: align its algorithm, then sync its Lamport
            // clock. The first sync() opens the node's boot gate for its first election.
            if (election != null) {
                NodeElection el = election;
                try {
                    callWithTimeout(() -> {
                        el.setAlgorithm(desiredAlgorithm);
                        return null;
                    });
                    algorithm = desiredAlgorithm;
                } catch (Exception ignored) {
                }
            }
            // Align the auto clock-sync setting too (the node also got it at launch).
            boolean auto = desiredAutoSync;
            try {
                Registry registry = callWithTimeout(() -> LocateRegistry.getRegistry("localhost", port));
                ClockCoordinatorService clock = callWithTimeout(() -> (ClockCoordinatorService) registry.lookup("clock"));
                callWithTimeout(() -> {
                    clock.setAutoSync(auto);
                    return null;
                });
            } catch (Exception ignored) {
            }
            syncNode(agent);
            expectElectionActivity(5000);
        }

        NodeStatus status = new NodeStatus(nodeId, port, up, poolSize, queueDepth, lamport, clockOffsetMs,
                bindings, leaderView, electing, algorithm);
        latestStatuses.put(nodeId, status);
        return status;
    }

    /**
     * A node's process was just replaced. Its next UP poll is treated as a first sighting
     * (algorithm and auto-sync aligned, first sync() sent) even if no poll saw it DOWN: a kill
     * and restart between two polls otherwise left the new process without its first sync().
     */
    public void markRestarted(int nodeId) {
        lastKnownState.put(nodeId, false);
    }

    /** Sends the control plane's Lamport time to a node (bounded by the RMI timeout). */
    public void syncNode(NodeAgent agent) {
        if (agent == null) {
            return;
        }
        long lamport = eventLog.getLamportClock().getTime();
        try {
            callWithTimeout(() -> agent.sync(lamport));
        } catch (Exception ignored) {
        }
    }

    /** Syncs every node that is currently UP with the control plane's Lamport time. */
    public void syncAllUp() {
        for (NodeStatus s : getSnapshot()) {
            if (!s.isUp()) {
                continue;
            }
            try {
                Registry registry = callWithTimeout(() -> LocateRegistry.getRegistry("localhost", s.getPort()));
                NodeAgent agent = callWithTimeout(() -> (NodeAgent) registry.lookup("agent"));
                syncNode(agent);
            } catch (Exception ignored) {
            }
        }
    }

    /** Looks up a node's election service with the RMI timeout. */
    public NodeElection electionOf(int nodeId) throws Exception {
        int port = config.getNode(nodeId).getPort();
        Registry registry = callWithTimeout(() -> LocateRegistry.getRegistry("localhost", port));
        return callWithTimeout(() -> (NodeElection) registry.lookup("election-node-" + nodeId));
    }

    public <T> T call(Callable<T> callable) throws Exception {
        return callWithTimeout(callable);
    }

    /** Like call(), with a caller-chosen timeout (for STRONG writes and Berkeley rounds). */
    public <T> T call(Callable<T> callable, long timeoutMs) throws Exception {
        Future<T> future = rmiPool.submit(callable);
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException("RMI call timed out after " + timeoutMs + " ms", e);
        } catch (Exception e) {
            future.cancel(true);
            throw e;
        }
    }

    /** A remote object bound in a node's registry, looked up with the RMI timeout. */
    public <T> T lookup(int nodeId, String name, Class<T> type) throws Exception {
        int port = config.getNode(nodeId).getPort();
        Registry registry = callWithTimeout(() -> LocateRegistry.getRegistry("localhost", port));
        return type.cast(callWithTimeout(() -> registry.lookup(name)));
    }

    private List<EventLog.Event> pullTelemetry() {
        List<NodeEvent> pulled = new ArrayList<>();
        for (NodeStatus s : getSnapshot()) {
            if (!s.isUp()) {
                continue;
            }
            try {
                Registry registry = callWithTimeout(() -> LocateRegistry.getRegistry("localhost", s.getPort()));
                NodeTelemetry telemetry = callWithTimeout(() -> (NodeTelemetry) registry.lookup("telemetry"));
                long[] cursor = telemetryCursors.getOrDefault(s.getId(), new long[] {0L, 0L});
                List<NodeEvent> events;
                if (cursor[1] == 0) {
                    events = callWithTimeout(() -> telemetry.getEvents(0L));
                } else {
                    // Re-read the last event already merged as an anchor. If it comes back
                    // unchanged, this is the same process; otherwise the node was restarted
                    // (its seq numbers started again, possibly reaching the old cursor value),
                    // so its buffer is read from the start.
                    List<NodeEvent> got = callWithTimeout(() -> telemetry.getEvents(cursor[1] - 1));
                    boolean anchored = !got.isEmpty()
                            && got.get(0).getIncarnation() == cursor[0]
                            && got.get(0).getSeq() == cursor[1];
                    if (anchored) {
                        events = got.subList(1, got.size());
                    } else {
                        events = new ArrayList<>();
                        for (NodeEvent e : callWithTimeout(() -> telemetry.getEvents(0L))) {
                            if (e.getIncarnation() != cursor[0] || e.getSeq() > cursor[1]) {
                                events.add(e);
                            }
                        }
                    }
                }
                if (!events.isEmpty()) {
                    NodeEvent last = events.get(events.size() - 1);
                    telemetryCursors.put(s.getId(), new long[] {last.getIncarnation(), last.getSeq()});
                    pulled.addAll(events);
                }
            } catch (Exception ignored) {
            }
        }
        if (pulled.isEmpty()) {
            return Collections.emptyList();
        }
        return eventLog.mergeNodeEvents(pulled);
    }

    private <T> T callWithTimeout(Callable<T> callable) throws Exception {
        Future<T> future = rmiPool.submit(callable);
        try {
            return future.get(RMI_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException("RMI call timed out after " + RMI_TIMEOUT_MS + " ms", e);
        } catch (Exception e) {
            future.cancel(true);
            throw e;
        }
    }

    public List<NodeStatus> getSnapshot() {
        List<NodeStatus> list = new ArrayList<>();
        for (int id : config.getNodeIds()) {
            NodeStatus s = latestStatuses.get(id);
            if (s != null) {
                list.add(s);
            } else {
                ClusterConfig.NodeConfig nc = config.getNode(id);
                list.add(new NodeStatus(id, nc.getPort(), false, nc.getPoolSize(), null, null, null, Collections.emptyList()));
            }
        }
        return list;
    }

    public List<Map<String, Object>> getSnapshotAsMaps() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (NodeStatus s : getSnapshot()) {
            result.add(s.toMap());
        }
        return result;
    }
}
