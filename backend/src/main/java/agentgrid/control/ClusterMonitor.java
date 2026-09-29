package agentgrid.control;

import agentgrid.clock.TimeService;
import agentgrid.node.ClusterConfig;
import agentgrid.node.NodeAgent;

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
 */
public class ClusterMonitor {

    private static final long RMI_TIMEOUT_MS = 700L;

    public static class NodeStatus {
        private final int id;
        private final int port;
        private final boolean up;
        private final int poolSize;
        private final int queueDepth;
        private final long lamport;
        private final long clockOffsetMs;
        private final List<String> bindings;

        public NodeStatus(
                int id,
                int port,
                boolean up,
                int poolSize,
                int queueDepth,
                long lamport,
                long clockOffsetMs,
                List<String> bindings) {
            this.id = id;
            this.port = port;
            this.up = up;
            this.poolSize = poolSize;
            this.queueDepth = queueDepth;
            this.lamport = lamport;
            this.clockOffsetMs = clockOffsetMs;
            this.bindings = bindings != null ? bindings : Collections.emptyList();
        }

        public int getId() { return id; }
        public int getPort() { return port; }
        public boolean isUp() { return up; }
        public int getPoolSize() { return poolSize; }
        public int getQueueDepth() { return queueDepth; }
        public long getLamport() { return lamport; }
        public long getClockOffsetMs() { return clockOffsetMs; }
        public List<String> getBindings() { return bindings; }

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
            return m;
        }
    }

    private final ClusterConfig config;
    private final EventLog eventLog;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService rmiPool = Executors.newCachedThreadPool();

    private final Map<Integer, NodeStatus> latestStatuses = new ConcurrentHashMap<>();
    private final Map<Integer, Boolean> lastKnownState = new ConcurrentHashMap<>();

    public ClusterMonitor(ClusterConfig config, EventLog eventLog) {
        this.config = config;
        this.eventLog = eventLog;
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::pollAll, 0, 1000, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
        rmiPool.shutdownNow();
    }

    public void pollAll() {
        for (int id : config.getNodeIds()) {
            try {
                pollNode(id);
            } catch (Exception ignored) {
            }
        }
    }

    public NodeStatus pollNode(int nodeId) {
        ClusterConfig.NodeConfig nc = config.getNode(nodeId);
        int port = nc.getPort();

        boolean up = false;
        int poolSize = nc.getPoolSize();
        int queueDepth = 0;
        long lamport = 0;
        long clockOffsetMs = nc.getClockDriftMs();
        List<String> bindings = new ArrayList<>();

        try {
            // 1. Registry connection and listing
            Registry registry = callWithTimeout(() -> LocateRegistry.getRegistry("localhost", port));
            String[] list = callWithTimeout(registry::list);
            bindings = Arrays.asList(list);

            // 2. NodeAgent lookup & metrics
            NodeAgent agent = callWithTimeout(() -> (NodeAgent) registry.lookup("agent"));
            callWithTimeout(agent::ping);
            poolSize = callWithTimeout(agent::getPoolSize);
            queueDepth = callWithTimeout(agent::getQueueDepth);
            lamport = callWithTimeout(agent::getLamportTime);

            // 3. TimeService lookup with RTT midpoint calculation
            TimeService timeService = callWithTimeout(() -> (TimeService) registry.lookup("time"));
            long t0 = System.currentTimeMillis();
            long nodeTime = callWithTimeout(timeService::getTime);
            long t1 = System.currentTimeMillis();
            long rtt = t1 - t0;
            long coordinatorMid = t0 + (rtt / 2);
            clockOffsetMs = nodeTime - coordinatorMid;

            up = true;
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

        NodeStatus status = new NodeStatus(nodeId, port, up, poolSize, queueDepth, lamport, clockOffsetMs, bindings);
        latestStatuses.put(nodeId, status);
        return status;
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
                list.add(new NodeStatus(id, nc.getPort(), false, nc.getPoolSize(), 0, 0, nc.getClockDriftMs(), Collections.emptyList()));
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
