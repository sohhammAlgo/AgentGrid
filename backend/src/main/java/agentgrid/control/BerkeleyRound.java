package agentgrid.control;

import agentgrid.clock.TimeService;
import agentgrid.node.ClusterConfig;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes a single round of the Berkeley clock synchronization algorithm
 * with RTT-compensation over currently UP nodes.
 * Copies the core algorithm from BerkeleySyncCoordinator without modifying submitted classes.
 */
public class BerkeleyRound {

    public static class SyncResult {
        private final long spreadBefore;
        private final long spreadAfter;
        private final long averageOffset;
        private final int nodeCount;
        private final Map<Integer, Long> corrections;

        public SyncResult(
                long spreadBefore,
                long spreadAfter,
                long averageOffset,
                int nodeCount,
                Map<Integer, Long> corrections) {
            this.spreadBefore = spreadBefore;
            this.spreadAfter = spreadAfter;
            this.averageOffset = averageOffset;
            this.nodeCount = nodeCount;
            this.corrections = corrections;
        }

        public long getSpreadBefore() { return spreadBefore; }
        public long getSpreadAfter() { return spreadAfter; }
        public long getAverageOffset() { return averageOffset; }
        public int getNodeCount() { return nodeCount; }
        public Map<Integer, Long> getCorrections() { return corrections; }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("spreadBefore", spreadBefore);
            m.put("spreadAfter", spreadAfter);
            m.put("averageOffset", averageOffset);
            m.put("nodeCount", nodeCount);
            m.put("corrections", corrections);
            return m;
        }
    }

    private static class NodeHandle {
        final int nodeId;
        final TimeService service;

        NodeHandle(int nodeId, TimeService service) {
            this.nodeId = nodeId;
            this.service = service;
        }
    }

    private static class Reading {
        final NodeHandle node;
        final long rawTimeMillis;
        final long offsetMillis;
        final long roundTripMillis;

        Reading(NodeHandle node, long rawTimeMillis, long offsetMillis, long roundTripMillis) {
            this.node = node;
            this.rawTimeMillis = rawTimeMillis;
            this.offsetMillis = offsetMillis;
            this.roundTripMillis = roundTripMillis;
        }
    }

    public static SyncResult execute(ClusterConfig config, List<Integer> targetNodeIds) throws Exception {
        List<NodeHandle> handles = new ArrayList<>();

        for (int id : targetNodeIds) {
            ClusterConfig.NodeConfig nc = config.getNode(id);
            try {
                Registry registry = LocateRegistry.getRegistry("localhost", nc.getPort());
                TimeService service = (TimeService) registry.lookup("time");
                handles.add(new NodeHandle(id, service));
            } catch (Exception ignored) {
                // Skip unreachable nodes
            }
        }

        if (handles.isEmpty()) {
            throw new IllegalStateException("No active nodes available for clock synchronization");
        }

        // 1. Poll all nodes before sync
        List<Reading> before = pollAll(handles);
        long avgOffsetBefore = averageOffset(before);
        long spreadBefore = spread(before);

        // 2. Apply corrections
        Map<Integer, Long> corrections = new LinkedHashMap<>();
        for (Reading reading : before) {
            long correction = avgOffsetBefore - reading.offsetMillis;
            reading.node.service.adjustTime(correction);
            corrections.put(reading.node.nodeId, correction);
        }

        // 3. Poll all nodes after sync
        List<Reading> after = pollAll(handles);
        long spreadAfter = spread(after);

        return new SyncResult(spreadBefore, spreadAfter, avgOffsetBefore, handles.size(), corrections);
    }

    private static List<Reading> pollAll(List<NodeHandle> nodes) throws Exception {
        List<Reading> readings = new ArrayList<>();
        for (NodeHandle node : nodes) {
            long t0 = System.currentTimeMillis();
            long nodeTime = node.service.getTime();
            long t1 = System.currentTimeMillis();
            long rtt = t1 - t0;
            long coordinatorMid = t0 + (rtt / 2);
            readings.add(new Reading(node, nodeTime, nodeTime - coordinatorMid, rtt));
        }
        return readings;
    }

    private static long averageOffset(List<Reading> readings) {
        if (readings.isEmpty()) return 0;
        long total = 0L;
        for (Reading r : readings) {
            total += r.offsetMillis;
        }
        return total / readings.size();
    }

    private static long spread(List<Reading> readings) {
        if (readings.isEmpty()) return 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (Reading r : readings) {
            min = Math.min(min, r.offsetMillis);
            max = Math.max(max, r.offsetMillis);
        }
        return max - min;
    }
}
