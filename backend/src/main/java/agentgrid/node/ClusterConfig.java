package agentgrid.node;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Cluster configuration loaded from cluster.properties.
 * Defines node IDs, ports, worker pool sizes, and simulated physical clock drift offsets.
 */
public class ClusterConfig {

    public static class NodeConfig {
        private final int id;
        private final int port;
        private final int poolSize;
        private final long clockDriftMs;

        public NodeConfig(int id, int port, int poolSize, long clockDriftMs) {
            this.id = id;
            this.port = port;
            this.poolSize = poolSize;
            this.clockDriftMs = clockDriftMs;
        }

        public int getId() {
            return id;
        }

        public int getPort() {
            return port;
        }

        public int getPoolSize() {
            return poolSize;
        }

        public long getClockDriftMs() {
            return clockDriftMs;
        }

        @Override
        public String toString() {
            return "NodeConfig{id=" + id
                    + ", port=" + port
                    + ", poolSize=" + poolSize
                    + ", clockDriftMs=" + clockDriftMs + "}";
        }
    }

    /** Default per-subtask simulated work delay; see getSimulatedWorkMs(). */
    public static final long DEFAULT_SIMULATED_WORK_MS = 150L;

    /** Default simulated propagation lag of EVENTUAL blackboard writes. */
    public static final long DEFAULT_EVENTUAL_LAG_MS = 400L;
    /** Default interval between blackboard anti-entropy pulls. */
    public static final long DEFAULT_ANTI_ENTROPY_MS = 2000L;
    /** Default interval of the leader's periodic Berkeley round (0 disables it). */
    public static final long DEFAULT_BERKELEY_INTERVAL_MS = 30000L;

    private final Map<Integer, NodeConfig> nodes;
    private final List<Integer> nodeIds;
    private final long simulatedWorkMs;
    private final long eventualLagMs;
    private final long antiEntropyMs;
    private final long berkeleyIntervalMs;

    public ClusterConfig(Map<Integer, NodeConfig> nodes) {
        this(nodes, DEFAULT_SIMULATED_WORK_MS);
    }

    public ClusterConfig(Map<Integer, NodeConfig> nodes, long simulatedWorkMs) {
        this(nodes, simulatedWorkMs, DEFAULT_EVENTUAL_LAG_MS, DEFAULT_ANTI_ENTROPY_MS, DEFAULT_BERKELEY_INTERVAL_MS);
    }

    public ClusterConfig(Map<Integer, NodeConfig> nodes, long simulatedWorkMs, long eventualLagMs,
                         long antiEntropyMs, long berkeleyIntervalMs) {
        this.nodes = Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
        this.nodeIds = Collections.unmodifiableList(new ArrayList<>(nodes.keySet()));
        this.simulatedWorkMs = simulatedWorkMs;
        this.eventualLagMs = Math.max(0, eventualLagMs);
        this.antiEntropyMs = Math.max(0, antiEntropyMs);
        this.berkeleyIntervalMs = Math.max(0, berkeleyIntervalMs);
    }

    /**
     * SIMULATED delay before an EVENTUAL blackboard write is pushed to peers (property
     * "eventualLagMs"). On one machine replication takes about a millisecond, so without it
     * the staleness window of eventual consistency would be invisible.
     */
    public long getEventualLagMs() {
        return eventualLagMs;
    }

    /** Interval of each node's blackboard anti-entropy pull (property "blackboardAntiEntropyMs", 0 disables). */
    public long getAntiEntropyMs() {
        return antiEntropyMs;
    }

    /** Interval of the leader's periodic Berkeley round (property "berkeleyIntervalMs", 0 disables). */
    public long getBerkeleyIntervalMs() {
        return berkeleyIntervalMs;
    }

    /** Nodes needed for a live majority: floor(n / 2) + 1 (3 of 5). */
    public int majority() {
        return nodes.size() / 2 + 1;
    }

    /**
     * Delay every node adds to each subtask (property "simulatedWorkMs", overridable with
     * -Dagentgrid.simulatedWorkMs). The real stage work is sub-millisecond, so without it
     * the thread-pool (Exp 2) and load-balancing (Exp 6) effects would not be measurable.
     */
    public long getSimulatedWorkMs() {
        return simulatedWorkMs;
    }

    public NodeConfig getNode(int nodeId) {
        NodeConfig config = nodes.get(nodeId);
        if (config == null) {
            throw new IllegalArgumentException(
                    "Unknown node ID: " + nodeId + ". Configured node IDs: " + nodes.keySet());
        }
        return config;
    }

    public Map<Integer, NodeConfig> getNodes() {
        return nodes;
    }

    public List<Integer> getNodeIds() {
        return nodeIds;
    }

    /**
     * Map of nodeId -> port for BullyElectionNode.
     */
    public Map<Integer, Integer> getNodePortMap() {
        Map<Integer, Integer> portMap = new LinkedHashMap<>();
        for (NodeConfig node : nodes.values()) {
            portMap.put(node.getId(), node.getPort());
        }
        return portMap;
    }

    /**
     * Map of peerId ("node-<id>") -> port for ReplicatedBlackboardNode.
     */
    public Map<String, Integer> getPeerPortMap() {
        Map<String, Integer> portMap = new LinkedHashMap<>();
        for (NodeConfig node : nodes.values()) {
            portMap.put("node-" + node.getId(), node.getPort());
        }
        return portMap;
    }

    /**
     * Loads cluster configuration from system properties, well-known filesystem paths, or classpath.
     */
    public static ClusterConfig load() throws IOException {
        String customPath = System.getProperty("agentgrid.cluster.config");
        if (customPath != null && !customPath.isBlank()) {
            Path p = Path.of(customPath);
            if (Files.exists(p)) {
                return load(p);
            }
        }

        Path[] candidates = new Path[] {
            Path.of("cluster.properties"),
            Path.of("backend", "cluster.properties"),
            Path.of("..", "cluster.properties"),
            Path.of("..", "backend", "cluster.properties")
        };

        for (Path p : candidates) {
            if (Files.exists(p)) {
                return load(p);
            }
        }

        try (InputStream in = ClusterConfig.class.getResourceAsStream("/cluster.properties")) {
            if (in != null) {
                Properties props = new Properties();
                props.load(in);
                return fromProperties(props);
            }
        }

        throw new IOException("Could not find cluster.properties in working directory, backend/, or classpath");
    }

    public static ClusterConfig load(Path path) throws IOException {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            props.load(in);
        }
        return fromProperties(props);
    }

    public static ClusterConfig fromProperties(Properties props) {
        Map<Integer, NodeConfig> nodes = new LinkedHashMap<>();
        String idsStr = props.getProperty("node.ids");
        List<Integer> ids = new ArrayList<>();

        if (idsStr != null && !idsStr.isBlank()) {
            for (String part : idsStr.split(",")) {
                ids.add(Integer.parseInt(part.trim()));
            }
        } else {
            for (String key : props.stringPropertyNames()) {
                if (key.startsWith("node.") && key.endsWith(".port")) {
                    String[] parts = key.split("\\.");
                    if (parts.length == 3) {
                        try {
                            ids.add(Integer.parseInt(parts[1]));
                        } catch (NumberFormatException ignored) {
                        }
                    }
                }
            }
            Collections.sort(ids);
        }

        for (int id : ids) {
            String portStr = props.getProperty("node." + id + ".port");
            if (portStr == null) {
                throw new IllegalArgumentException("Missing property node." + id + ".port in configuration");
            }
            int port = Integer.parseInt(portStr.trim());
            int poolSize = Integer.parseInt(props.getProperty("node." + id + ".poolSize", "4").trim());
            long clockDriftMs = Long.parseLong(props.getProperty("node." + id + ".clockDriftMs", "0").trim());
            nodes.put(id, new NodeConfig(id, port, poolSize, clockDriftMs));
        }

        String work = System.getProperty("agentgrid.simulatedWorkMs",
                props.getProperty("simulatedWorkMs", String.valueOf(DEFAULT_SIMULATED_WORK_MS)));
        return new ClusterConfig(nodes, Long.parseLong(work.trim()),
                longProperty(props, "eventualLagMs", DEFAULT_EVENTUAL_LAG_MS),
                longProperty(props, "blackboardAntiEntropyMs", DEFAULT_ANTI_ENTROPY_MS),
                longProperty(props, "berkeleyIntervalMs", DEFAULT_BERKELEY_INTERVAL_MS));
    }

    private static long longProperty(Properties props, String name, long fallback) {
        String v = System.getProperty("agentgrid." + name, props.getProperty(name));
        return v == null || v.isBlank() ? fallback : Long.parseLong(v.trim());
    }
}
