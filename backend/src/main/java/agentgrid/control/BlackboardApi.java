package agentgrid.control;

import agentgrid.node.BlackboardMetrics;
import agentgrid.node.BlackboardRecord;
import agentgrid.node.BlackboardState;
import agentgrid.node.BlackboardWriteOutcome;
import agentgrid.node.ClusterBlackboardService;
import agentgrid.node.ClusterConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Control-plane side of the replicated blackboard: writes and reads through a chosen node,
 * a per-node replica overview, and cluster-wide metrics.
 *
 * Stale reads: the control plane remembers the last value it wrote and got acknowledged for
 * each key (its own writes are single-writer and sequential). A read that returns a different
 * value or nothing is stale; a read from a replica that is not ready is reported as NOT_READY
 * and counted neither stale nor fresh. Keys under lww/ (conflict demos) are not checked.
 */
public class BlackboardApi {

    public static final String UNCHECKED_PREFIX = "lww/";
    private static final int MAX_ROWS_PER_NODE = 300;
    private static final long WRITE_TIMEOUT_MS = 6000;
    private static final long READ_TIMEOUT_MS = 2000;

    private final ClusterConfig config;
    private final ClusterMonitor monitor;
    private final Map<String, String> lastAcked = new ConcurrentHashMap<>();
    private final AtomicLong staleReads = new AtomicLong();
    private final AtomicLong freshReads = new AtomicLong();
    private final AtomicLong notReadyReads = new AtomicLong();
    private final AtomicLong uncheckedReads = new AtomicLong();

    public BlackboardApi(ClusterConfig config, ClusterMonitor monitor) {
        this.config = config;
        this.monitor = monitor;
    }

    private ClusterBlackboardService replica(int node) throws JobDirectory.ApiException {
        if (!config.getNodeIds().contains(node)) {
            throw new JobDirectory.ApiException(400, "Unknown node ID: " + node);
        }
        try {
            return monitor.lookup(node, "blackboard-node-node-" + node, ClusterBlackboardService.class);
        } catch (Exception e) {
            throw new JobDirectory.ApiException(503, "Node " + node + " is unreachable");
        }
    }

    public Map<String, Object> write(int node, String key, String value, String mode) throws JobDirectory.ApiException {
        ClusterBlackboardService bb = replica(node);
        BlackboardWriteOutcome outcome;
        try {
            outcome = monitor.call(() -> bb.write(key, value, mode), WRITE_TIMEOUT_MS);
        } catch (Exception e) {
            throw new JobDirectory.ApiException(503, "Write through node " + node + " failed: " + root(e));
        }
        if (outcome.getStatus().stored() && !key.startsWith(UNCHECKED_PREFIX)) {
            lastAcked.put(key, value);
        }
        return outcome.toMap();
    }

    public Map<String, Object> read(int node, String key) throws JobDirectory.ApiException {
        ClusterBlackboardService bb = replica(node);
        BlackboardState state;
        try {
            state = monitor.call(() -> bb.read(key), READ_TIMEOUT_MS);
        } catch (Exception e) {
            throw new JobDirectory.ApiException(503, "Read from node " + node + " failed: " + root(e));
        }
        BlackboardRecord r = state.record();
        String expected = lastAcked.get(key);
        String verdict;
        if (!state.isReady()) {
            verdict = "NOT_READY";
            notReadyReads.incrementAndGet();
        } else if (key.startsWith(UNCHECKED_PREFIX) || expected == null) {
            verdict = "UNCHECKED";
            uncheckedReads.incrementAndGet();
        } else if (r != null && expected.equals(r.getValue())) {
            verdict = "FRESH";
            freshReads.incrementAndGet();
        } else {
            verdict = "STALE";
            staleReads.incrementAndGet();
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("node", node);
        m.put("key", key);
        m.put("ready", state.isReady());
        m.put("clockSynced", state.isClockSynced());
        m.put("found", r != null);
        m.put("value", r == null ? null : r.getValue());
        m.put("timestamp", r == null ? null : r.getTimestamp());
        m.put("writer", r == null ? null : r.getWriterNodeId());
        m.put("version", r == null ? null : r.getVersion());
        m.put("verdict", verdict);
        return m;
    }

    /** Every replica's records (keys starting with prefix), each marked stale if another replica holds a newer one. */
    public Map<String, Object> overview(String prefix) {
        Map<Integer, BlackboardState> states = new TreeMap<>();
        List<Map<String, Object>> nodes = new ArrayList<>();
        for (ClusterMonitor.NodeStatus s : monitor.getSnapshot()) {
            if (!s.isUp()) {
                continue;
            }
            try {
                ClusterBlackboardService bb = replica(s.getId());
                states.put(s.getId(), monitor.call(() -> bb.snapshot(prefix), READ_TIMEOUT_MS));
            } catch (Exception ignored) {
                // shown as unreachable below
            }
        }
        Map<String, BlackboardRecord> winners = new HashMap<>();
        for (BlackboardState st : states.values()) {
            for (BlackboardRecord r : st.getRecords()) {
                winners.merge(r.getKey(), r, BlackboardRecord::merge);
            }
        }
        for (ClusterMonitor.NodeStatus s : monitor.getSnapshot()) {
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("node", s.getId());
            n.put("up", s.isUp());
            BlackboardState st = states.get(s.getId());
            n.put("reachable", st != null);
            if (st != null) {
                n.putAll(st.header());
                List<Map<String, Object>> rows = new ArrayList<>();
                int stale = 0;
                for (BlackboardRecord r : st.getRecords()) {
                    boolean isStale = !r.sameStamp(winners.get(r.getKey()));
                    if (isStale) {
                        stale++;
                    }
                    if (rows.size() < MAX_ROWS_PER_NODE) {
                        Map<String, Object> row = r.toMap();
                        row.put("stale", isStale);
                        rows.add(row);
                    }
                }
                TreeSet<String> missing = new TreeSet<>(winners.keySet());
                for (BlackboardRecord r : st.getRecords()) {
                    missing.remove(r.getKey());
                }
                n.put("entryCount", st.getRecords().size());
                n.put("staleEntries", stale);
                n.put("missingKeys", missing.size());
                n.put("truncated", st.getRecords().size() > MAX_ROWS_PER_NODE);
                n.put("entries", rows);
            }
            nodes.add(n);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("prefix", prefix);
        agentgrid.node.Membership members = config.membership();
        out.put("majority", members.quorum());
        out.put("members", members.size());
        out.put("epoch", members.getEpoch());
        out.put("eventualLagMs", config.getEventualLagMs());
        out.put("eventualLagSimulated", true);
        out.put("autoSync", monitor.getDesiredAutoSync());
        out.put("keys", winners.size());
        out.put("nodes", nodes);
        return out;
    }

    public Map<String, Object> metrics() {
        long stored = 0;
        long degraded = 0;
        long refused = 0;
        long failedPartial = 0;
        long accepted = 0;
        long retries = 0;
        long antiEntropy = 0;
        int pending = 0;
        List<Long> strong = new ArrayList<>();
        List<Long> eventual = new ArrayList<>();
        Map<String, Object> perNode = new LinkedHashMap<>();
        for (ClusterMonitor.NodeStatus s : monitor.getSnapshot()) {
            if (!s.isUp()) {
                continue;
            }
            try {
                ClusterBlackboardService bb = replica(s.getId());
                BlackboardMetrics m = monitor.call(bb::metrics, READ_TIMEOUT_MS);
                stored += m.getStrongStored();
                degraded += m.getStrongDegraded();
                refused += m.getStrongRefused();
                failedPartial += m.getStrongFailedPartial();
                accepted += m.getEventualAccepted();
                retries += m.getEventualRetries();
                antiEntropy += m.getAntiEntropyApplied();
                pending += m.getEventualPending();
                strong.addAll(m.strongLatencySamples());
                eventual.addAll(m.eventualLatencySamples());
                Map<String, Object> n = new LinkedHashMap<>();
                n.put("eventualPending", m.getEventualPending());
                n.put("strongRefused", m.getStrongRefused());
                perNode.put(String.valueOf(s.getId()), n);
            } catch (Exception ignored) {
                // unreachable node: its counters are not included
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> strongMap = new LinkedHashMap<>();
        strongMap.put("stored", stored);
        strongMap.put("storedDegraded", degraded);
        strongMap.put("refused", refused);
        strongMap.put("failedPartial", failedPartial);
        strongMap.put("medianWriteLatencyMs", median(strong));
        strongMap.put("samples", strong.size());
        Map<String, Object> eventualMap = new LinkedHashMap<>();
        eventualMap.put("accepted", accepted);
        eventualMap.put("pendingPropagation", pending);
        eventualMap.put("retries", retries);
        eventualMap.put("antiEntropyApplied", antiEntropy);
        eventualMap.put("medianWriteLatencyMs", median(eventual));
        eventualMap.put("samples", eventual.size());
        Map<String, Object> reads = new LinkedHashMap<>();
        reads.put("stale", staleReads.get());
        reads.put("fresh", freshReads.get());
        reads.put("notReady", notReadyReads.get());
        reads.put("unchecked", uncheckedReads.get());
        out.put("strong", strongMap);
        out.put("eventual", eventualMap);
        out.put("reads", reads);
        out.put("perNode", perNode);
        out.put("latencyWindow", BlackboardMetrics.WINDOW + " most recent writes per node and mode");
        return out;
    }

    private static Long median(List<Long> values) {
        if (values.isEmpty()) {
            return null;
        }
        List<Long> s = new ArrayList<>(values);
        Collections.sort(s);
        return s.get(s.size() / 2);
    }

    private static String root(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getMessage() == null ? c.toString() : c.getMessage();
    }
}
