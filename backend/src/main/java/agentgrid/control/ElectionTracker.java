package agentgrid.control;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Derives the cluster's election state from polled leader views and merged node events.
 *
 * Views, agreement and backup come from the latest poll. Convergence is measured here,
 * not inside the election code: an episode runs from the first ELECTION_STARTED after the
 * last stable state through the last LEADER_ACCEPTED, using the machine clock (trueMs).
 * An episode is closed on the first poll cycle that sees a stable cluster (all UP nodes
 * agree on one UP leader and none is electing) and pulls no new election events, or,
 * if stability is lost again before that, at the last cycle that observed stability.
 */
public class ElectionTracker {

    public static final Set<String> ELECTION_TYPES = Set.of(
            "ELECTION_STARTED", "ELECTION_MSG", "LEADER_ACCEPTED", "LEADER_LOST", "HEARTBEAT_MISS");

    private final List<EventLog.Event> pending = new ArrayList<>();
    private long lastClosedAtMs = 0;
    private long lastStableAtMs = 0;
    private Integer lastStableLeader = null;
    private boolean wasStable = false;
    private Map<String, Object> lastEpisode = null;
    private Map<String, Object> state = null;
    private volatile boolean active = false;

    /** True while an election is visible: views disagree, a node is electing, or an episode is open. */
    public boolean isActive() {
        return active;
    }

    /**
     * @param cycleStartMs machine time at which this cycle started reading node statuses.
     *                     Every event an UP node recorded before it has been pulled by the
     *                     end of the cycle, so it is the boundary of a closed episode.
     */
    public synchronized void onCycle(List<ClusterMonitor.NodeStatus> statuses, List<EventLog.Event> merged,
                                     String configuredAlgorithm, long cycleStartMs) {
        boolean newElectionEvents = false;
        for (EventLog.Event e : merged) {
            if (ELECTION_TYPES.contains(e.getType())) {
                newElectionEvents = true;
                if (e.getTrueMs() > lastClosedAtMs) {
                    pending.add(e);
                }
            }
        }

        Map<String, Object> s = computeState(statuses, configuredAlgorithm);
        boolean agreed = (Boolean) s.get("agreed");
        boolean anyElecting = false;
        for (ClusterMonitor.NodeStatus st : statuses) {
            if (st.isUp() && Boolean.TRUE.equals(st.getElecting())) {
                anyElecting = true;
            }
        }
        boolean stable = agreed && !anyElecting;

        if (stable) {
            lastStableAtMs = cycleStartMs;
            lastStableLeader = (Integer) s.get("leaderId");
            if (!newElectionEvents) {
                if (pending.stream().anyMatch(e -> e.getType().equals("ELECTION_STARTED"))) {
                    lastEpisode = buildEpisode(pending, lastStableLeader);
                }
                pending.clear();
                // Not "now": a node read as DOWN in this cycle may have booted and recorded
                // events since; those belong to the next episode and must not be filtered out.
                lastClosedAtMs = cycleStartMs;
            }
        } else if (wasStable && !pending.isEmpty()) {
            // Stability was lost before the previous episode went quiet (e.g. the leader was
            // killed while the last election's final messages were still being pulled). The
            // previous episode ends at the last stable observation; later events start the
            // new one ("first ELECTION_STARTED after the last stable state").
            List<EventLog.Event> previous = new ArrayList<>();
            List<EventLog.Event> rest = new ArrayList<>();
            for (EventLog.Event e : pending) {
                (e.getTrueMs() <= lastStableAtMs ? previous : rest).add(e);
            }
            if (previous.stream().anyMatch(e -> e.getType().equals("ELECTION_STARTED"))) {
                lastEpisode = buildEpisode(previous, lastStableLeader);
            }
            pending.clear();
            pending.addAll(rest);
            lastClosedAtMs = lastStableAtMs;
        }
        wasStable = stable;
        active = !stable || !pending.isEmpty();
        state = s;
    }

    private Map<String, Object> computeState(List<ClusterMonitor.NodeStatus> statuses, String configuredAlgorithm) {
        Map<String, Object> views = new LinkedHashMap<>();
        Set<Integer> up = new java.util.TreeSet<>();
        Integer common = null;
        boolean agreed = true;
        boolean anyUp = false;
        Set<String> algorithms = new java.util.TreeSet<>();
        for (ClusterMonitor.NodeStatus st : statuses) {
            Integer view = st.isUp() ? st.getLeaderView() : null;
            views.put(String.valueOf(st.getId()), view);
            if (!st.isUp()) {
                continue;
            }
            anyUp = true;
            up.add(st.getId());
            if (st.getAlgorithm() != null) {
                algorithms.add(st.getAlgorithm());
            }
            if (view == null) {
                agreed = false;
            } else if (common == null) {
                common = view;
            } else if (!common.equals(view)) {
                agreed = false;
            }
        }
        agreed = agreed && anyUp && common != null && up.contains(common);
        Integer leaderId = agreed ? common : null;
        Integer backupId = null;
        if (leaderId != null) {
            for (int id : up) {
                if (id < leaderId) {
                    backupId = id;
                }
            }
        }
        String algorithm = algorithms.size() == 1 ? algorithms.iterator().next()
                : algorithms.isEmpty() ? configuredAlgorithm : "MIXED";

        Map<String, Object> s = new LinkedHashMap<>();
        s.put("algorithm", algorithm);
        s.put("views", views);
        s.put("agreed", agreed);
        s.put("leaderId", leaderId);
        s.put("backupId", backupId);
        return s;
    }

    private static Map<String, Object> buildEpisode(List<EventLog.Event> events, Integer leaderId) {
        long start = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;
        List<Integer> initiators = new ArrayList<>();
        for (EventLog.Event e : events) {
            if (e.getType().equals("ELECTION_STARTED")) {
                start = Math.min(start, e.getTrueMs());
                if (!initiators.contains(e.getNode())) {
                    initiators.add(e.getNode());
                }
            }
        }
        for (EventLog.Event e : events) {
            if (e.getType().equals("LEADER_ACCEPTED") && e.getTrueMs() >= start) {
                end = Math.max(end, e.getTrueMs());
            }
        }
        if (end == Long.MIN_VALUE) {
            end = start;
        }

        List<EventLog.Event> msgs = new ArrayList<>();
        for (EventLog.Event e : events) {
            if (e.getType().equals("ELECTION_MSG") && e.getTrueMs() >= start && e.getTrueMs() <= end) {
                msgs.add(e);
            }
        }
        msgs.sort((a, b) -> a.getLamport() != b.getLamport() ? Long.compare(a.getLamport(), b.getLamport())
                : a.getNode() != b.getNode() ? Integer.compare(a.getNode(), b.getNode())
                : Long.compare(nz(a.getNodeSeq()), nz(b.getNodeSeq())));

        Map<String, Integer> byKind = new TreeMap<>();
        List<Map<String, Object>> table = new ArrayList<>();
        for (EventLog.Event m : msgs) {
            String kind = String.valueOf(m.getFields().get("kind"));
            byKind.merge(kind, 1, Integer::sum);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("seq", m.getSeq());
            row.put("kind", kind);
            row.put("from", m.getFields().get("from"));
            row.put("to", m.getFields().get("to"));
            row.put("lamport", m.getLamport());
            row.put("trueMs", m.getTrueMs());
            row.put("offsetMs", m.getTrueMs() - start);
            table.add(row);
        }

        Map<String, Object> ep = new LinkedHashMap<>();
        ep.put("startTrueMs", start);
        ep.put("endTrueMs", end);
        ep.put("convergenceMs", end - start);
        ep.put("messages", msgs.size());
        ep.put("byKind", byKind);
        ep.put("leaderId", leaderId);
        ep.put("initiators", initiators);
        ep.put("table", table);
        return ep;
    }

    private static long nz(Long v) {
        return v == null ? 0 : v;
    }

    public synchronized Map<String, Object> toMap() {
        Map<String, Object> out = new LinkedHashMap<>();
        if (state == null) {
            out.put("algorithm", null);
            out.put("views", new LinkedHashMap<>());
            out.put("agreed", false);
            out.put("leaderId", null);
            out.put("backupId", null);
        } else {
            out.putAll(state);
        }
        out.put("lastEpisode", lastEpisode);
        return out;
    }
}
