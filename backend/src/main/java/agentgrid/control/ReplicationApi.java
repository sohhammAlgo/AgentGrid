package agentgrid.control;

import agentgrid.common.Subtask;
import agentgrid.node.ClusterConfig;
import agentgrid.node.NodeAgent;
import agentgrid.node.NodeProcessManager;
import agentgrid.orchestrator.OrchestratorService;
import agentgrid.orchestrator.ReplAck;
import agentgrid.orchestrator.ReplEvent;
import agentgrid.orchestrator.ReplState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The control plane's side of primary-backup replication (Exp 8): the cluster-wide ON/OFF setting
 * (default ON, pushed to every node and passed to nodes started later), the replication view
 * (primary, backup, term, seq, lag), and per-node log summaries for comparing replicas.
 *
 * The state itself lives in the nodes (agentgrid.orchestrator.Replicator); this only reads it
 * over RMI. A node that is down is reported as such, never guessed.
 */
public final class ReplicationApi {

    private static final long LOG_TIMEOUT_MS = 6000;

    /** Control flow of staleWrite: the event half is skipped on request. */
    private static final class SkipEvent extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SkipEvent() {
            super(null, null, false, false);
        }
    }

    private final ClusterConfig config;
    private final ClusterMonitor monitor;
    private final EventLog eventLog;
    private final NodeProcessManager processManager;
    private volatile boolean enabled = Boolean.parseBoolean(System.getProperty("agentgrid.primaryBackup", "true"));

    public ReplicationApi(ClusterConfig config, ClusterMonitor monitor, EventLog eventLog,
                          NodeProcessManager processManager) {
        this.config = config;
        this.monitor = monitor;
        this.eventLog = eventLog;
        this.processManager = processManager;
        this.processManager.setPrimaryBackup(enabled);
    }

    /** The cluster setting. JobDirectory reads it to decide between recovering and orphaning a job. */
    public boolean isEnabled() {
        return enabled;
    }

    private OrchestratorService orchestrator(int nodeId) throws Exception {
        return monitor.lookup(nodeId, "orchestrator", OrchestratorService.class);
    }

    private ReplState stateOf(int nodeId) {
        try {
            OrchestratorService o = orchestrator(nodeId);
            return monitor.call(o::replicationState);
        } catch (Exception e) {
            return null;
        }
    }

    // =========================================================================
    // GET /api/replication
    // =========================================================================

    public Map<String, Object> view() {
        Object leader = monitor.getElectionTracker().toMap().get("leaderId");
        Integer leaderId = leader instanceof Integer ? (Integer) leader : null;

        Map<Integer, ReplState> states = new LinkedHashMap<>();
        List<ClusterMonitor.NodeStatus> statuses = monitor.getSnapshot();
        for (ClusterMonitor.NodeStatus s : statuses) {
            if (s.isUp()) {
                ReplState st = stateOf(s.getId());
                if (st != null) {
                    states.put(s.getId(), st);
                }
            }
        }
        ReplState primary = leaderId == null ? null : states.get(leaderId);
        Integer backupId = primary == null ? null : primary.getBackupId();
        ReplState backup = backupId == null ? null : states.get(backupId);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("enabled", enabled);
        out.put("primary", leaderId);
        out.put("backup", backupId);
        out.put("term", primary == null ? null : primary.getTerm());
        out.put("lastSeq", primary == null ? null : primary.getLastSeq());
        out.put("backupLastSeq", backup == null ? null : backup.getLastSeq());
        out.put("lag", primary != null && backup != null ? primary.getLastSeq() - backup.getLastSeq() : null);
        boolean noBackup = enabled && primary != null && backupId == null;
        out.put("noBackup", noBackup);
        out.put("ready", primary == null || primary.isReady());
        String warning = null;
        if (!enabled) {
            warning = "replication is OFF: if the leader dies, its running jobs are ORPHANED";
        } else if (leaderId == null) {
            warning = "no agreed leader";
        } else if (primary == null) {
            warning = "the leader's replication state is unreachable";
        } else if (!primary.isReady()) {
            warning = "the new primary is catching up and replaying its log";
        } else if (noBackup) {
            warning = "no backup: jobs run unreplicated until a backup is chosen";
        }
        out.put("warning", warning);

        long staleTotal = 0;
        long recoveredTotal = 0;
        List<Map<String, Object>> nodes = new ArrayList<>();
        for (ClusterMonitor.NodeStatus s : statuses) {
            ReplState st = states.get(s.getId());
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("node", s.getId());
            n.put("up", s.isUp());
            String role = !s.isUp() ? "DOWN" : Integer.valueOf(s.getId()).equals(leaderId) ? "PRIMARY"
                    : Integer.valueOf(s.getId()).equals(backupId) ? "BACKUP" : "MEMBER";
            n.put("role", role);
            n.put("term", st == null ? null : st.getTerm());
            n.put("lastSeq", st == null ? null : st.getLastSeq());
            n.put("logEvents", st == null ? null : st.getLogEvents());
            n.put("staleRejected", st == null ? null : st.getStaleRejected());
            n.put("enabled", st == null ? null : st.isEnabled());
            if (st != null) {
                staleTotal += st.getStaleRejected();
                recoveredTotal += st.getRecoveredJobs();
            }
            nodes.add(n);
        }
        out.put("nodes", nodes);
        out.put("staleRejected", staleTotal);
        out.put("recoveredJobs", recoveredTotal);
        return out;
    }

    // =========================================================================
    // POST /api/replication/toggle
    // =========================================================================

    public Map<String, Object> toggle(boolean on) {
        enabled = on;
        processManager.setPrimaryBackup(on);
        List<Integer> applied = new ArrayList<>();
        List<Integer> failed = new ArrayList<>();
        for (ClusterMonitor.NodeStatus s : monitor.getSnapshot()) {
            if (!s.isUp()) {
                continue;
            }
            try {
                OrchestratorService o = orchestrator(s.getId());
                monitor.call(() -> {
                    o.setPrimaryBackup(on);
                    return null;
                });
                applied.add(s.getId());
            } catch (Exception e) {
                failed.add(s.getId());
            }
        }
        eventLog.record("REPLICATION_TOGGLED", 0, "Primary-backup replication " + (on ? "ON" : "OFF") + " on nodes " + applied
                + (failed.isEmpty() ? "" : " (failed: " + failed + ")"), System.currentTimeMillis());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("enabled", on);
        out.put("applied", applied);
        out.put("failed", failed);
        return out;
    }

    // =========================================================================
    // GET /api/replication/log?node=N&from=S
    // =========================================================================

    /** One node's log as summaries (seq, term, jobId, type, subtaskId, digest) with seq above from. */
    public Map<String, Object> log(int nodeId, long from) throws JobDirectory.ApiException {
        if (!config.getNodeIds().contains(nodeId)) {
            throw new JobDirectory.ApiException(404, "node " + nodeId + " is not a member (members " + config.getNodeIds() + ")");
        }
        try {
            OrchestratorService o = orchestrator(nodeId);
            List<ReplEvent> log = monitor.call(o::fetchLog, LOG_TIMEOUT_MS);
            ReplState st = monitor.call(o::replicationState);
            List<Map<String, Object>> events = new ArrayList<>();
            for (ReplEvent e : log) {
                if (e.getSeq() > from) {
                    events.add(e.toSummaryMap());
                }
            }
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("node", nodeId);
            out.put("term", st.getTerm());
            out.put("lastSeq", st.getLastSeq());
            out.put("count", log.size());
            out.put("events", events);
            return out;
        } catch (Exception e) {
            throw new JobDirectory.ApiException(503, "node " + nodeId + " is unreachable: " + e.getMessage());
        }
    }

    // =========================================================================
    // POST /api/replication/test/stale (only with -Dagentgrid.testHooks=true)
    // =========================================================================

    /**
     * Plays a stale primary against one node: sends its orchestrator a replicated event of the given
     * (lower) term, and its worker agent a subtask carrying that term. Both must be refused. Returns what each answered.
     */
    public Map<String, Object> staleWrite(int nodeId, long staleTerm, boolean sendEvent) throws JobDirectory.ApiException {
        if (!config.getNodeIds().contains(nodeId)) {
            throw new JobDirectory.ApiException(404, "node " + nodeId + " is not a member (members " + config.getNodeIds() + ")");
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("node", nodeId);
        out.put("staleTerm", staleTerm);
        try {
            OrchestratorService o = orchestrator(nodeId);
            ReplState before = monitor.call(o::replicationState);
            if (!sendEvent) {
                throw new SkipEvent();
            }
            ReplEvent fake = new ReplEvent(before.getLastSeq() + 1, staleTerm, "stale-write-test",
                    ReplEvent.Type.JOB_SUBMITTED, Map.of("query", "stale primary write"), System.currentTimeMillis());
            ReplAck ack = monitor.call(() -> o.replicate(fake));
            ReplState after = monitor.call(o::replicationState);
            Map<String, Object> rep = new LinkedHashMap<>();
            rep.put("status", ack.getStatus().name());
            rep.put("ackTerm", ack.getTerm());
            rep.put("message", ack.getMessage());
            rep.put("nodeTerm", after.getTerm());
            rep.put("lastSeqBefore", before.getLastSeq());
            rep.put("lastSeqAfter", after.getLastSeq());
            rep.put("staleRejectedBefore", before.getStaleRejected());
            rep.put("staleRejectedAfter", after.getStaleRejected());
            out.put("replicate", rep);
        } catch (SkipEvent skip) {
            // the caller only wants the dispatch tested (this node's term is not above the stale term)
        } catch (Exception e) {
            throw new JobDirectory.ApiException(503, "node " + nodeId + " orchestrator is unreachable: " + e.getMessage());
        }
        Map<String, Object> dispatch = new LinkedHashMap<>();
        try {
            NodeAgent agent = monitor.lookup(nodeId, "agent", NodeAgent.class);
            long refusedBefore = monitor.call(agent::getStaleDispatchesRefused);
            dispatch.put("highestTerm", monitor.call(agent::getHighestTerm));
            Subtask stale = new Subtask("stale-write-test", "subtask-stale", Subtask.Type.RANK, "query=stale", 1, staleTerm);
            try {
                monitor.call(() -> agent.execute(stale));
                dispatch.put("refused", false);
            } catch (Exception e) {
                String text = String.valueOf(e);
                for (Throwable c = e; c != null; c = c.getCause() == c ? null : c.getCause()) {
                    if (c instanceof agentgrid.common.StaleTermException) {
                        text = c.getMessage();
                        break;
                    }
                }
                dispatch.put("refused", text.contains("highest term"));
                dispatch.put("error", text);
            }
            dispatch.put("refusedBefore", refusedBefore);
            dispatch.put("refusedAfter", monitor.call(agent::getStaleDispatchesRefused));
        } catch (Exception e) {
            dispatch.put("refused", false);
            dispatch.put("error", "agent unreachable: " + e.getMessage());
        }
        out.put("dispatch", dispatch);
        return out;
    }
}
