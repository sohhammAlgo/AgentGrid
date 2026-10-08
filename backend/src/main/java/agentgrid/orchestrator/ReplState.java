package agentgrid.orchestrator;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** A node's replication state as the control plane and the other nodes read it. */
public final class ReplState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int nodeId;
    private final boolean enabled;
    private final boolean primary;
    private final boolean ready;
    private final long term;
    private final long lastSeq;
    private final long lastLogTerm;
    private final Integer backupId;
    private final long backupLastSeq;
    private final boolean noBackup;
    private final long staleRejected;
    private final long recoveredJobs;
    private final int logEvents;

    public ReplState(int nodeId, boolean enabled, boolean primary, boolean ready, long term, long lastSeq,
                     long lastLogTerm, Integer backupId, long backupLastSeq, boolean noBackup, long staleRejected,
                     long recoveredJobs, int logEvents) {
        this.nodeId = nodeId;
        this.enabled = enabled;
        this.primary = primary;
        this.ready = ready;
        this.term = term;
        this.lastSeq = lastSeq;
        this.lastLogTerm = lastLogTerm;
        this.backupId = backupId;
        this.backupLastSeq = backupLastSeq;
        this.noBackup = noBackup;
        this.staleRejected = staleRejected;
        this.recoveredJobs = recoveredJobs;
        this.logEvents = logEvents;
    }

    public int getNodeId() { return nodeId; }
    public boolean isEnabled() { return enabled; }
    /** True while this node is the active primary (its orchestrator is the elected leader's). */
    public boolean isPrimary() { return primary; }
    /** False while a newly elected primary is still catching up and replaying its log. */
    public boolean isReady() { return ready; }
    public long getTerm() { return term; }
    public long getLastSeq() { return lastSeq; }
    /** Term of the last event in the log (0 for an empty log). */
    public long getLastLogTerm() { return lastLogTerm; }
    public Integer getBackupId() { return backupId; }
    public long getBackupLastSeq() { return backupLastSeq; }
    public boolean isNoBackup() { return noBackup; }
    /** Events, snapshots and dispatches this node refused because they carried a lower term. */
    public long getStaleRejected() { return staleRejected; }
    public long getRecoveredJobs() { return recoveredJobs; }
    public int getLogEvents() { return logEvents; }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("node", nodeId);
        m.put("enabled", enabled);
        m.put("primary", primary);
        m.put("ready", ready);
        m.put("term", term);
        m.put("lastSeq", lastSeq);
        m.put("lastLogTerm", lastLogTerm);
        m.put("backup", backupId);
        m.put("backupLastSeq", backupLastSeq);
        m.put("noBackup", noBackup);
        m.put("staleRejected", staleRejected);
        m.put("recoveredJobs", recoveredJobs);
        m.put("logEvents", logEvents);
        return m;
    }
}
