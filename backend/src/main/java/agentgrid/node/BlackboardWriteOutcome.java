package agentgrid.node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Result of one blackboard write, as seen by the node that accepted it. */
public final class BlackboardWriteOutcome implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        /** STRONG: every node that was live at the check acknowledged. */
        STORED,
        /** STRONG: at least a majority acknowledged, but a node live at the check did not; it catches up later. */
        STORED_DEGRADED,
        /** STRONG: fewer than a majority were live at the check; nothing was applied anywhere. */
        REFUSED,
        /** STRONG: applied, but fewer than a majority acknowledged. */
        FAILED_PARTIAL,
        /** EVENTUAL: applied locally and acknowledged; propagation to peers is pending. */
        ACCEPTED;

        /** Whether the write counts as stored (acknowledged under its mode). */
        public boolean stored() {
            return this == STORED || this == STORED_DEGRADED || this == ACCEPTED;
        }
    }

    private final Status status;
    private final String mode;
    private final String key;
    private final int origin;
    private final BlackboardRecord record;
    private final ArrayList<Integer> liveAtCheck;
    private final ArrayList<Integer> acked;
    private final ArrayList<Integer> failed;
    private final long latencyMs;
    private final String message;

    public BlackboardWriteOutcome(Status status, String mode, String key, int origin, BlackboardRecord record,
                                  List<Integer> liveAtCheck, List<Integer> acked, List<Integer> failed,
                                  long latencyMs, String message) {
        this.status = status;
        this.mode = mode;
        this.key = key;
        this.origin = origin;
        this.record = record;
        this.liveAtCheck = new ArrayList<>(liveAtCheck);
        this.acked = new ArrayList<>(acked);
        this.failed = new ArrayList<>(failed);
        this.latencyMs = latencyMs;
        this.message = message;
    }

    public Status getStatus() { return status; }
    public String getMode() { return mode; }
    public String getKey() { return key; }
    public BlackboardRecord getRecord() { return record; }
    public long getLatencyMs() { return latencyMs; }
    public String getMessage() { return message; }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", status.name());
        m.put("stored", status.stored());
        m.put("mode", mode);
        m.put("key", key);
        m.put("node", origin);
        m.put("timestamp", record == null ? null : record.getTimestamp());
        m.put("writer", record == null ? null : record.getWriterNodeId());
        m.put("version", record == null ? null : record.getVersion());
        m.put("liveAtCheck", liveAtCheck);
        m.put("acked", acked);
        m.put("failed", failed);
        m.put("latencyMs", latencyMs);
        m.put("message", message);
        return m;
    }
}
