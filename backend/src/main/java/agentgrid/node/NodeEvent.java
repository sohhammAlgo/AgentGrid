package agentgrid.node;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One telemetry event recorded inside a node process and pulled by the control plane.
 *
 * lamport is the node's own Lamport time for the event; nodeWallMs is the node's
 * drifted physical clock (TimeService); trueMs is the machine clock, used by the
 * control plane to measure real durations. incarnation identifies the node process,
 * so the control plane can tell a restarted node's sequence numbers apart.
 */
public class NodeEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long seq;
    private final String type;
    private final int node;
    private final String details;
    private final long lamport;
    private final long nodeWallMs;
    private final long trueMs;
    private final long incarnation;
    private final LinkedHashMap<String, Object> fields;

    public NodeEvent(long seq, String type, int node, String details, long lamport,
                     long nodeWallMs, long trueMs, long incarnation, Map<String, Object> fields) {
        this.seq = seq;
        this.type = type;
        this.node = node;
        this.details = details;
        this.lamport = lamport;
        this.nodeWallMs = nodeWallMs;
        this.trueMs = trueMs;
        this.incarnation = incarnation;
        this.fields = fields == null ? new LinkedHashMap<>() : new LinkedHashMap<>(fields);
    }

    public long getSeq() { return seq; }
    public String getType() { return type; }
    public int getNode() { return node; }
    public String getDetails() { return details; }
    public long getLamport() { return lamport; }
    public long getNodeWallMs() { return nodeWallMs; }
    public long getTrueMs() { return trueMs; }
    public long getIncarnation() { return incarnation; }
    public Map<String, Object> getFields() { return Collections.unmodifiableMap(fields); }

    @Override
    public String toString() {
        return "NodeEvent{seq=" + seq + ", type=" + type + ", node=" + node
                + ", lamport=" + lamport + ", fields=" + fields + ", details=" + details + "}";
    }
}
