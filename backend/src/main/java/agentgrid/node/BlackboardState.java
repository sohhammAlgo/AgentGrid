package agentgrid.node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A replica's state as reported to readers: whether it is ready (caught up after boot),
 * whether its clock has been synced since boot, and either one record (a read) or all of
 * them (a snapshot), plus its outstanding EVENTUAL deliveries.
 */
public final class BlackboardState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int nodeId;
    private final boolean ready;
    private final boolean clockSynced;
    private final String snapshotSource;
    private final int pendingPropagation;
    private final long lastStamp;
    private final ArrayList<BlackboardRecord> records;

    public BlackboardState(int nodeId, boolean ready, boolean clockSynced, String snapshotSource,
                           int pendingPropagation, long lastStamp, List<BlackboardRecord> records) {
        this.nodeId = nodeId;
        this.ready = ready;
        this.clockSynced = clockSynced;
        this.snapshotSource = snapshotSource;
        this.pendingPropagation = pendingPropagation;
        this.lastStamp = lastStamp;
        this.records = new ArrayList<>(records);
    }

    public int getNodeId() { return nodeId; }
    public boolean isReady() { return ready; }
    public boolean isClockSynced() { return clockSynced; }
    public String getSnapshotSource() { return snapshotSource; }
    public int getPendingPropagation() { return pendingPropagation; }
    public long getLastStamp() { return lastStamp; }
    public List<BlackboardRecord> getRecords() { return records; }

    /** The single record of a read, or null. */
    public BlackboardRecord record() {
        return records.isEmpty() ? null : records.get(0);
    }

    public Map<String, Object> header() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("node", nodeId);
        m.put("ready", ready);
        m.put("clockSynced", clockSynced);
        m.put("snapshotSource", snapshotSource);
        m.put("pendingPropagation", pendingPropagation);
        m.put("lastStamp", lastStamp);
        return m;
    }
}
