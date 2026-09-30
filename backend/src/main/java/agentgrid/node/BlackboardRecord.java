package agentgrid.node;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One replicated blackboard entry: {key, value, timestamp, writerNodeId, version}.
 *
 * timestamp is the writer node's Berkeley-corrected clock (made monotonic per node);
 * version is the writer's own write counter. Replicas decide between two records for the
 * same key with {@link #merge}, a pure function.
 */
public final class BlackboardRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String key;
    private final String value;
    private final long timestamp;
    private final int writerNodeId;
    private final long version;

    public BlackboardRecord(String key, String value, long timestamp, int writerNodeId, long version) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.writerNodeId = writerNodeId;
        this.version = version;
    }

    public String getKey() { return key; }
    public String getValue() { return value; }
    public long getTimestamp() { return timestamp; }
    public int getWriterNodeId() { return writerNodeId; }
    public long getVersion() { return version; }

    /**
     * Last-writer-wins order: the higher timestamp wins; equal timestamps are broken by the
     * higher writerNodeId. Returns true if a is strictly newer than b.
     */
    public static boolean newer(BlackboardRecord a, BlackboardRecord b) {
        if (a.timestamp != b.timestamp) {
            return a.timestamp > b.timestamp;
        }
        return a.writerNodeId > b.writerNodeId;
    }

    /**
     * The record a replica keeps when it holds current and receives incoming (either may be
     * null). Pure and order-independent: merge(a, b) and merge(b, a) pick the same record
     * whenever the two differ in (timestamp, writerNodeId); a writer never issues two
     * records with the same pair, because its stamps are strictly increasing.
     */
    public static BlackboardRecord merge(BlackboardRecord current, BlackboardRecord incoming) {
        if (current == null) {
            return incoming;
        }
        if (incoming == null) {
            return current;
        }
        return newer(incoming, current) ? incoming : current;
    }

    /** True if the two records carry the same (timestamp, writerNodeId). */
    public boolean sameStamp(BlackboardRecord other) {
        return other != null && timestamp == other.timestamp && writerNodeId == other.writerNodeId;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("key", key);
        m.put("value", value);
        m.put("timestamp", timestamp);
        m.put("writer", writerNodeId);
        m.put("version", version);
        return m;
    }

    @Override
    public String toString() {
        return "BlackboardRecord{" + key + " ts=" + timestamp + " writer=" + writerNodeId + " v=" + version + "}";
    }
}
