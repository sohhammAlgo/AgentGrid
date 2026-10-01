package agentgrid.node;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** One cluster member: its id, RMI registry port (1600 + id), worker pool size and routing weight. */
public final class MemberSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int BASE_PORT = 1600;

    private final int id;
    private final int port;
    private final int poolSize;
    private final int weight;

    public MemberSpec(int id, int port, int poolSize, int weight) {
        if (id <= 0) {
            throw new IllegalArgumentException("member id must be positive, got " + id);
        }
        this.id = id;
        this.port = port;
        this.poolSize = Math.max(1, poolSize);
        this.weight = Math.max(1, weight);
    }

    /** The spec of a node added at runtime: port 1600 + id. */
    public static MemberSpec of(int id, int poolSize, int weight) {
        return new MemberSpec(id, BASE_PORT + id, poolSize, weight);
    }

    public int getId() { return id; }
    public int getPort() { return port; }
    public int getPoolSize() { return poolSize; }
    public int getWeight() { return weight; }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("port", port);
        m.put("poolSize", poolSize);
        m.put("weight", weight);
        return m;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MemberSpec)) return false;
        MemberSpec s = (MemberSpec) o;
        return id == s.id && port == s.port && poolSize == s.poolSize && weight == s.weight;
    }

    @Override
    public int hashCode() {
        return ((id * 31 + port) * 31 + poolSize) * 31 + weight;
    }

    @Override
    public String toString() {
        return "node-" + id + "(port " + port + ", pool " + poolSize + ", weight " + weight + ")";
    }
}
