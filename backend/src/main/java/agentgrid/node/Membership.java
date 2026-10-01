package agentgrid.node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The cluster's membership: an epoch and the member specs, keyed (and ordered) by node id.
 * Immutable. The control plane is the only authority that creates a new epoch; every node
 * keeps a copy and applies an incoming membership only if its epoch is higher.
 *
 * STRONG blackboard writes need a live quorum of floor(n / 2) + 1 of these n members.
 */
public final class Membership implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int MIN_MEMBERS = 3;
    public static final int MAX_MEMBERS = 9;

    private final long epoch;
    private final TreeMap<Integer, MemberSpec> members;

    public Membership(long epoch, Map<Integer, MemberSpec> members) {
        this.epoch = epoch;
        this.members = new TreeMap<>(members);
    }

    public long getEpoch() { return epoch; }
    public SortedMap<Integer, MemberSpec> getMembers() { return Collections.unmodifiableSortedMap(members); }
    public int size() { return members.size(); }
    public boolean contains(int id) { return members.containsKey(id); }
    public MemberSpec get(int id) { return members.get(id); }

    /** Member ids in ascending order (also the ring order). */
    public List<Integer> ids() {
        return new ArrayList<>(members.keySet());
    }

    /** Live members a STRONG write needs: floor(n / 2) + 1. */
    public int quorum() {
        return members.size() / 2 + 1;
    }

    /** This membership plus spec, at the next epoch. */
    public Membership with(MemberSpec spec) {
        TreeMap<Integer, MemberSpec> m = new TreeMap<>(members);
        m.put(spec.getId(), spec);
        return new Membership(epoch + 1, m);
    }

    /** This membership without id, at the next epoch. */
    public Membership without(int id) {
        TreeMap<Integer, MemberSpec> m = new TreeMap<>(members);
        m.remove(id);
        return new Membership(epoch + 1, m);
    }

    /**
     * The view handed to a node that is joining: the current members plus itself, at the
     * CURRENT epoch. It is never pushed to anyone; the official membership that includes the
     * node is the next epoch, pushed only after the node is ready.
     */
    public Membership joining(MemberSpec spec) {
        TreeMap<Integer, MemberSpec> m = new TreeMap<>(members);
        m.put(spec.getId(), spec);
        return new Membership(epoch, m);
    }

    /** "epoch;id:port:pool:weight,..." for passing on a node's command line. */
    public String encode() {
        StringBuilder sb = new StringBuilder().append(epoch).append(';');
        boolean first = true;
        for (MemberSpec s : members.values()) {
            if (!first) sb.append(',');
            first = false;
            sb.append(s.getId()).append(':').append(s.getPort()).append(':').append(s.getPoolSize()).append(':').append(s.getWeight());
        }
        return sb.toString();
    }

    public static Membership decode(String text) {
        String[] head = text.trim().split(";", 2);
        long epoch = Long.parseLong(head[0].trim());
        TreeMap<Integer, MemberSpec> m = new TreeMap<>();
        if (head.length > 1 && !head[1].isBlank()) {
            for (String part : head[1].split(",")) {
                String[] f = part.trim().split(":");
                MemberSpec s = new MemberSpec(Integer.parseInt(f[0]), Integer.parseInt(f[1]),
                        Integer.parseInt(f[2]), Integer.parseInt(f[3]));
                m.put(s.getId(), s);
            }
        }
        return new Membership(epoch, m);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("epoch", epoch);
        out.put("size", members.size());
        out.put("quorum", quorum());
        List<Map<String, Object>> specs = new ArrayList<>();
        for (MemberSpec s : members.values()) {
            specs.add(s.toMap());
        }
        out.put("members", specs);
        return out;
    }

    @Override
    public String toString() {
        return "epoch " + epoch + " " + members.keySet();
    }
}
