package agentgrid.orchestrator;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * One entry of the replicated job log: {seq, term, jobId, type, payload}. seq grows by 1 per
 * event; term is the term of the primary that wrote it. Immutable once built; the payload holds
 * only Strings and boxed numbers, so its digest is the same in every JVM.
 */
public final class ReplEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Type {
        JOB_SUBMITTED, SUBTASK_DISPATCHED, SUBTASK_DONE, JOB_DONE, JOB_FAILED
    }

    private final long seq;
    private final long term;
    private final String jobId;
    private final Type type;
    private final LinkedHashMap<String, Object> payload;
    private final long trueMs;

    public ReplEvent(long seq, long term, String jobId, Type type, Map<String, Object> payload, long trueMs) {
        this.seq = seq;
        this.term = term;
        this.jobId = jobId;
        this.type = type;
        this.payload = new LinkedHashMap<>(payload);
        this.trueMs = trueMs;
    }

    public long getSeq() { return seq; }
    public long getTerm() { return term; }
    public String getJobId() { return jobId; }
    public Type getType() { return type; }
    public long getTrueMs() { return trueMs; }
    public Map<String, Object> getPayload() { return payload; }

    public String str(String key) {
        Object v = payload.get(key);
        return v == null ? null : String.valueOf(v);
    }

    public long num(String key, long fallback) {
        Object v = payload.get(key);
        return v instanceof Number ? ((Number) v).longValue() : fallback;
    }

    public boolean same(ReplEvent o) {
        return o != null && seq == o.seq && term == o.term && type == o.type && jobId.equals(o.jobId);
    }

    /** A short hash over the whole event (seq, term, jobId, type, payload in key order), equal on every replica. */
    public String digest() {
        StringBuilder sb = new StringBuilder();
        sb.append(seq).append('|').append(term).append('|').append(jobId).append('|').append(type);
        new TreeMap<>(payload).forEach((k, v) -> sb.append('|').append(k).append('=').append(v));
        return Integer.toHexString(sb.toString().hashCode());
    }

    /** What the API shows: everything except the (large) payload, plus its digest and the payload's keys. */
    public Map<String, Object> toSummaryMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("seq", seq);
        m.put("term", term);
        m.put("jobId", jobId);
        m.put("type", type.name());
        m.put("subtaskId", payload.get("subtaskId"));
        m.put("digest", digest());
        m.put("trueMs", trueMs);
        return m;
    }

    @Override
    public String toString() {
        return "#" + seq + " t" + term + " " + type + " " + jobId
                + (payload.get("subtaskId") == null ? "" : " " + payload.get("subtaskId"));
    }
}
