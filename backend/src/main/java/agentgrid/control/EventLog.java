package agentgrid.control;

import agentgrid.clock.LamportClock;
import agentgrid.node.NodeEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Thread-safe fixed-capacity (5000) ring buffer for cluster control-plane events.
 * Manages the control-plane's Lamport clock and dispatches event notifications.
 *
 * Holds two kinds of events: those the control plane records itself (stamped by its own
 * clock) and those pulled from node telemetry (which keep the node's own Lamport time).
 */
public class EventLog {

    public static class Event {
        private final long seq;
        private final String type;
        private final int node;
        private final String details;
        private final long lamport;
        private final long nodeWallMs;
        private final long trueMs;
        private final Long nodeSeq;
        private final Map<String, Object> fields;

        public Event(long seq, String type, int node, String details, long lamport, long nodeWallMs) {
            this(seq, type, node, details, lamport, nodeWallMs, System.currentTimeMillis(), null, null);
        }

        public Event(long seq, String type, int node, String details, long lamport, long nodeWallMs,
                     long trueMs, Long nodeSeq, Map<String, Object> fields) {
            this.seq = seq;
            this.type = type;
            this.node = node;
            this.details = details;
            this.lamport = lamport;
            this.nodeWallMs = nodeWallMs;
            this.trueMs = trueMs;
            this.nodeSeq = nodeSeq;
            this.fields = fields == null ? Collections.emptyMap() : new LinkedHashMap<>(fields);
        }

        public long getSeq() {
            return seq;
        }

        public String getType() {
            return type;
        }

        public int getNode() {
            return node;
        }

        public String getDetails() {
            return details;
        }

        public long getLamport() {
            return lamport;
        }

        public long getNodeWallMs() {
            return nodeWallMs;
        }

        public long getTrueMs() {
            return trueMs;
        }

        public Long getNodeSeq() {
            return nodeSeq;
        }

        public Map<String, Object> getFields() {
            return fields;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("seq", seq);
            map.put("type", type);
            map.put("node", node);
            map.put("details", details);
            map.put("lamport", lamport);
            map.put("nodeWallMs", nodeWallMs);
            map.put("trueMs", trueMs);
            if (nodeSeq != null) {
                map.put("nodeSeq", nodeSeq);
            }
            if (!fields.isEmpty()) {
                map.put("fields", fields);
            }
            return map;
        }
    }

    /**
     * 5000 (up from 500): each job adds about 95 node events, and the Exp 5 tests run many jobs;
     * a smaller ring evicted election and leader events the verifiers still need. Node
     * buffers stay at 500 because the control plane drains them every cycle.
     */
    private static final int CAPACITY = 5000;
    private final Event[] buffer = new Event[CAPACITY];
    private final AtomicLong seqGenerator = new AtomicLong(0);
    private final LamportClock lamportClock = new LamportClock();
    private final List<Consumer<Event>> listeners = new CopyOnWriteArrayList<>();

    public void addListener(Consumer<Event> listener) {
        listeners.add(listener);
    }

    public void removeListener(Consumer<Event> listener) {
        listeners.remove(listener);
    }

    /**
     * Records a control-plane initiated event. Advances the control plane's Lamport clock via tick().
     */
    public synchronized Event record(String type, int node, String details, long nodeWallMs) {
        return record(type, node, details, nodeWallMs, null);
    }

    /** As record(), with structured fields (e.g. MEMBERSHIP_CHANGED carries epoch and members). */
    public synchronized Event record(String type, int node, String details, long nodeWallMs, Map<String, Object> fields) {
        long lamport = lamportClock.tick();
        long seq = seqGenerator.incrementAndGet();
        Event event = new Event(seq, type, node, details, lamport, nodeWallMs, System.currentTimeMillis(), null, fields);
        buffer[(int) ((seq - 1) % CAPACITY)] = event;
        notifyListeners(event);
        return event;
    }

    /**
     * Records an event resulting from a received message. Advances the control plane's clock via update().
     */
    public synchronized Event recordWithReceivedLamport(
            String type, int node, String details, long receivedLamport, long nodeWallMs) {
        long lamport = lamportClock.update(receivedLamport);
        long seq = seqGenerator.incrementAndGet();
        Event event = new Event(seq, type, node, details, lamport, nodeWallMs);
        buffer[(int) ((seq - 1) % CAPACITY)] = event;
        notifyListeners(event);
        return event;
    }

    /**
     * Merges events pulled from node telemetry, ordered by (lamport, node, node seq).
     * Each keeps its node's Lamport time. Pulling is a receive for the control plane, so
     * its own clock moves past every merged event; control-plane events recorded later
     * (e.g. NODE_KILLED) therefore carry a higher Lamport time than anything already seen.
     */
    public List<Event> mergeNodeEvents(List<NodeEvent> pulled) {
        List<NodeEvent> sorted = new ArrayList<>(pulled);
        sorted.sort(Comparator.comparingLong(NodeEvent::getLamport)
                .thenComparingInt(NodeEvent::getNode)
                .thenComparingLong(NodeEvent::getSeq));
        List<Event> merged = new ArrayList<>(sorted.size());
        synchronized (this) {
            for (NodeEvent ne : sorted) {
                lamportClock.update(ne.getLamport());
                long seq = seqGenerator.incrementAndGet();
                Event event = new Event(seq, ne.getType(), ne.getNode(), ne.getDetails(), ne.getLamport(),
                        ne.getNodeWallMs(), ne.getTrueMs(), ne.getSeq(), ne.getFields());
                buffer[(int) ((seq - 1) % CAPACITY)] = event;
                merged.add(event);
            }
        }
        for (Event e : merged) {
            notifyListeners(e);
        }
        return merged;
    }

    private void notifyListeners(Event event) {
        for (Consumer<Event> listener : listeners) {
            try {
                listener.accept(event);
            } catch (Exception ignored) {
            }
        }
    }

    public synchronized List<Event> getEventsSince(long sinceSeq) {
        List<Event> result = new ArrayList<>();
        for (Event e : buffer) {
            if (e != null && e.getSeq() > sinceSeq) {
                result.add(e);
            }
        }
        result.sort(Comparator.comparingLong(Event::getSeq));
        return result;
    }

    public synchronized List<Event> getAllEvents() {
        return getEventsSince(0);
    }

    public LamportClock getLamportClock() {
        return lamportClock;
    }
}
