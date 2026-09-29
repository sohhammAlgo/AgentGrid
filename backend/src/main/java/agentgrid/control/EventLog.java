package agentgrid.control;

import agentgrid.clock.LamportClock;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Thread-safe fixed-capacity (500) ring buffer for cluster control-plane events.
 * Manages the control-plane's Lamport clock and dispatches event notifications.
 */
public class EventLog {

    public static class Event {
        private final long seq;
        private final String type;
        private final int node;
        private final String details;
        private final long lamport;
        private final long nodeWallMs;

        public Event(long seq, String type, int node, String details, long lamport, long nodeWallMs) {
            this.seq = seq;
            this.type = type;
            this.node = node;
            this.details = details;
            this.lamport = lamport;
            this.nodeWallMs = nodeWallMs;
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

        public Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("seq", seq);
            map.put("type", type);
            map.put("node", node);
            map.put("details", details);
            map.put("lamport", lamport);
            map.put("nodeWallMs", nodeWallMs);
            return map;
        }
    }

    private static final int CAPACITY = 500;
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
        long lamport = lamportClock.tick();
        long seq = seqGenerator.incrementAndGet();
        Event event = new Event(seq, type, node, details, lamport, nodeWallMs);
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
