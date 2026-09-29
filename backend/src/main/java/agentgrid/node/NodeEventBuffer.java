package agentgrid.node;

import agentgrid.clock.LamportClock;
import agentgrid.clock.TimeServiceImpl;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Fixed-capacity (500) ring of this node's telemetry events, exported as "telemetry".
 *
 * Every recorded event ticks the node's LamportClock. The tick, the seq assignment and
 * the store happen under the clock's monitor (the same monitor NodeAgentService uses for
 * its receive rule), so one node's events are strictly increasing in both seq and lamport.
 */
public class NodeEventBuffer extends UnicastRemoteObject implements NodeTelemetry {

    private static final long serialVersionUID = 1L;
    private static final int CAPACITY = 500;

    private final int nodeId;
    private final transient LamportClock clock;
    private final transient TimeServiceImpl timeService;
    private final long incarnation = System.currentTimeMillis();
    private final transient NodeEvent[] ring = new NodeEvent[CAPACITY];
    private long seq = 0;

    public NodeEventBuffer(int nodeId, LamportClock clock, TimeServiceImpl timeService) throws RemoteException {
        super();
        this.nodeId = nodeId;
        this.clock = clock;
        this.timeService = timeService;
    }

    /**
     * Records a local event (Lamport tick rule) and returns it.
     */
    public NodeEvent record(String type, String details, Map<String, Object> fields) {
        NodeEvent event;
        synchronized (clock) {
            long lamport = clock.tick();
            long s = ++seq;
            event = new NodeEvent(s, type, nodeId, details, lamport,
                    timeService.getTime(), System.currentTimeMillis(), incarnation, fields);
            ring[(int) ((s - 1) % CAPACITY)] = event;
        }
        System.out.println("[Node " + nodeId + "] " + type + " L=" + event.getLamport()
                + " " + event.getFields() + " " + details);
        return event;
    }

    /**
     * Lamport receive rule for an inbound message stamped with the sender's time.
     */
    public void receive(long senderLamport) {
        synchronized (clock) {
            clock.update(senderLamport);
        }
    }

    @Override
    public List<NodeEvent> getEvents(long sinceSeq) {
        synchronized (clock) {
            long from = sinceSeq > seq ? 0 : sinceSeq;
            List<NodeEvent> out = new ArrayList<>();
            long oldest = Math.max(1, seq - CAPACITY + 1);
            for (long s = Math.max(from + 1, oldest); s <= seq; s++) {
                out.add(ring[(int) ((s - 1) % CAPACITY)]);
            }
            return out;
        }
    }

    public long getIncarnation() {
        return incarnation;
    }
}
