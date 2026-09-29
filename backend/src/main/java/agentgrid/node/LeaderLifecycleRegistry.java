package agentgrid.node;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Fans leadership transitions out to registered LeaderLifecycle listeners.
 * A listener that throws does not stop the others or the election code.
 */
public class LeaderLifecycleRegistry implements LeaderLifecycle {

    private final List<LeaderLifecycle> listeners = new CopyOnWriteArrayList<>();

    public void register(LeaderLifecycle listener) {
        listeners.add(listener);
    }

    public void unregister(LeaderLifecycle listener) {
        listeners.remove(listener);
    }

    @Override
    public void onElected(int nodeId) {
        for (LeaderLifecycle l : listeners) {
            try {
                l.onElected(nodeId);
            } catch (RuntimeException e) {
                System.err.println("[Node " + nodeId + "] lifecycle listener failed in onElected: " + e);
            }
        }
    }

    @Override
    public void onDemoted(int nodeId, int newLeaderId) {
        for (LeaderLifecycle l : listeners) {
            try {
                l.onDemoted(nodeId, newLeaderId);
            } catch (RuntimeException e) {
                System.err.println("[Node " + nodeId + "] lifecycle listener failed in onDemoted: " + e);
            }
        }
    }

    @Override
    public void onLeaderLost(int nodeId, int lostLeaderId) {
        for (LeaderLifecycle l : listeners) {
            try {
                l.onLeaderLost(nodeId, lostLeaderId);
            } catch (RuntimeException e) {
                System.err.println("[Node " + nodeId + "] lifecycle listener failed in onLeaderLost: " + e);
            }
        }
    }

    /**
     * Default listener: emits the lifecycle as events. LEADER_LOST goes to the node's
     * telemetry buffer; elected/demoted are logged, because the LEADER_ACCEPTED event
     * the election code records at the same moment already carries that transition.
     */
    public static LeaderLifecycle eventEmitter(NodeEventBuffer events) {
        return new LeaderLifecycle() {
            @Override
            public void onElected(int nodeId) {
                System.out.println("[Node " + nodeId + "] lifecycle: elected leader");
            }

            @Override
            public void onDemoted(int nodeId, int newLeaderId) {
                System.out.println("[Node " + nodeId + "] lifecycle: demoted, new leader " + newLeaderId);
            }

            @Override
            public void onLeaderLost(int nodeId, int lostLeaderId) {
                events.record("LEADER_LOST", "leader " + lostLeaderId + " declared dead by failure detector",
                        Map.of("leader", lostLeaderId));
            }
        };
    }
}
