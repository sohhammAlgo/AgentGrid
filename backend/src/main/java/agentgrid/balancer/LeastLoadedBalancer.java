package agentgrid.balancer;

import agentgrid.rmi.AgentService;

import java.rmi.RemoteException;
import java.util.List;

/**
 * Queries every node's live queue depth (Exp 1's getQueueDepth(), backed
 * by Exp 2's AtomicInteger) over RMI and routes to whichever node is
 * currently least busy. This is the only policy here that reacts to
 * real-time state rather than a fixed assumption, so it should stay
 * competitive even when node capacity is unknown or changes at runtime
 * -- at the cost of one extra RMI round-trip per routing decision.
 */
public class LeastLoadedBalancer implements LoadBalancer {

    @Override
    public AgentService select(List<AgentService> nodes) throws RemoteException {
        AgentService best = null;
        int bestDepth = Integer.MAX_VALUE;

        for (AgentService node : nodes) {
            int depth = node.getQueueDepth();
            if (depth < bestDepth) {
                bestDepth = depth;
                best = node;
            }
        }
        return best;
    }

    @Override
    public String getName() {
        return "LeastLoaded";
    }
}