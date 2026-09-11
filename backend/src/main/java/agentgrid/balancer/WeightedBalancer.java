package agentgrid.balancer;

import agentgrid.rmi.AgentService;

import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Routes using a fixed weight per node (e.g. proportional to its
 * thread-pool / hardware capacity), via smooth weighted round robin:
 * each node accumulates "credit" equal to its weight every round, and
 * whichever node has the highest credit is picked and then debited by
 * the total weight. Over many picks this converges to each node
 * receiving a share of traffic proportional to its weight, without the
 * bursty back-to-back selections a naive weighted-round-robin produces.
 *
 * Unlike LeastLoaded, this never calls back to the nodes -- it trusts
 * a capacity figure decided up front, which is cheaper per decision but
 * blind to a node slowing down for reasons its configured weight didn't
 * anticipate.
 */
public class WeightedBalancer implements LoadBalancer {

    private final List<AgentService> orderedNodes;
    private final int[] weights;
    private final AtomicInteger[] credit;
    private final Object lock = new Object();

    public WeightedBalancer(List<AgentService> orderedNodes, Map<String, Integer> weightsByAgentId) throws RemoteException {
        this.orderedNodes = orderedNodes;
        this.weights = new int[orderedNodes.size()];
        this.credit = new AtomicInteger[orderedNodes.size()];

        for (int i = 0; i < orderedNodes.size(); i++) {
            String id = orderedNodes.get(i).getAgentId();
            weights[i] = weightsByAgentId.getOrDefault(id, 1);
            credit[i] = new AtomicInteger(0);
        }
    }

    @Override
    public AgentService select(List<AgentService> nodes) {
        synchronized (lock) {
            int totalWeight = 0;
            int bestIndex = -1;
            int bestCredit = Integer.MIN_VALUE;

            for (int i = 0; i < orderedNodes.size(); i++) {
                credit[i].addAndGet(weights[i]);
                totalWeight += weights[i];
                if (credit[i].get() > bestCredit) {
                    bestCredit = credit[i].get();
                    bestIndex = i;
                }
            }

            credit[bestIndex].addAndGet(-totalWeight);
            return orderedNodes.get(bestIndex);
        }
    }

    @Override
    public String getName() {
        return "Weighted";
    }
}