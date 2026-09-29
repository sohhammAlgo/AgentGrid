package agentgrid.orchestrator;

import agentgrid.balancer.LeastLoadedBalancer;
import agentgrid.balancer.LoadBalancer;
import agentgrid.balancer.RoundRobinBalancer;
import agentgrid.balancer.WeightedBalancer;
import agentgrid.rmi.AgentService;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The three routing policies of Exp 6, each built from the SUBMITTED LoadBalancer class
 * over WorkerNode adapters of the live nodes.
 *  - ROUND_ROBIN: RoundRobinBalancer, unchanged.
 *  - WEIGHTED: WeightedBalancer with each node's weight = its live pool size.
 *  - LEAST_LOADED: LeastLoadedBalancer, fed capacity-normalised load by WorkerNode (fix G).
 */
public enum BalancingPolicy {
    ROUND_ROBIN, LEAST_LOADED, WEIGHTED;

    /**
     * @param leastLoadedView what LEAST_LOADED sees as load; the orchestrator always uses
     *                        NORMALIZED_IN_FLIGHT, the baseline probe also measures the others
     */
    public LoadBalancer create(List<WorkerNode> workers, WorkerNode.LoadView leastLoadedView) throws RemoteException {
        for (WorkerNode w : workers) {
            w.setLoadView(this == LEAST_LOADED ? leastLoadedView : WorkerNode.LoadView.RAW);
        }
        switch (this) {
            case WEIGHTED: {
                Map<String, Integer> weights = new LinkedHashMap<>();
                for (WorkerNode w : workers) {
                    weights.put(w.getAgentId(), w.getPoolSize());
                }
                return new WeightedBalancer(asServices(workers), weights);
            }
            case LEAST_LOADED:
                return new LeastLoadedBalancer();
            case ROUND_ROBIN:
            default:
                return new RoundRobinBalancer();
        }
    }

    static List<AgentService> asServices(List<WorkerNode> workers) {
        return new ArrayList<>(workers);
    }

    /** Parses ROUND_ROBIN / LEAST_LOADED / WEIGHTED (case-insensitive); null if unknown. */
    public static BalancingPolicy parse(String name) {
        if (name == null) {
            return null;
        }
        try {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
