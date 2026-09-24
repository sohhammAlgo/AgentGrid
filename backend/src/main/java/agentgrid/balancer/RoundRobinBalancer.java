package agentgrid.balancer;

import agentgrid.rmi.AgentService;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cycles through nodes in fixed order, ignoring current load or
 * capacity entirely. This is the baseline every other policy is
 * measured against -- it's fair by task count, not by actual work
 * done, which is exactly its weakness when nodes are heterogeneous
 * or task cost varies.
 */
public class RoundRobinBalancer implements LoadBalancer {

    private final AtomicInteger index = new AtomicInteger(0);

    @Override
    public AgentService select(List<AgentService> nodes) {
        int i = Math.floorMod(index.getAndIncrement(), nodes.size());
        return nodes.get(i);
    }

    @Override
    public String getName() {
        return "RoundRobin";
    }
}