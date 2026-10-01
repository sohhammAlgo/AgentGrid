package agentgrid.orchestrator;

import agentgrid.node.ClusterConfig;
import agentgrid.node.Membership;
import agentgrid.node.NodeAgent;
import agentgrid.node.TimeoutSocketFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The orchestrator's view of which nodes can take work: every configured node whose
 * "agent" answers a ping. Lookups go through TimeoutSocketFactory so a killed node is
 * skipped after at most the connect timeout.
 */
public final class ClusterView {

    private final ClusterConfig config;
    private final Map<Integer, NodeAgent> stubs = new ConcurrentHashMap<>();

    public ClusterView(ClusterConfig config) {
        this.config = config;
    }

    /** Live workers among the current members, in ascending node id order. */
    public List<WorkerNode> liveWorkers() {
        List<WorkerNode> out = new ArrayList<>();
        Membership members = config.membership();
        stubs.keySet().removeIf(id -> !members.contains(id));
        for (int id : members.ids()) {
            NodeAgent agent = reachable(id, members.get(id).getPort());
            if (agent != null) {
                try {
                    out.add(new WorkerNode(id, agent, agent.getPoolSize(), members.get(id).getWeight()));
                } catch (Exception e) {
                    stubs.remove(id);
                }
            }
        }
        return out;
    }

    /** Forgets a cached stub after a failed call, so the next refresh looks it up again. */
    public void forget(int nodeId) {
        stubs.remove(nodeId);
    }

    private NodeAgent reachable(int id, int port) {
        NodeAgent cached = stubs.get(id);
        if (cached != null) {
            try {
                cached.ping();
                return cached;
            } catch (Exception e) {
                stubs.remove(id, cached);
            }
        }
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", port, TimeoutSocketFactory.INSTANCE);
            NodeAgent fresh = (NodeAgent) registry.lookup("agent");
            fresh.ping();
            stubs.put(id, fresh);
            return fresh;
        } catch (Exception e) {
            return null;
        }
    }
}
