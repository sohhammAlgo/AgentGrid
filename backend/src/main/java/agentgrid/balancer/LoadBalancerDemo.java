package agentgrid.balancer;

import agentgrid.common.Subtask;
import agentgrid.rmi.AgentService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Experiment 6 demonstration: connects to three heterogeneous agent
 * nodes (different thread-pool sizes, simulating different hardware
 * capacity), submits the same batch of subtasks under each of the
 * three routing policies in turn, and prints the makespan (total wall
 * time to finish the whole batch) for each -- proving the policy choice
 * has a measurable effect, not just a different code path.
 *
 * Expects three AgentServer nodes already running, started with
 * different pool sizes to simulate a weak / medium / strong node, e.g.:
 * java agentgrid.rmi.AgentServer agent-weak 1201 2
 * java agentgrid.rmi.AgentServer agent-medium 1202 4
 * java agentgrid.rmi.AgentServer agent-strong 1203 6
 *
 * Usage:
 * java agentgrid.balancer.LoadBalancerDemo
 */
public class LoadBalancerDemo {

    private static final String[] AGENT_IDS = { "agent-weak", "agent-medium", "agent-strong" };
    private static final int[] PORTS = { 1201, 1202, 1203 };
    private static final int SUBTASK_COUNT = 18;

    // Weights mirror the pool sizes above (2:4:6 simplifies to 1:2:3),
    // so the Weighted policy is expected to route roughly proportional
    // to each node's real capacity.
    private static final Map<String, Integer> WEIGHTS = Map.of(
            "agent-weak", 1,
            "agent-medium", 2,
            "agent-strong", 3
    );

    public static void main(String[] args) throws Exception {
        List<AgentService> nodes = connectAll();

        System.out.println("Connected nodes: " + AGENT_IDS.length
                + " (pool sizes 2 / 4 / 6, simulating weak / medium / strong hardware)\n");

        runPolicy(new RoundRobinBalancer(), nodes);
        runPolicy(new WeightedBalancer(nodes, WEIGHTS), nodes);
        runPolicy(new LeastLoadedBalancer(), nodes);
    }

    private static void runPolicy(LoadBalancer balancer, List<AgentService> nodes) throws Exception {
        List<Subtask> batch = buildBatch(SUBTASK_COUNT);
        Map<String, Integer> tasksPerNode = new HashMap<>();
        ExecutorService clientPool = Executors.newFixedThreadPool(SUBTASK_COUNT);

        long start = System.currentTimeMillis();

        List<Future<?>> futures = new ArrayList<>();
        for (Subtask subtask : batch) {
            AgentService chosen = balancer.select(nodes);
            String agentId = chosen.getAgentId();
            tasksPerNode.merge(agentId, 1, Integer::sum);

            futures.add(clientPool.submit(() -> {
                try {
                    chosen.executeBatch(List.of(subtask));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get();
        }

        long makespan = System.currentTimeMillis() - start;
        clientPool.shutdown();

        System.out.println("--- " + balancer.getName() + " ---");
        System.out.println(" Distribution: " + tasksPerNode);
        System.out.println(" Makespan: " + makespan + " ms\n");
    }

    private static List<AgentService> connectAll() throws Exception {
        List<AgentService> nodes = new ArrayList<>();
        for (int i = 0; i < AGENT_IDS.length; i++) {
            Registry registry = LocateRegistry.getRegistry("localhost", PORTS[i]);
            nodes.add((AgentService) registry.lookup(AGENT_IDS[i]));
        }
        return nodes;
    }

    private static List<Subtask> buildBatch(int n) {
        List<Subtask> batch = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            batch.add(new Subtask("task-lb", "subtask-" + i, Subtask.Type.SUMMARIZE, "chunk-" + i, i));
        }
        return batch;
    }
}