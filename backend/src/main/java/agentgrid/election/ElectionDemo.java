package agentgrid.election;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Experiment 4 demonstration:
 * 1. Bootstraps a cluster of 5 nodes (IDs 1 to 5).
 * 2. Runs the Bully Election Algorithm: initial election -> leader failure (Node 5) -> failover election.
 * 3. Runs the Ring Election Algorithm: initial election -> leader failure (Node 5) -> failover election.
 * 4. Compares convergence latency and total election message overhead between Bully and Ring.
 */
public class ElectionDemo {

    private static final int NODE_COUNT = 5;
    private static final int BASE_PORT = 1300;

    public static void main(String[] args) throws Exception {
        System.out.println("=================================================");
        System.out.println(" AgentGrid-Lite: Experiment 4 (Leader Election)");
        System.out.println(" Comparing Bully Algorithm vs Ring Algorithm");
        System.out.println("=================================================\n");

        Map<Integer, Integer> nodePortMap = new HashMap<>();
        List<Integer> ringOrder = new ArrayList<>();
        for (int i = 1; i <= NODE_COUNT; i++) {
            nodePortMap.put(i, BASE_PORT + i);
            ringOrder.add(i);
        }

        // ==========================================
        // 1. BULLY ALGORITHM TEST
        // ==========================================
        System.out.println("--- 1. BULLY ALGORITHM ---");
        ElectionMetrics bullyMetrics = new ElectionMetrics();
        List<BullyElectionNode> bullyNodes = new ArrayList<>();
        List<Registry> bullyRegistries = new ArrayList<>();

        for (int i = 1; i <= NODE_COUNT; i++) {
            int port = BASE_PORT + i;
            BullyElectionNode node = new BullyElectionNode(i, port, nodePortMap, bullyMetrics);
            Registry registry = LocateRegistry.createRegistry(port);
            registry.rebind("election-node-" + i, node);
            bullyNodes.add(node);
            bullyRegistries.add(registry);
        }

        System.out.println("Bootstrapped 5 Bully nodes on ports 1301..1305.");

        // Initial Bully Election
        System.out.println("Triggering initial Bully election from Node 1...");
        bullyNodes.get(0).startElection();
        Thread.sleep(1200);

        int initialBullyLeader = bullyNodes.get(0).getLeaderId();
        System.out.println("Initial Bully Leader: Node " + initialBullyLeader + "\n");

        // Failover Bully Election (Crash Node 5)
        System.out.println("Simulating crash of leader Node " + initialBullyLeader + "...");
        bullyNodes.get(4).simulateCrash(); // Crash Node 5

        bullyMetrics.reset();
        System.out.println("Triggering Bully failover election from Node 1...");
        bullyNodes.get(0).startElection();
        Thread.sleep(1200);

        int bullyFailoverLeader = bullyNodes.get(0).getLeaderId();
        int bullyMsgCount = bullyMetrics.getMessageCount();
        long bullyLatencyMs = bullyMetrics.getConvergenceTimeMs();

        System.out.println("Bully Failover Leader: Node " + bullyFailoverLeader);
        System.out.println("Bully Messages Sent:   " + bullyMsgCount);
        System.out.println("Bully Failover Time:   " + bullyLatencyMs + " ms\n");

        // Clean up Bully nodes and registries
        for (BullyElectionNode node : bullyNodes) {
            node.shutdown();
        }
        for (int i = 1; i <= NODE_COUNT; i++) {
            try {
                bullyRegistries.get(i - 1).unbind("election-node-" + i);
                UnicastRemoteObject.unexportObject(bullyRegistries.get(i - 1), true);
            } catch (Exception e) {
                System.err.println("Warning during Bully registry cleanup: " + e.getMessage());
            }
        }
        Thread.sleep(500);

        // ==========================================
        // 2. RING ALGORITHM TEST
        // ==========================================
        System.out.println("--- 2. RING ALGORITHM ---");
        ElectionMetrics ringMetrics = new ElectionMetrics();
        List<RingElectionNode> ringNodes = new ArrayList<>();
        List<Registry> ringRegistries = new ArrayList<>();

        for (int i = 1; i <= NODE_COUNT; i++) {
            int port = BASE_PORT + i;
            RingElectionNode node = new RingElectionNode(i, port, ringOrder, nodePortMap, ringMetrics);
            Registry registry = LocateRegistry.createRegistry(port);
            registry.rebind("election-node-" + i, node);
            ringNodes.add(node);
            ringRegistries.add(registry);
        }

        System.out.println("Bootstrapped 5 Ring nodes on ports 1301..1305 (Ring: 1->2->3->4->5->1).");

        // Initial Ring Election
        System.out.println("Triggering initial Ring election from Node 1...");
        ringNodes.get(0).startElection();
        Thread.sleep(1200);

        int initialRingLeader = ringNodes.get(0).getLeaderId();
        System.out.println("Initial Ring Leader: Node " + initialRingLeader + "\n");

        // Failover Ring Election (Crash Node 5)
        System.out.println("Simulating crash of leader Node " + initialRingLeader + "...");
        ringNodes.get(4).simulateCrash(); // Crash Node 5

        ringMetrics.reset();
        System.out.println("Triggering Ring failover election from Node 1...");
        ringNodes.get(0).startElection();
        Thread.sleep(1200);

        int ringFailoverLeader = ringNodes.get(0).getLeaderId();
        int ringMsgCount = ringMetrics.getMessageCount();
        long ringLatencyMs = ringMetrics.getConvergenceTimeMs();

        System.out.println("Ring Failover Leader: Node " + ringFailoverLeader);
        System.out.println("Ring Messages Sent:   " + ringMsgCount);
        System.out.println("Ring Failover Time:   " + ringLatencyMs + " ms\n");

        // Clean up Ring nodes
        for (RingElectionNode node : ringNodes) {
            node.shutdown();
        }
        for (int i = 1; i <= NODE_COUNT; i++) {
            try {
                ringRegistries.get(i - 1).unbind("election-node-" + i);
                UnicastRemoteObject.unexportObject(ringRegistries.get(i - 1), true);
            } catch (Exception e) {
                System.err.println("Warning during Ring registry cleanup: " + e.getMessage());
            }
        }

        // ==========================================
        // 3. COMPARATIVE METRICS SUMMARY
        // ==========================================
        System.out.println("=================================================");
        System.out.println(" EXPERIMENT 4 SUMMARY: BULLY VS RING ELECTION");
        System.out.println("=================================================");
        System.out.printf("%-15s %-15s %-15s %-15s%n", "Algorithm", "Elected Leader", "Message Count", "Convergence Time");
        System.out.printf("%-15s %-15s %-15s %-15s%n", "Bully", "Node " + bullyFailoverLeader, bullyMsgCount + " msgs", bullyLatencyMs + " ms");
        System.out.printf("%-15s %-15s %-15s %-15s%n", "Ring", "Node " + ringFailoverLeader, ringMsgCount + " msgs", ringLatencyMs + " ms");
        System.out.println("=================================================");
    }
}
