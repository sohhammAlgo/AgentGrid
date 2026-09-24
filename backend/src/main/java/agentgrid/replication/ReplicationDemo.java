package agentgrid.replication;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Experiment 5 demonstration:
 * 1. Bootstraps 3 Replicated Blackboard Nodes (node-1, node-2, node-3 on ports 1401..1403).
 * 2. Runs Strong Consistency workload: synchronous multi-node write, measuring write latency and verifying 0% stale reads.
 * 3. Runs Eventual Consistency workload: async write with immediate local response, measuring write latency and monitoring convergence time & stale reads.
 * 4. Verifies Last-Write-Wins (LWW) conflict resolution for concurrent/out-of-order agent findings.
 * 5. Outputs a comparative summary table for the lab report.
 */
public class ReplicationDemo {

    private static final String[] NODE_IDS = { "node-1", "node-2", "node-3" };
    private static final int[] PORTS = { 1401, 1402, 1403 };

    public static void main(String[] args) throws Exception {
        System.out.println("=================================================");
        System.out.println(" AgentGrid-Lite: Experiment 5 (Replicated Blackboard)");
        System.out.println(" Comparing Strong vs Eventual Consistency Models");
        System.out.println("=================================================\n");

        Map<String, Integer> peerMap = new HashMap<>();
        for (int i = 0; i < NODE_IDS.length; i++) {
            peerMap.put(NODE_IDS[i], PORTS[i]);
        }

        List<ReplicatedBlackboardNode> nodes = new ArrayList<>();
        List<Registry> registries = new ArrayList<>();

        for (int i = 0; i < NODE_IDS.length; i++) {
            ReplicatedBlackboardNode node = new ReplicatedBlackboardNode(NODE_IDS[i], PORTS[i], peerMap);
            Registry registry = LocateRegistry.createRegistry(PORTS[i]);
            registry.rebind("blackboard-node-" + NODE_IDS[i], node);
            nodes.add(node);
            registries.add(registry);
        }

        System.out.println("Bootstrapped 3 Replicated Blackboard Nodes on ports 1401..1403.\n");

        ReplicationMetrics strongMetrics = new ReplicationMetrics();
        ReplicationMetrics eventualMetrics = new ReplicationMetrics();

        // ==========================================
        // 1. STRONG CONSISTENCY BENCHMARK
        // ==========================================
        System.out.println("--- 1. STRONG CONSISTENCY BENCHMARK ---");
        for (int i = 0; i < 5; i++) {
            String subtaskId = "subtask-summary-" + i;
            BlackboardEntry entry = new BlackboardEntry("task-1", subtaskId, "Summary of chunk-" + i, "agent-node-1", System.currentTimeMillis() + i, 1);

            long start = System.currentTimeMillis();
            nodes.get(0).publishFinding(entry, ConsistencyLevel.STRONG);
            long latency = System.currentTimeMillis() - start;
            strongMetrics.recordWriteLatency(latency);

            // Verify reads across all 3 nodes immediately
            for (ReplicatedBlackboardNode node : nodes) {
                BlackboardEntry read = node.getFinding(subtaskId);
                boolean isStale = (read == null || !read.getFindingPayload().equals(entry.getFindingPayload()));
                strongMetrics.recordRead(isStale);
            }
        }

        System.out.printf("Strong Mode Avg Write Latency: %.2f ms%n", strongMetrics.getAverageWriteLatencyMs());
        System.out.printf("Strong Mode Stale Reads:      %d / %d (%.1f%%)%n%n", strongMetrics.getStaleReadCount(), strongMetrics.getTotalReadCount(), strongMetrics.getStalenessPercentage());

        // ==========================================
        // 2. EVENTUAL CONSISTENCY BENCHMARK
        // ==========================================
        System.out.println("--- 2. EVENTUAL CONSISTENCY BENCHMARK ---");
        long eventualConvergenceStart = System.currentTimeMillis();

        for (int i = 0; i < 5; i++) {
            String subtaskId = "subtask-rank-" + i;
            BlackboardEntry entry = new BlackboardEntry("task-2", subtaskId, "Ranked score " + (i * 10), "agent-node-2", System.currentTimeMillis() + i, 1);

            long start = System.currentTimeMillis();
            nodes.get(1).publishFinding(entry, ConsistencyLevel.EVENTUAL);
            long latency = System.currentTimeMillis() - start;
            eventualMetrics.recordWriteLatency(latency);

            // Immediate query to peer nodes (node-1 & node-3) right after write returns
            for (ReplicatedBlackboardNode node : nodes) {
                BlackboardEntry read = node.getFinding(subtaskId);
                boolean isStale = (read == null || !read.getFindingPayload().equals(entry.getFindingPayload()));
                eventualMetrics.recordRead(isStale);
            }
        }

        // Poll until all nodes converge on state
        boolean converged = false;
        long pollStart = System.currentTimeMillis();
        while (System.currentTimeMillis() - pollStart < 2000) {
            Thread.sleep(50);
            if (nodes.get(0).getAllFindings().size() == 10 &&
                nodes.get(1).getAllFindings().size() == 10 &&
                nodes.get(2).getAllFindings().size() == 10) {
                converged = true;
                break;
            }
        }
        long eventualConvergenceDuration = System.currentTimeMillis() - eventualConvergenceStart;

        System.out.printf("Eventual Mode Avg Write Latency: %.2f ms%n", eventualMetrics.getAverageWriteLatencyMs());
        System.out.printf("Eventual Mode Immediate Staleness: %d / %d (%.1f%%)%n", eventualMetrics.getStaleReadCount(), eventualMetrics.getTotalReadCount(), eventualMetrics.getStalenessPercentage());
        System.out.printf("Convergence Reached:               %b (%d ms total)%n%n", converged, eventualConvergenceDuration);

        // ==========================================
        // 3. LAST-WRITE-WINS (LWW) CONFLICT RESOLUTION
        // ==========================================
        System.out.println("--- 3. LAST-WRITE-WINS (LWW) CONFLICT RESOLUTION TEST ---");
        String testKey = "subtask-conflict-1";

        BlackboardEntry baseEntry = new BlackboardEntry("task-3", testKey, "Initial finding", "agent-1", 1000L, 1L);
        BlackboardEntry newerEntry = new BlackboardEntry("task-3", testKey, "Fresher finding from Agent 2", "agent-2", 1050L, 2L);
        BlackboardEntry staleEntry = new BlackboardEntry("task-3", testKey, "Out-of-order stale finding", "agent-3", 950L, 1L);

        nodes.get(0).publishFinding(baseEntry, ConsistencyLevel.STRONG);
        nodes.get(1).publishFinding(newerEntry, ConsistencyLevel.STRONG);
        nodes.get(2).publishFinding(staleEntry, ConsistencyLevel.STRONG); // Should be rejected by LWW

        BlackboardEntry finalState = nodes.get(0).getFinding(testKey);
        System.out.println("Final LWW Blackboard Payload: '" + finalState.getFindingPayload() + "' (timestamp " + finalState.getTimestamp() + ")\n");

        // Clean up nodes
        for (ReplicatedBlackboardNode node : nodes) {
            node.shutdown();
        }
        for (int i = 0; i < NODE_IDS.length; i++) {
            try {
                registries.get(i).unbind("blackboard-node-" + NODE_IDS[i]);
                UnicastRemoteObject.unexportObject(registries.get(i), true);
            } catch (Exception e) {}
        }

        // ==========================================
        // 4. SUMMARY REPORT
        // ==========================================
        System.out.println("=================================================");
        System.out.println(" EXPERIMENT 5 SUMMARY: CONSISTENCY MODEL COMPARISON");
        System.out.println("=================================================");
        System.out.printf("%-18s %-20s %-18s %-15s%n", "Consistency Mode", "Avg Write Latency", "Stale Reads %", "Convergence");
        System.out.printf("%-18s %-20s %-18s %-15s%n", "STRONG", String.format("%.2f ms", strongMetrics.getAverageWriteLatencyMs()), String.format("%.1f%%", strongMetrics.getStalenessPercentage()), "0 ms (Instant)");
        System.out.printf("%-18s %-20s %-18s %-15s%n", "EVENTUAL", String.format("%.2f ms", eventualMetrics.getAverageWriteLatencyMs()), String.format("%.1f%%", eventualMetrics.getStalenessPercentage()), eventualConvergenceDuration + " ms");
        System.out.println("=================================================");
    }
}
