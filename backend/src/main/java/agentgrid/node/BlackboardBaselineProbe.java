package agentgrid.node;

import agentgrid.replication.BlackboardEntry;
import agentgrid.replication.ConsistencyLevel;
import agentgrid.replication.ReplicatedBlackboardNode;

import java.io.OutputStream;
import java.io.PrintStream;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * "Before" measurements for the Exp 5 fixes in FIXES.md, against the SUBMITTED
 * ReplicatedBlackboardNode (unmodified), three nodes in one JVM on ports 1731-1733.
 * A node is taken "down" by unbinding its registry name, so peers' lookups fail
 * (the submitted code treats that like an unreachable node).
 *
 * Usage (from backend/): java -cp build/classes agentgrid.node.BlackboardBaselineProbe
 */
public class BlackboardBaselineProbe {

    private static final String[] IDS = {"node-1", "node-2", "node-3"};
    private static final int[] PORTS = {1731, 1732, 1733};
    private static final ReplicatedBlackboardNode[] NODES = new ReplicatedBlackboardNode[3];
    private static final Registry[] REGISTRIES = new Registry[3];

    public static void main(String[] args) throws Exception {
        PrintStream out = System.out;
        // The submitted class prints a line per update; keep the probe's own output readable.
        System.setOut(new PrintStream(OutputStream.nullOutputStream()));
        System.setErr(new PrintStream(OutputStream.nullOutputStream()));
        Map<String, Integer> peers = new LinkedHashMap<>();
        for (int i = 0; i < 3; i++) {
            peers.put(IDS[i], PORTS[i]);
        }
        for (int i = 0; i < 3; i++) {
            NODES[i] = new ReplicatedBlackboardNode(IDS[i], PORTS[i], peers);
            REGISTRIES[i] = LocateRegistry.createRegistry(PORTS[i]);
            up(i);
        }

        out.println("=== submitted ReplicatedBlackboardNode, 3 nodes in one JVM ===");

        // 1. LWW tie: equal timestamps arriving in opposite orders.
        BlackboardEntry one = new BlackboardEntry("t", "tie", "one", "node-1", 1000, 1);
        BlackboardEntry two = new BlackboardEntry("t", "tie", "two", "node-2", 1000, 1);
        NODES[1].receiveReplicate(one);
        NODES[1].receiveReplicate(two);
        NODES[2].receiveReplicate(two);
        NODES[2].receiveReplicate(one);
        out.println("1 LWW tie (ts 1000 from node-1 and node-2, opposite arrival orders): node-2 holds '"
                + NODES[1].getFinding("tie").getFindingPayload() + "', node-3 holds '"
                + NODES[2].getFinding("tie").getFindingPayload() + "' -> replicas diverge");

        // 2. STRONG with one node down.
        down(2);
        int falseCount = 0;
        int partial = 0;
        for (int i = 0; i < 5; i++) {
            String key = "strong-" + i;
            boolean ok = NODES[0].publishFinding(new BlackboardEntry("t", key, "v" + i, "node-1",
                    System.currentTimeMillis() + i, 1), ConsistencyLevel.STRONG);
            if (!ok) {
                falseCount++;
            }
            int holders = count(key);
            if (!ok && holders > 0) {
                partial++;
            }
        }
        out.println("2 STRONG with node-3 down: " + falseCount + "/5 writes returned false; " + partial
                + "/5 of those 'failed' writes are nevertheless held by " + count("strong-0") + " of 3 nodes (origin + live peer)");

        // 3. STRONG when a peer already holds a newer timestamp.
        up(2);
        NODES[1].receiveReplicate(new BlackboardEntry("t", "newer", "peer-newer", "node-2", 5000, 1));
        boolean ok3 = NODES[0].publishFinding(new BlackboardEntry("t", "newer", "older-write", "node-1", 4000, 1),
                ConsistencyLevel.STRONG);
        out.println("3 STRONG ts 4000 while node-2 already holds ts 5000: publishFinding returned " + ok3
                + " although every node holds that key (node-2 the newer value)");

        // 4. EVENTUAL while a peer is down: never delivered.
        down(2);
        NODES[0].publishFinding(new BlackboardEntry("t", "ev-down", "eventual", "node-1", System.currentTimeMillis(), 1),
                ConsistencyLevel.EVENTUAL);
        Thread.sleep(500);
        up(2);
        Thread.sleep(3000);
        out.println("4 EVENTUAL written while node-3 was down, node-3 back for 3 s: node-3 holds it = "
                + (NODES[2].getFinding("ev-down") != null) + " (no retry, no anti-entropy)");

        // 5. A node that (re)joins empty is never caught up and serves reads at once.
        int before = NODES[0].getAllFindings().size();
        ReplicatedBlackboardNode fresh = new ReplicatedBlackboardNode("node-3", PORTS[2], peers);
        REGISTRIES[2].rebind("blackboard-node-node-3", fresh);
        Thread.sleep(3000);
        out.println("5 restarted node-3 (new empty instance) after 3 s: holds " + fresh.getAllFindings().size() + " of "
                + before + " entries node-1 holds; getFinding(\"strong-0\") = " + fresh.getFinding("strong-0")
                + " (no snapshot pull, no ready flag)");

        // 6. Keys: two tasks with the same subtask id.
        NODES[0].publishFinding(new BlackboardEntry("job-A", "summarize-1", "finding of job A", "node-1", 7000, 1),
                ConsistencyLevel.STRONG);
        NODES[0].publishFinding(new BlackboardEntry("job-B", "summarize-1", "finding of job B", "node-1", 7001, 1),
                ConsistencyLevel.STRONG);
        out.println("6 two jobs each write subtask 'summarize-1': node-1 keeps 1 entry, taskId="
                + NODES[0].getFinding("summarize-1").getTaskId() + " -> job-A's finding is lost");

        // 8 and 12: what the interface and metrics offer.
        out.println("8 ReplicationService remote methods: " + agentgrid.replication.ReplicationService.class.getDeclaredMethods().length
                + " (no quorum check, no ready flag, no snapshot metadata, no writer id); ClusterBlackboardService adds "
                + ClusterBlackboardService.class.getDeclaredMethods().length);
        out.println("12 ReplicationMetrics exposes an average write latency and a stale-read count; no median and no refused-write count");

        for (ReplicatedBlackboardNode n : NODES) {
            n.shutdown();
        }
        fresh.shutdown();
        System.exit(0);
    }

    private static void up(int i) throws Exception {
        REGISTRIES[i].rebind("blackboard-node-" + IDS[i], NODES[i]);
    }

    private static void down(int i) throws Exception {
        REGISTRIES[i].unbind("blackboard-node-" + IDS[i]);
    }

    private static int count(String key) {
        int n = 0;
        for (ReplicatedBlackboardNode node : NODES) {
            if (node.getFinding(key) != null) {
                n++;
            }
        }
        return n;
    }
}
