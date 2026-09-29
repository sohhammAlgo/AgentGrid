package agentgrid.node;

import agentgrid.clock.TimeServiceImpl;
import agentgrid.election.BullyElectionNode;
import agentgrid.replication.ReplicatedBlackboardNode;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Main entry point for a single integrated cluster node process.
 * Hosts all 4 node services inside one JVM process and binds them to one RMI registry
 * on the node's dedicated port.
 *
 * Usage:
 *   java agentgrid.node.NodeMain <nodeId>
 */
public class NodeMain {

    // Retain strong references to remote objects to prevent distributed garbage collection
    private static NodeAgentService agentService;
    private static TimeServiceImpl timeService;
    private static BullyElectionNode electionNode;
    private static ReplicatedBlackboardNode blackboardNode;
    private static Registry registry;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java agentgrid.node.NodeMain <nodeId>");
            System.exit(1);
        }

        int nodeId;
        try {
            nodeId = Integer.parseInt(args[0].trim());
        } catch (NumberFormatException e) {
            System.err.println("Error: nodeId must be an integer, got: " + args[0]);
            System.exit(1);
            return;
        }

        try {
            ClusterConfig config = ClusterConfig.load();
            ClusterConfig.NodeConfig nodeConfig = config.getNode(nodeId);

            int port = nodeConfig.getPort();
            int poolSize = nodeConfig.getPoolSize();
            long driftMs = nodeConfig.getClockDriftMs();

            registry = LocateRegistry.createRegistry(port);

            agentService = new NodeAgentService("agent-" + nodeId, poolSize);
            registry.rebind("agent", agentService);

            timeService = new TimeServiceImpl("node-" + nodeId, driftMs);
            registry.rebind("time", timeService);

            electionNode = new BullyElectionNode(nodeId, port, config.getNodePortMap(), null);
            registry.rebind("election-node-" + nodeId, electionNode);

            blackboardNode = new ReplicatedBlackboardNode("node-" + nodeId, port, config.getPeerPortMap());
            registry.rebind("blackboard-node-node-" + nodeId, blackboardNode);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[Node " + nodeId + "] JVM shutting down; releasing thread pools...");
                if (agentService != null) {
                    agentService.shutdown();
                }
                if (electionNode != null) {
                    electionNode.shutdown();
                }
                if (blackboardNode != null) {
                    blackboardNode.shutdown();
                }
            }));

            System.out.println("[Node " + nodeId + "] Started successfully on port " + port
                    + " (pool=" + poolSize
                    + ", drift=" + (driftMs >= 0 ? "+" : "") + driftMs + "ms)");
            System.out.println("[Node " + nodeId + "] Registered services: 'agent', 'time', 'election-node-"
                    + nodeId + "', 'blackboard-node-node-" + nodeId + "'");

            // Keep process running indefinitely until terminated
            synchronized (NodeMain.class) {
                NodeMain.class.wait();
            }

        } catch (InterruptedException e) {
            System.out.println("[Node " + nodeId + "] Interrupted, exiting.");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("[Node " + nodeId + "] Fatal startup failure: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
