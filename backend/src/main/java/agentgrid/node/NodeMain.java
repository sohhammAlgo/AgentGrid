package agentgrid.node;

import agentgrid.clock.LamportClock;
import agentgrid.clock.TimeServiceImpl;
import agentgrid.orchestrator.Corpus;
import agentgrid.orchestrator.Orchestrator;
import agentgrid.orchestrator.StrategyRegistry;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Main entry point for a single integrated cluster node process.
 * Hosts the node services inside one JVM process and binds them to one RMI registry
 * on the node's dedicated port: "agent", "time", "election-node-<id>",
 * "blackboard-node-node-<id>" and "telemetry".
 *
 * Usage:
 *   java [-Dagentgrid.election.algorithm=BULLY|RING] agentgrid.node.NodeMain <nodeId>
 */
public class NodeMain {

    /**
     * Bounds how long a node's outgoing RMI call waits for a reply once connected. Election
     * handlers return at once, but the leader's orchestrator blocks in execute() while a
     * subtask waits in a worker's queue, so this must exceed a subtask's queue wait plus work.
     */
    private static final String RMI_RESPONSE_TIMEOUT_MS = "10000";

    // Retain strong references to remote objects to prevent distributed garbage collection
    private static NodeAgentService agentService;
    private static TimeServiceImpl timeService;
    private static ElectionNode electionNode;
    private static FailureDetector failureDetector;
    private static NodeEventBuffer telemetry;
    private static Orchestrator orchestrator;
    private static ClusterBlackboard blackboard;
    private static ClockCoordinator clockCoordinator;
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

        // Must be set before the RMI transport initialises.
        if (System.getProperty("sun.rmi.transport.tcp.responseTimeout") == null) {
            System.setProperty("sun.rmi.transport.tcp.responseTimeout", RMI_RESPONSE_TIMEOUT_MS);
        }

        try {
            ClusterConfig config = ClusterConfig.load();
            ClusterConfig.NodeConfig nodeConfig = config.getNode(nodeId);

            int port = nodeConfig.getPort();
            int poolSize = nodeConfig.getPoolSize();
            long driftMs = nodeConfig.getClockDriftMs();
            String algorithm = System.getProperty("agentgrid.election.algorithm", ElectionNode.BULLY);

            registry = LocateRegistry.createRegistry(port);

            LamportClock lamportClock = new LamportClock();

            agentService = new NodeAgentService("agent-" + nodeId, poolSize, lamportClock,
                    StrategyRegistry.defaults(Corpus.load()), config.getSimulatedWorkMs());
            registry.rebind("agent", agentService);

            timeService = new TimeServiceImpl("node-" + nodeId, driftMs);
            registry.rebind("time", timeService);

            telemetry = new NodeEventBuffer(nodeId, lamportClock, timeService);
            registry.rebind("telemetry", telemetry);

            LeaderLifecycleRegistry lifecycle = new LeaderLifecycleRegistry();
            lifecycle.register(LeaderLifecycleRegistry.eventEmitter(telemetry));

            // Every node hosts an orchestrator; it runs only while this node is the leader.
            orchestrator = new Orchestrator(nodeId, config, telemetry);
            registry.rebind("orchestrator", orchestrator);
            lifecycle.register(orchestrator);

            // Berkeley clock sync: every node has a coordinator; only the leader's runs rounds.
            boolean autoSync = Boolean.parseBoolean(System.getProperty("agentgrid.clock.auto", "true"));
            clockCoordinator = new ClockCoordinator(nodeId, config, timeService, telemetry, autoSync);
            registry.rebind("clock", clockCoordinator);
            lifecycle.register(clockCoordinator);

            electionNode = new ElectionNode(nodeId, config.getNodePortMap(), algorithm, telemetry, lifecycle);
            registry.rebind("election-node-" + nodeId, electionNode);

            // Replicated blackboard (Exp 5), under the submitted binding name; it also serves the
            // submitted ReplicationService methods.
            ElectionNode election = electionNode;
            blackboard = new ClusterBlackboard(nodeId, config, timeService, telemetry, clockCoordinator,
                    election::leader);
            registry.rebind("blackboard-node-node-" + nodeId, blackboard);
            agentService.setBlackboard(blackboard);

            failureDetector = new FailureDetector(electionNode);
            agentService.setSyncListener(failureDetector::onSync);
            failureDetector.start();
            blackboard.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[Node " + nodeId + "] JVM shutting down; releasing thread pools...");
                if (failureDetector != null) {
                    failureDetector.shutdown();
                }
                if (agentService != null) {
                    agentService.shutdown();
                }
                if (electionNode != null) {
                    electionNode.shutdown();
                }
                if (blackboard != null) {
                    blackboard.shutdown();
                }
                if (clockCoordinator != null) {
                    clockCoordinator.shutdown();
                }
            }));

            System.out.println("[Node " + nodeId + "] Started successfully on port " + port
                    + " (pool=" + poolSize
                    + ", drift=" + (driftMs >= 0 ? "+" : "") + driftMs + "ms"
                    + ", election=" + algorithm + ")");
            System.out.println("[Node " + nodeId + "] Registered services: 'agent', 'time', 'election-node-"
                    + nodeId + "', 'blackboard-node-node-" + nodeId + "', 'telemetry', 'orchestrator', 'clock'");

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
