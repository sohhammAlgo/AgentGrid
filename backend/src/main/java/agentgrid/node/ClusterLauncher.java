package agentgrid.node;

import agentgrid.clock.TimeService;
import agentgrid.election.ElectionNodeService;
import agentgrid.replication.ReplicationService;
import agentgrid.rmi.AgentService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Launcher for the AgentGrid-Lite 5-node cluster.
 * Starts nodes 1..5 as independent OS processes via ProcessBuilder,
 * redirects their output to backend/build/logs/node-<id>.log, performs
 * an RMI health check across all 20 services, and registers a shutdown
 * hook to prevent orphaned child processes upon exit.
 *
 * Usage:
 *   java agentgrid.node.ClusterLauncher
 */
public class ClusterLauncher {

    private static final List<Process> childProcesses = new CopyOnWriteArrayList<>();

    public static void main(String[] args) {
        try {
            ClusterConfig config = ClusterConfig.load();
            List<Integer> nodeIds = config.getNodeIds();

            Path javaBin = resolveJavaExecutable();
            String classpath = System.getProperty("java.class.path");
            Path logsDir = resolveLogsDirectory();
            Files.createDirectories(logsDir);

            // Register shutdown hook first so Ctrl-C cleanly kills all children
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n[ClusterLauncher] Shutdown signal received; terminating child processes...");
                for (Process p : childProcesses) {
                    if (p.isAlive()) {
                        p.destroy();
                    }
                }
                for (Process p : childProcesses) {
                    try {
                        if (p.isAlive() && !p.waitFor(2, TimeUnit.SECONDS)) {
                            p.destroyForcibly();
                        }
                    } catch (InterruptedException ignored) {
                        p.destroyForcibly();
                    }
                }
                System.out.println("[ClusterLauncher] All cluster node processes stopped.");
            }));

            System.out.println("[ClusterLauncher] Launching " + nodeIds.size() + " cluster nodes...");

            for (int nodeId : nodeIds) {
                Path logFile = logsDir.resolve("node-" + nodeId + ".log");
                ProcessBuilder pb = new ProcessBuilder(
                        javaBin.toString(),
                        "-cp",
                        classpath,
                        "agentgrid.node.NodeMain",
                        String.valueOf(nodeId)
                );
                pb.redirectErrorStream(true);
                pb.redirectOutput(logFile.toFile());

                Process proc = pb.start();
                childProcesses.add(proc);
                System.out.println("  -> Node " + nodeId + " process spawned (pid=" + proc.pid()
                        + ", log=" + logFile + ")");
            }

            // Await node startup and perform health check
            System.out.println("\n[ClusterLauncher] Performing health checks on all nodes...");
            awaitClusterReadiness(config, 6000);

            printHealthCheckTable(config);

            System.out.println("\nCluster running with " + nodeIds.size()
                    + " nodes (20 services total). Press Ctrl-C to stop.");

            // Keep launcher alive until interrupted or terminated
            synchronized (ClusterLauncher.class) {
                ClusterLauncher.class.wait();
            }

        } catch (InterruptedException e) {
            System.out.println("[ClusterLauncher] Interrupted, exiting.");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("[ClusterLauncher] Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Path resolveJavaExecutable() {
        Path javaBin = Path.of(System.getProperty("java.home"), "bin", "java");
        if (System.getProperty("os.name", "").toLowerCase().contains("win")) {
            Path exe = Path.of(System.getProperty("java.home"), "bin", "java.exe");
            if (Files.exists(exe)) {
                return exe;
            }
        }
        return javaBin;
    }

    private static Path resolveLogsDirectory() {
        Path backendDir = Path.of("backend");
        if (Files.isDirectory(backendDir)) {
            return backendDir.resolve("build").resolve("logs");
        }
        return Path.of("build", "logs");
    }

    private static void awaitClusterReadiness(ClusterConfig config, long timeoutMs) {
        long start = System.currentTimeMillis();
        boolean allReady = false;

        while (System.currentTimeMillis() - start < timeoutMs && !allReady) {
            allReady = true;
            for (int id : config.getNodeIds()) {
                ClusterConfig.NodeConfig nc = config.getNode(id);
                try {
                    Registry reg = LocateRegistry.getRegistry("localhost", nc.getPort());
                    AgentService agent = (AgentService) reg.lookup("agent");
                    if (!agent.ping()) {
                        allReady = false;
                        break;
                    }
                } catch (Exception e) {
                    allReady = false;
                    break;
                }
            }
            if (!allReady) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static void printHealthCheckTable(ClusterConfig config) {
        String format = "| %-4s | %-4s | %-4s | %-9s | %-5s | %-4s | %-8s | %-10s |%n";
        String separator = "+------+------+------+-----------+-------+------+----------+------------+";

        System.out.println(separator);
        System.out.printf(format, "Node", "Port", "Pool", "Drift", "Agent", "Time", "Election", "Blackboard");
        System.out.println(separator);

        for (int id : config.getNodeIds()) {
            ClusterConfig.NodeConfig nc = config.getNode(id);
            int port = nc.getPort();
            int pool = nc.getPoolSize();
            long drift = nc.getClockDriftMs();
            String driftStr = (drift >= 0 ? "+" : "") + drift + "ms";

            String agentStatus = "NO";
            String timeStatus = "NO";
            String electionStatus = "NO";
            String bbStatus = "NO";

            try {
                Registry reg = LocateRegistry.getRegistry("localhost", port);

                try {
                    AgentService agent = (AgentService) reg.lookup("agent");
                    if (agent.ping()) {
                        agentStatus = "YES";
                    }
                } catch (Exception ignored) {}

                try {
                    TimeService time = (TimeService) reg.lookup("time");
                    time.getTime();
                    timeStatus = "YES";
                } catch (Exception ignored) {}

                try {
                    ElectionNodeService election = (ElectionNodeService) reg.lookup("election-node-" + id);
                    if (election.isAlive()) {
                        electionStatus = "YES";
                    }
                } catch (Exception ignored) {}

                try {
                    ReplicationService bb = (ReplicationService) reg.lookup("blackboard-node-node-" + id);
                    if (bb.getNodeId() != null) {
                        bbStatus = "YES";
                    }
                } catch (Exception ignored) {}

            } catch (Exception ignored) {}

            System.out.printf(format, id, port, pool, driftStr, agentStatus, timeStatus, electionStatus, bbStatus);
        }

        System.out.println(separator);
    }
}
