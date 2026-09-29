package agentgrid.node;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Manages OS process lifecycles for cluster nodes.
 * Supports start, kill (destroyForcibly), restart, and liveness inspection across platforms.
 */
public class NodeProcessManager {

    private final Map<Integer, Process> processes = new ConcurrentHashMap<>();
    private volatile String electionAlgorithm = "BULLY";

    /** Election algorithm passed to nodes started from now on. */
    public void setElectionAlgorithm(String algorithm) {
        this.electionAlgorithm = algorithm;
    }

    public synchronized Process start(int nodeId) throws IOException {
        // Kill existing process if currently tracked and alive
        Process existing = processes.get(nodeId);
        if (existing != null && existing.isAlive()) {
            existing.destroyForcibly();
        }

        Path javaBin = resolveJavaExecutable();
        String classpath = System.getProperty("java.class.path");
        Path logsDir = resolveLogsDirectory();
        Files.createDirectories(logsDir);

        Path logFile = logsDir.resolve("node-" + nodeId + ".log");
        ProcessBuilder pb = new ProcessBuilder(
                javaBin.toString(),
                "-cp",
                classpath,
                "-Dagentgrid.election.algorithm=" + electionAlgorithm,
                "agentgrid.node.NodeMain",
                String.valueOf(nodeId)
        );
        pb.redirectErrorStream(true);
        pb.redirectOutput(logFile.toFile());

        Process proc = pb.start();
        processes.put(nodeId, proc);
        return proc;
    }

    public synchronized boolean kill(int nodeId) {
        Process proc = processes.get(nodeId);
        if (proc != null && proc.isAlive()) {
            proc.destroyForcibly();
            try {
                proc.waitFor(1, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            return true;
        }
        return false;
    }

    public synchronized Process restart(int nodeId) throws IOException {
        kill(nodeId);
        return start(nodeId);
    }

    public boolean isAlive(int nodeId) {
        Process proc = processes.get(nodeId);
        return proc != null && proc.isAlive();
    }

    public Process getProcess(int nodeId) {
        return processes.get(nodeId);
    }

    public synchronized void startAll(List<Integer> nodeIds) throws IOException {
        for (int nodeId : nodeIds) {
            start(nodeId);
        }
    }

    public synchronized void stopAll() {
        for (Process proc : processes.values()) {
            if (proc.isAlive()) {
                proc.destroy();
            }
        }
        for (Process proc : processes.values()) {
            try {
                if (proc.isAlive() && !proc.waitFor(2, TimeUnit.SECONDS)) {
                    proc.destroyForcibly();
                }
            } catch (InterruptedException ignored) {
                proc.destroyForcibly();
            }
        }
    }

    public static Path resolveJavaExecutable() {
        Path javaBin = Path.of(System.getProperty("java.home"), "bin", "java");
        if (System.getProperty("os.name", "").toLowerCase().contains("win")) {
            Path exe = Path.of(System.getProperty("java.home"), "bin", "java.exe");
            if (Files.exists(exe)) {
                return exe;
            }
        }
        return javaBin;
    }

    public static Path resolveLogsDirectory() {
        Path backendDir = Path.of("backend");
        if (Files.isDirectory(backendDir)) {
            return backendDir.resolve("build").resolve("logs");
        }
        return Path.of("build", "logs");
    }
}
