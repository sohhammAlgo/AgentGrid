package agentgrid.node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
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

    static final String NODE_INITIAL_HEAP = "16m";
    static final String NODE_MAX_HEAP = "256m";

    private final Map<Integer, Process> processes = new ConcurrentHashMap<>();
    private volatile String electionAlgorithm = "BULLY";

    private volatile boolean clockAutoSync = true;
    /** The control plane's current membership: ports come from it and every node is launched with it. */
    private volatile java.util.function.Supplier<Membership> membership;

    public void setMembershipSource(java.util.function.Supplier<Membership> membership) {
        this.membership = membership;
    }

    /** Election algorithm passed to nodes started from now on. */
    public void setElectionAlgorithm(String algorithm) {
        this.electionAlgorithm = algorithm;
    }

    /** Auto clock sync setting (periodic and rejoin Berkeley rounds) passed to nodes started from now on. */
    public void setClockAutoSync(boolean enabled) {
        this.clockAutoSync = enabled;
    }

    /** Starts (or restarts) a current member with the current membership. */
    public synchronized Process start(int nodeId) throws IOException {
        java.util.function.Supplier<Membership> source = membership;
        if (source == null) {
            return launch(nodeId, ClusterConfig.load().getNode(nodeId).getPort(), null, false);
        }
        Membership m = source.get();
        MemberSpec spec = m.get(nodeId);
        if (spec == null) {
            throw new IOException("node " + nodeId + " is not a member of " + m);
        }
        return launch(nodeId, spec.getPort(), m, false);
    }

    /**
     * Starts a node that is being added: it gets the joining view (current members plus
     * itself at the current epoch) and waits for the next epoch before its first election.
     */
    public synchronized Process startJoining(MemberSpec spec, Membership joiningView) throws IOException {
        return launch(spec.getId(), spec.getPort(), joiningView, true);
    }

    /** Stops a node's process and stops tracking it (a removed member, or a failed add). */
    public synchronized boolean forget(int nodeId) {
        boolean killed = kill(nodeId);
        processes.remove(nodeId);
        return killed;
    }

    /** Pids of the node processes this manager started and still tracks, by node id. */
    public Map<Integer, Long> pids() {
        Map<Integer, Long> out = new java.util.TreeMap<>();
        processes.forEach((id, p) -> {
            if (p.isAlive()) out.put(id, p.pid());
        });
        return out;
    }

    private Process launch(int nodeId, int port, Membership launchMembership, boolean joining) throws IOException {
        // Kill existing process if currently tracked and alive, and wait until it has exited
        Process existing = processes.get(nodeId);
        if (existing != null && existing.isAlive()) {
            existing.destroyForcibly();
            awaitExit(existing);
        }

        // A killed JVM can take a moment to release its registry port, above all when several
        // nodes are restarted at once. A JVM started before that fails to bind and exits, so
        // wait for the port and refuse to start one that is doomed.
        long t0 = System.currentTimeMillis();
        while (!portFree(port)) {
            if (System.currentTimeMillis() - t0 >= PORT_WAIT_MS) {
                System.err.println("[NodeProcessManager] node " + nodeId + ": port " + port
                        + " still in use after " + PORT_WAIT_MS + " ms; not starting it");
                throw new IOException("port " + port + " is still in use " + PORT_WAIT_MS
                        + " ms after node " + nodeId + " was stopped; the node was not started");
            }
            sleep(PORT_POLL_MS);
        }
        long waited = System.currentTimeMillis() - t0;
        if (waited > 0) {
            System.out.println("[NodeProcessManager] node " + nodeId + ": port " + port + " free after " + waited + " ms");
        }

        Path javaBin = resolveJavaExecutable();
        String classpath = System.getProperty("java.class.path");
        Path logsDir = resolveLogsDirectory();
        Files.createDirectories(logsDir);

        Path logFile = logsDir.resolve("node-" + nodeId + ".log");
        List<String> cmd = new java.util.ArrayList<>(List.of(
                javaBin.toString(),
                // A node needs a few MB of heap. Without a cap each JVM commits the default
                // initial heap (1/64 of RAM, ~250 MB here); five of them plus the control plane
                // exhausted Windows commit memory once in Phase 3B (ElectionVerifier run 4).
                "-Xms" + NODE_INITIAL_HEAP,
                "-Xmx" + NODE_MAX_HEAP,
                "-cp",
                classpath,
                "-Dagentgrid.election.algorithm=" + electionAlgorithm,
                "-Dagentgrid.clock.auto=" + clockAutoSync));
        if (launchMembership != null) {
            cmd.add("-Dagentgrid.membership=" + launchMembership.encode());
        }
        if (joining) {
            cmd.add("-Dagentgrid.membership.joining=true");
        }
        cmd.add("agentgrid.node.NodeMain");
        cmd.add(String.valueOf(nodeId));
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        // Append, so the log of a killed or restarted node is kept (one header per start).
        Files.writeString(logFile, System.lineSeparator() + "===== " + java.time.LocalDateTime.now() + " starting node "
                + nodeId + " (clock.auto=" + clockAutoSync + (launchMembership == null ? "" : ", membership " + launchMembership)
                + (joining ? ", joining" : "") + ") =====" + System.lineSeparator(),
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile.toFile()));

        Process proc = pb.start();
        processes.put(nodeId, proc);
        return proc;
    }

    public synchronized boolean kill(int nodeId) {
        Process proc = processes.get(nodeId);
        if (proc != null && proc.isAlive()) {
            proc.destroyForcibly();
            awaitExit(proc);
            return true;
        }
        return false;
    }

    static final long EXIT_WAIT_MS = 5000;
    static final long PORT_WAIT_MS = 5000;
    static final long PORT_POLL_MS = 50;

    private static void awaitExit(Process proc) {
        try {
            proc.waitFor(EXIT_WAIT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    /** True if nothing listens on port: a wildcard bind succeeds (and is released at once). */
    static boolean portFree(int port) {
        try (ServerSocket probe = new ServerSocket()) {
            probe.setReuseAddress(false);
            probe.bind(new InetSocketAddress(port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
