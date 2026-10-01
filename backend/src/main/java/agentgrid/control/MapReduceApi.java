package agentgrid.control;

import agentgrid.node.ClusterConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Exp 7 from the control plane: runs mapreduce/exp7_mapreduce.py --json (PySpark in local mode
 * on this machine, not on the cluster nodes) and reports its one-line JSON result.
 *
 * One run at a time (409 otherwise). The Python process gets 300 s; a missing interpreter or
 * pyspark, a timeout, a non-zero exit or output without a JSON line is a 503 whose message
 * includes the last 20 lines the script wrote to stderr. After a successful run the
 * IndexStore reloads mapreduce/out/inverted_index.json.
 *
 * Events: MAPREDUCE_STARTED, MAPREDUCE_COMPLETE (job, ms, verified), MAPREDUCE_FAILED (reason).
 */
public class MapReduceApi {

    static final String SCRIPT = "mapreduce/exp7_mapreduce.py";
    static final long TIMEOUT_SECONDS = 300;
    private static final int STDERR_TAIL = 20;

    private final ClusterConfig config;
    private final EventLog eventLog;
    private final IndexStore indexStore;
    private final Path repoRoot;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Map<String, Object> last;
    private volatile Map<String, Object> current;
    private volatile Map<String, Object> lastFailure;

    public MapReduceApi(ClusterConfig config, EventLog eventLog, IndexStore indexStore, Path repoRoot) {
        this.config = config;
        this.eventLog = eventLog;
        this.indexStore = indexStore;
        this.repoRoot = repoRoot;
    }

    /**
     * The repository root: -Dagentgrid.repoRoot, else the working directory or its parent,
     * whichever holds mapreduce/exp7_mapreduce.py (the control plane runs from backend/).
     */
    public static Path resolveRepoRoot() {
        String configured = System.getProperty("agentgrid.repoRoot");
        if (configured != null && !configured.isBlank()) {
            return Path.of(configured).toAbsolutePath().normalize();
        }
        for (Path p : new Path[] {Path.of("."), Path.of("..")}) {
            if (Files.isRegularFile(p.resolve(SCRIPT))) {
                return p.toAbsolutePath().normalize();
            }
        }
        return Path.of("..").toAbsolutePath().normalize();
    }

    public static Path indexFile(Path repoRoot) {
        return repoRoot.resolve("mapreduce").resolve("out").resolve("inverted_index.json");
    }

    public boolean isRunning() {
        return running.get();
    }

    /** POST /api/mapreduce/run. Blocks until the script finishes (about 30 s for one index pass). */
    public Map<String, Object> run(String job, int partitions, boolean compare) throws JobDirectory.ApiException {
        if (!running.compareAndSet(false, true)) {
            Map<String, Object> c = current;
            long since = c == null ? 0 : (System.currentTimeMillis() - (Long) c.get("startedAtMs")) / 1000;
            throw new JobDirectory.ApiException(409, "a MapReduce run is already in progress (job "
                    + (c == null ? "?" : c.get("job")) + ", started " + since + " s ago); wait for it to finish");
        }
        long start = System.currentTimeMillis();
        try {
            Map<String, Object> cur = new LinkedHashMap<>();
            cur.put("job", job);
            cur.put("partitions", partitions);
            cur.put("compare", compare);
            cur.put("startedAtMs", start);
            current = cur;
            List<String> command = command(job, partitions, compare);
            Map<String, Object> f = new LinkedHashMap<>(cur);
            f.remove("startedAtMs");
            eventLog.record("MAPREDUCE_STARTED", 0, "MapReduce " + job + " started (" + partitions + " partitions"
                    + (compare ? ", also local[1]" : "") + "): " + String.join(" ", command), start, f);
            try {
                Map<String, Object> result = execute(command);
                long ms = System.currentTimeMillis() - start;
                result.put("wallMs", ms);
                Map<String, Object> index = indexStore.reload();
                result.put("index", index);
                last = result;
                boolean verified = Boolean.TRUE.equals(result.get("verified"));
                Map<String, Object> done = new LinkedHashMap<>();
                done.put("job", job);
                done.put("ms", ms);
                done.put("verified", verified);
                done.put("indexUsable", index.get("usable"));
                eventLog.record("MAPREDUCE_COMPLETE", 0, "MapReduce " + job + " complete in " + ms + " ms, verified="
                        + verified + (verified ? "" : " (Spark and plain Python disagree; no files written)")
                        + "; index " + (Boolean.TRUE.equals(index.get("usable")) ? "usable"
                        : "not usable: " + index.get("reason")), System.currentTimeMillis(), done);
                lastFailure = null;
                return result;
            } catch (JobDirectory.ApiException e) {
                long ms = System.currentTimeMillis() - start;
                Map<String, Object> fail = new LinkedHashMap<>();
                fail.put("job", job);
                fail.put("ms", ms);
                fail.put("reason", oneLine(e.getMessage()));
                Map<String, Object> failure = new LinkedHashMap<>(fail);
                failure.put("message", e.getMessage());
                failure.put("atMs", System.currentTimeMillis());
                lastFailure = failure;
                eventLog.record("MAPREDUCE_FAILED", 0, "MapReduce " + job + " failed after " + ms + " ms: "
                        + oneLine(e.getMessage()), System.currentTimeMillis(), fail);
                throw e;
            }
        } finally {
            current = null;
            running.set(false);
        }
    }

    /** The interpreter (pythonCommand), the script and its arguments. */
    private List<String> command(String job, int partitions, boolean compare) {
        List<String> cmd = new ArrayList<>();
        String python = config.getPythonCommand();
        if (Files.isRegularFile(Path.of(python))) {
            cmd.add(python);                                // a path, possibly with spaces
        } else {
            cmd.addAll(Arrays.asList(python.trim().split("\\s+")));   // e.g. "python" or "py -3"
        }
        cmd.add(repoRoot.resolve(SCRIPT).toString());
        cmd.add("--json");
        cmd.add("--job");
        cmd.add(job);
        cmd.add("--partitions");
        cmd.add(String.valueOf(partitions));
        if (compare) {
            cmd.add("--compare");
        }
        return cmd;
    }

    private Map<String, Object> execute(List<String> command) throws JobDirectory.ApiException {
        if (!Files.isRegularFile(repoRoot.resolve(SCRIPT))) {
            throw new JobDirectory.ApiException(503, "script not found: " + repoRoot.resolve(SCRIPT)
                    + " (set -Dagentgrid.repoRoot to the repository root)");
        }
        ProcessBuilder pb = new ProcessBuilder(command).directory(repoRoot.toFile());
        Map<String, String> env = pb.environment();
        env.put("PYTHONUNBUFFERED", "1");
        env.put("PYTHONIOENCODING", "utf-8");
        if (env.get("JAVA_HOME") == null || env.get("JAVA_HOME").isBlank()) {
            // Spark needs Java 17+; the control plane's own JVM is one.
            env.put("JAVA_HOME", System.getProperty("java.home"));
        }
        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            throw new JobDirectory.ApiException(503, "could not start the Python interpreter '" + config.getPythonCommand()
                    + "': " + e.getMessage() + ". Set pythonCommand in cluster.properties or -Dagentgrid.pythonCommand"
                    + " to a Python with pyspark installed (mapreduce/requirements.txt).");
        }
        StringBuilder stdout = new StringBuilder();
        Deque<String> stderrTail = new ArrayDeque<>();
        Thread outReader = drain(process.getInputStream(), line -> {
            synchronized (stdout) {
                stdout.append(line).append('\n');
            }
        }, "mapreduce-stdout");
        Thread errReader = drain(process.getErrorStream(), line -> {
            synchronized (stderrTail) {
                stderrTail.addLast(line);
                while (stderrTail.size() > STDERR_TAIL) {
                    stderrTail.removeFirst();
                }
            }
        }, "mapreduce-stderr");
        boolean finished;
        try {
            finished = process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            destroyTree(process);
            throw new JobDirectory.ApiException(503, "interrupted while waiting for the MapReduce run");
        }
        if (!finished) {
            destroyTree(process);
            join(outReader, errReader);
            throw new JobDirectory.ApiException(503, "MapReduce run timed out after " + TIMEOUT_SECONDS
                    + " s and was stopped" + tail(stderrTail));
        }
        join(outReader, errReader);
        int exit = process.exitValue();
        if (exit != 0) {
            throw new JobDirectory.ApiException(503, "the MapReduce script exited with status " + exit
                    + " (is pyspark installed for '" + config.getPythonCommand() + "', and Java 17+ available?)"
                    + tail(stderrTail));
        }
        String json = null;
        synchronized (stdout) {
            for (String line : stdout.toString().split("\n")) {
                if (line.trim().startsWith("{")) {
                    json = line.trim();   // the last JSON line wins
                }
            }
        }
        if (json == null) {
            throw new JobDirectory.ApiException(503, "the MapReduce script printed no JSON result" + tail(stderrTail));
        }
        try {
            return JsonUtil.parseObject(json);
        } catch (RuntimeException e) {
            throw new JobDirectory.ApiException(503, "the MapReduce script's JSON could not be parsed: " + e.getMessage());
        }
    }

    /** GET /api/mapreduce/status. */
    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        Map<String, Object> c = current;
        m.put("running", running.get());
        if (c != null) {
            Map<String, Object> run = new LinkedHashMap<>(c);
            run.put("elapsedMs", System.currentTimeMillis() - (Long) c.get("startedAtMs"));
            m.put("current", run);
        } else {
            m.put("current", null);
        }
        Map<String, Object> l = last;
        m.put("hasLast", l != null);
        m.put("lastJob", l == null ? null : l.get("job"));
        m.put("lastBuiltAt", l == null ? null : l.get("builtAt"));
        m.put("lastVerified", l == null ? null : l.get("verified"));
        m.put("lastFailure", lastFailure);
        m.put("pythonCommand", config.getPythonCommand());
        m.put("script", repoRoot.resolve(SCRIPT).toString());
        m.put("timeoutSeconds", TIMEOUT_SECONDS);
        m.put("index", indexStore.view());
        return m;
    }

    /** GET /api/mapreduce/last: the last successful run's result, or null. */
    public Map<String, Object> last() {
        return last;
    }

    // -----------------------------------------------------------------------------------------

    private interface LineSink {
        void accept(String line);
    }

    /** Reads a stream line by line on a daemon thread (\r counts as a line end: Spark progress bars). */
    private static Thread drain(InputStream in, LineSink sink, String name) {
        Thread t = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                StringBuilder line = new StringBuilder();
                int ch;
                while ((ch = r.read()) != -1) {
                    if (ch == '\n' || ch == '\r') {
                        if (line.length() > 0) {
                            sink.accept(line.toString());
                            line.setLength(0);
                        }
                    } else {
                        line.append((char) ch);
                    }
                }
                if (line.length() > 0) {
                    sink.accept(line.toString());
                }
            } catch (IOException ignored) {
                // the process was stopped
            }
        }, name);
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void join(Thread... threads) {
        for (Thread t : threads) {
            try {
                t.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Stops the script and what it started (the Spark JVM, Python workers). */
    private static void destroyTree(Process process) {
        process.descendants().forEach(ProcessHandle::destroyForcibly);
        process.destroyForcibly();
    }

    private static String tail(Deque<String> lines) {
        synchronized (lines) {
            if (lines.isEmpty()) {
                return "; the script wrote nothing to stderr";
            }
            return "; last " + lines.size() + " stderr lines:\n" + String.join("\n", lines);
        }
    }

    /** The message on one line ("a | b | c"), at most 600 characters, for the event log. */
    private static String oneLine(String s) {
        if (s == null) {
            return "";
        }
        String one = s.replace("\r", "").replace(":\n", ": ").replace("\n", " | ");
        return one.length() <= 600 ? one : one.substring(0, 300) + " ... " + one.substring(one.length() - 295);
    }
}
