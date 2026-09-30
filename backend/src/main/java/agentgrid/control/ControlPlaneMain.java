package agentgrid.control;

import agentgrid.clock.TimeService;
import agentgrid.common.Result;
import agentgrid.common.Subtask;
import agentgrid.node.ClockCoordinatorService;
import agentgrid.node.ClusterConfig;
import agentgrid.node.NodeAgent;
import agentgrid.node.NodeProcessManager;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Control plane server for AgentGrid-Lite.
 * Starts cluster node processes, runs continuous background health monitoring,
 * and serves the control HTTP API and static dashboard on port 8080 (127.0.0.1).
 *
 * Usage:
 *   java agentgrid.control.ControlPlaneMain
 */
public class ControlPlaneMain {

    private static final int PORT = 8080;
    private static final String HOST = "127.0.0.1";

    private final ClusterConfig config;
    private final NodeProcessManager processManager;
    private final EventLog eventLog;
    private final ClusterMonitor monitor;
    private final JobDirectory jobDirectory;
    private final BlackboardApi blackboardApi;
    private final HttpServer server;
    private final Path frontendDir;

    public ControlPlaneMain() throws Exception {
        this.config = ClusterConfig.load();
        this.processManager = new NodeProcessManager();
        this.eventLog = new EventLog();
        this.monitor = new ClusterMonitor(config, eventLog);
        this.jobDirectory = new JobDirectory(config, monitor);
        this.blackboardApi = new BlackboardApi(config, monitor);
        this.monitor.setJobDirectory(jobDirectory);

        this.frontendDir = resolveFrontendDirectory();

        this.server = HttpServer.create(new InetSocketAddress(HOST, PORT), 0);
        this.server.setExecutor(Executors.newCachedThreadPool());

        setupRoutes();
    }

    private Path resolveFrontendDirectory() {
        Path p = Path.of("frontend");
        if (Files.isDirectory(p)) {
            return p.toAbsolutePath().normalize();
        }
        Path parentP = Path.of("..", "frontend");
        if (Files.isDirectory(parentP)) {
            return parentP.toAbsolutePath().normalize();
        }
        return p.toAbsolutePath().normalize();
    }

    public void start() throws IOException {
        System.out.println("[ControlPlaneMain] Starting 5 cluster node processes...");
        for (int nodeId : config.getNodeIds()) {
            try {
                Process p = processManager.start(nodeId);
                eventLog.record("NODE_STARTED", nodeId,
                        "Node " + nodeId + " launched (pid=" + p.pid() + ")", System.currentTimeMillis());
                System.out.println("  -> Node " + nodeId + " started (pid=" + p.pid() + ")");
            } catch (Exception e) {
                System.err.println("  -> Failed to start Node " + nodeId + ": " + e.getMessage());
            }
        }

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[ControlPlaneMain] Shutdown initiated; terminating nodes and stopping server...");
            try {
                monitor.stop();
                processManager.stopAll();
                server.stop(0);
                System.out.println("[ControlPlaneMain] All cluster nodes and services stopped.");
            } catch (Exception ignored) {
            }
        }));

        monitor.start();
        server.start();

        System.out.println("\n[ControlPlaneMain] Control Plane running at http://" + HOST + ":" + PORT + "/");
        System.out.println("[ControlPlaneMain] Serving frontend from: " + frontendDir);
        System.out.println("[ControlPlaneMain] Press Ctrl-C to terminate cluster and control plane.");
    }

    private void setupRoutes() {
        server.createContext("/api/modules", this::handleModules);
        server.createContext("/api/cluster", this::handleCluster);
        server.createContext("/api/events", this::handleEvents);
        server.createContext("/api/stream", this::handleStream);
        server.createContext("/api/nodes/", this::handleNodes);
        server.createContext("/api/rmi/invoke", this::handleRmiInvoke);
        server.createContext("/api/clock/drift", this::handleClockDrift);
        server.createContext("/api/clock/sync", this::handleClockSync);
        server.createContext("/api/election", this::handleElection);
        server.createContext("/api/election/algorithm", this::handleElectionAlgorithm);
        server.createContext("/api/election/start", this::handleElectionStart);
        server.createContext("/api/jobs", this::handleJobs);
        server.createContext("/api/clock/auto", this::handleClockAuto);
        server.createContext("/api/blackboard", this::handleBlackboard);
        server.createContext("/api/blackboard/write", this::handleBlackboardWrite);
        server.createContext("/", this::handleStatic);
    }

    // =========================================================================
    // API ROUTE HANDLERS
    // =========================================================================

    private void handleModules(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        List<Map<String, String>> modules = List.of(
                Map.of("id", "exp1", "title", "RMI"),
                Map.of("id", "exp2", "title", "Threads"),
                Map.of("id", "exp3", "title", "Clocks"),
                Map.of("id", "exp4", "title", "Election"),
                Map.of("id", "exp5", "title", "Blackboard"),
                Map.of("id", "exp6", "title", "Load Balancing")
        );
        sendJsonResponse(exchange, 200, modules);
    }

    private void handleCluster(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        sendJsonResponse(exchange, 200, monitor.getSnapshotAsMaps());
    }

    private void handleEvents(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        long since = 0;
        String query = exchange.getRequestURI().getQuery();
        if (query != null && query.contains("since=")) {
            for (String param : query.split("&")) {
                if (param.startsWith("since=")) {
                    try {
                        since = Long.parseLong(param.substring("since=".length()));
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }
        List<EventLog.Event> events = eventLog.getEventsSince(since);
        List<Map<String, Object>> list = new ArrayList<>();
        for (EventLog.Event e : events) {
            list.add(e.toMap());
        }
        sendJsonResponse(exchange, 200, list);
    }

    private void handleStream(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            sendMethodNotAllowed(exchange);
            return;
        }

        exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=UTF-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-cache");
        exchange.getResponseHeaders().set("Connection", "keep-alive");
        exchange.sendResponseHeaders(200, 0);

        OutputStream os = exchange.getResponseBody();

        // Listen for new events and write SSE messages
        java.util.function.Consumer<EventLog.Event> listener = event -> {
            try {
                String data = "event: event\ndata: " + JsonUtil.toJson(event.toMap()) + "\n\n";
                synchronized (os) {
                    os.write(data.getBytes(StandardCharsets.UTF_8));
                    os.flush();
                }
            } catch (IOException ignored) {
            }
        };

        eventLog.addListener(listener);

        try {
            while (true) {
                // Send periodic cluster and election snapshots
                String snapJson = "event: cluster\ndata: " + JsonUtil.toJson(monitor.getSnapshotAsMaps()) + "\n\n"
                        + "event: election\ndata: " + JsonUtil.toJson(monitor.getElectionTracker().toMap()) + "\n\n";
                synchronized (os) {
                    os.write(snapJson.getBytes(StandardCharsets.UTF_8));
                    os.flush();
                }
                Thread.sleep(monitor.isBurstActive() ? 200 : 1000);
            }
        } catch (InterruptedException | IOException e) {
            // Client disconnected
        } finally {
            eventLog.removeListener(listener);
            try {
                os.close();
            } catch (Exception ignored) {
            }
        }
    }

    private boolean validatePostHeaders(HttpExchange exchange) throws IOException {
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType == null || !contentType.toLowerCase().contains("application/json")) {
            sendError(exchange, 415, "Content-Type must be application/json");
            return false;
        }
        String origin = exchange.getRequestHeaders().getFirst("Origin");
        if (origin != null && !origin.equals("http://127.0.0.1:8080") && !origin.equals("http://localhost:8080")) {
            sendError(exchange, 403, "Forbidden Origin");
            return false;
        }
        return true;
    }

    private void handleNodes(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath(); // /api/nodes/{id}/kill or /restart
        String[] parts = path.split("/");
        // Expected: ["", "api", "nodes", "{id}", "{action}"]
        if (parts.length < 5) {
            sendError(exchange, 400, "Invalid node path endpoint");
            return;
        }

        int nodeId;
        try {
            nodeId = Integer.parseInt(parts[3]);
        } catch (NumberFormatException e) {
            sendError(exchange, 400, "Node ID must be an integer");
            return;
        }

        if (!config.getNodeIds().contains(nodeId)) {
            sendError(exchange, 400, "Unknown node ID: " + nodeId);
            return;
        }

        String action = parts[4].toLowerCase();
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        if (!validatePostHeaders(exchange)) return;

        if (action.equals("kill")) {
            // Record the kill and push its Lamport time to every UP node BEFORE the process
            // dies, so every HEARTBEAT_MISS and election event it causes is ordered after it.
            Process target = processManager.getProcess(nodeId);
            eventLog.record("NODE_KILLED", nodeId, "Node " + nodeId + " process terminated via API"
                    + (target != null ? " (pid=" + target.pid() + ")" : ""), System.currentTimeMillis());
            syncAllUpNodesLamport();
            boolean killed = processManager.kill(nodeId);
            monitor.expectElectionActivity(10000);
            monitor.pollNode(nodeId);
            sendJsonResponse(exchange, 200, Map.of("success", true, "node", nodeId, "status", "killed", "previouslyAlive", killed));
        } else if (action.equals("restart")) {
            try {
                Process p = processManager.restart(nodeId);
                eventLog.record("NODE_RESTARTED", nodeId, "Node " + nodeId + " restarted (pid=" + p.pid() + ")", System.currentTimeMillis());
                syncAllUpNodesLamport();
                monitor.expectElectionActivity(10000);
                try {
                    Thread.sleep(600);
                } catch (InterruptedException ignored) {}
                monitor.pollNode(nodeId);
                sendJsonResponse(exchange, 200, Map.of("success", true, "node", nodeId, "status", "restarted", "pid", p.pid()));
            } catch (Exception e) {
                // The old process is stopped and no new one was started (e.g. its port was
                // still held): record it and show the node as down.
                eventLog.record("NODE_RESTART_FAILED", nodeId, "Node " + nodeId + " was not restarted: " + e.getMessage(),
                        System.currentTimeMillis());
                monitor.pollNode(nodeId);
                sendError(exchange, 500, "Failed to restart node " + nodeId + ": " + e.getMessage());
            }
        } else {
            sendError(exchange, 400, "Unknown node action: " + action + ". Supported actions: kill, restart");
        }
    }

    private void handleRmiInvoke(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        if (!validatePostHeaders(exchange)) return;

        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, Object> req;
        try {
            req = JsonUtil.parseObject(body);
        } catch (Exception e) {
            sendError(exchange, 400, "Malformed JSON request body: " + e.getMessage());
            return;
        }

        if (req == null || !req.containsKey("node")) {
            sendError(exchange, 400, "Missing required field: 'node'");
            return;
        }

        int nodeId;
        try {
            nodeId = ((Number) req.get("node")).intValue();
        } catch (Exception e) {
            sendError(exchange, 400, "Field 'node' must be an integer");
            return;
        }

        if (!config.getNodeIds().contains(nodeId)) {
            sendError(exchange, 400, "Node ID " + nodeId + " not found in cluster configuration");
            return;
        }

        String typeStr = req.containsKey("type") ? String.valueOf(req.get("type")) : "SUMMARIZE";
        Subtask.Type subtaskType;
        try {
            subtaskType = Subtask.Type.valueOf(typeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            sendError(exchange, 400, "Invalid subtask type: " + typeStr + ". Valid types: RETRIEVE, RANK, SUMMARIZE, SYNTHESIZE");
            return;
        }

        int count = 1;
        if (req.containsKey("count")) {
            try {
                count = ((Number) req.get("count")).intValue();
            } catch (Exception e) {
                sendError(exchange, 400, "Field 'count' must be an integer >= 1");
                return;
            }
        }
        if (count < 1 || count > 100) {
            sendError(exchange, 400, "Field 'count' must be between 1 and 100");
            return;
        }

        ClusterConfig.NodeConfig nc = config.getNode(nodeId);
        NodeAgent agent;
        TimeService timeService;
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", nc.getPort());
            agent = (NodeAgent) registry.lookup("agent");
            timeService = (TimeService) registry.lookup("time");
        } catch (Exception e) {
            sendError(exchange, 503, "Node " + nodeId + " is currently unreachable: " + e.getMessage());
            return;
        }

        // Execute count invocations concurrently
        ExecutorService burstPool = Executors.newFixedThreadPool(Math.min(count, 32));
        CountDownLatch latch = new CountDownLatch(count);
        List<Map<String, Object>> callResults = new CopyOnWriteArrayList<>();

        long overallStart = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            final int index = i;
            burstPool.submit(() -> {
                try {
                    long sentTs = eventLog.getLamportClock().tick();
                    Subtask subtask = new Subtask(
                            "job-req",
                            "subtask-" + index,
                            subtaskType,
                            "query-chunk-" + index,
                            sentTs
                    );

                    long t0 = System.currentTimeMillis();
                    Result res = agent.execute(subtask);
                    long t1 = System.currentTimeMillis();
                    long latency = t1 - t0;

                    long receivedTs = res.getLamportTimestamp();
                    long calleeWallMs;
                    try {
                        calleeWallMs = timeService.getTime();
                    } catch (Exception ignored) {
                        calleeWallMs = System.currentTimeMillis() + nc.getClockDriftMs();
                    }

                    eventLog.recordWithReceivedLamport(
                            "RMI_CALL",
                            nodeId,
                            "execute(" + subtaskType + ") subtask=" + subtask.getSubtaskId()
                                    + " -> " + res.getOutput() + " [latency=" + latency + "ms]",
                            receivedTs,
                            calleeWallMs
                    );

                    Map<String, Object> callMap = new LinkedHashMap<>();
                    callMap.put("index", index);
                    callMap.put("subtaskId", res.getSubtaskId());
                    callMap.put("agentId", res.getAgentId());
                    callMap.put("payload", res.getOutput());
                    callMap.put("lamportSent", sentTs);
                    callMap.put("lamportReceived", receivedTs);
                    callMap.put("latencyMs", latency);
                    callMap.put("nodeWallMs", calleeWallMs);

                    callResults.add(callMap);

                } catch (Exception e) {
                    Map<String, Object> errMap = new LinkedHashMap<>();
                    errMap.put("index", index);
                    errMap.put("error", e.getMessage());
                    callResults.add(errMap);
                } finally {
                    latch.countDown();
                }
            });
        }

        monitor.setBurstActive(true);
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            monitor.setBurstActive(false);
            burstPool.shutdown();
        }

        long makespan = System.currentTimeMillis() - overallStart;

        // Sort results by index
        List<Map<String, Object>> sortedResults = new ArrayList<>(callResults);
        sortedResults.sort((a, b) -> Integer.compare(
                ((Number) a.get("index")).intValue(),
                ((Number) b.get("index")).intValue()
        ));

        long simulatedWorkMs = 150;
        try {
            simulatedWorkMs = agent.getSimulatedWorkMs();
        } catch (Exception ignored) {}

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("node", nodeId);
        resp.put("type", subtaskType.name());
        resp.put("count", count);
        resp.put("makespanMs", makespan);
        resp.put("simulatedWorkMs", simulatedWorkMs);
        resp.put("calls", sortedResults);

        sendJsonResponse(exchange, 200, resp);
    }

    private void handleClockDrift(HttpExchange exchange) throws IOException {
        if (exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            sendClockOffsets(exchange);
            return;
        }
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        if (!validatePostHeaders(exchange)) return;

        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, Object> req;
        try {
            req = JsonUtil.parseObject(body);
        } catch (Exception e) {
            sendError(exchange, 400, "Malformed JSON request body: " + e.getMessage());
            return;
        }

        if (req == null || !req.containsKey("node") || !req.containsKey("deltaMs")) {
            sendError(exchange, 400, "Missing required fields: 'node' and 'deltaMs'");
            return;
        }

        int nodeId;
        long deltaMs;
        try {
            nodeId = ((Number) req.get("node")).intValue();
            deltaMs = ((Number) req.get("deltaMs")).longValue();
        } catch (Exception e) {
            sendError(exchange, 400, "Fields 'node' and 'deltaMs' must be numbers");
            return;
        }

        if (!config.getNodeIds().contains(nodeId)) {
            sendError(exchange, 400, "Node ID " + nodeId + " not configured");
            return;
        }

        ClusterConfig.NodeConfig nc = config.getNode(nodeId);
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", nc.getPort());
            TimeService timeService = (TimeService) registry.lookup("time");
            timeService.adjustTime(deltaMs);
            long nodeWallTime = timeService.getTime();

            eventLog.record("CLOCK_DRIFT_SET", nodeId,
                    "Adjusted clock offset by " + (deltaMs >= 0 ? "+" : "") + deltaMs + " ms",
                    nodeWallTime);

            monitor.pollNode(nodeId);

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("success", true);
            resp.put("node", nodeId);
            resp.put("deltaMs", deltaMs);
            resp.put("reportedTime", nodeWallTime);

            sendJsonResponse(exchange, 200, resp);

        } catch (Exception e) {
            sendError(exchange, 503, "Failed to adjust drift on Node " + nodeId + ": " + e.getMessage());
        }
    }

    private void syncAllUpNodesLamport() {
        monitor.syncAllUp();
    }

    // =========================================================================
    // EXPERIMENT 4: LEADER ELECTION
    // =========================================================================

    private void handleElection(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestURI().getPath().equals("/api/election")) {
            sendError(exchange, 404, "Unknown election endpoint: " + exchange.getRequestURI().getPath());
            return;
        }
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        sendJsonResponse(exchange, 200, monitor.getElectionTracker().toMap());
    }

    private void handleElectionAlgorithm(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        if (!validatePostHeaders(exchange)) return;

        Map<String, Object> req = readJsonBody(exchange);
        if (req == null) return;
        String name = req.get("name") == null ? "" : String.valueOf(req.get("name")).trim().toUpperCase();
        if (!name.equals("BULLY") && !name.equals("RING")) {
            sendError(exchange, 400, "Field 'name' must be \"BULLY\" or \"RING\"");
            return;
        }

        monitor.setDesiredAlgorithm(name);
        processManager.setElectionAlgorithm(name);

        List<Integer> applied = new ArrayList<>();
        List<Integer> failed = new ArrayList<>();
        for (ClusterMonitor.NodeStatus status : monitor.getSnapshot()) {
            if (!status.isUp()) {
                continue;
            }
            try {
                agentgrid.node.NodeElection election = monitor.electionOf(status.getId());
                monitor.call(() -> {
                    election.setAlgorithm(name);
                    return null;
                });
                applied.add(status.getId());
            } catch (Exception e) {
                failed.add(status.getId());
            }
        }
        eventLog.record("ELECTION_ALGORITHM_SET", 0,
                "Election algorithm set to " + name + " on nodes " + applied
                        + (failed.isEmpty() ? "" : " (failed: " + failed + ")"),
                System.currentTimeMillis());
        monitor.pollAll();

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("algorithm", name);
        resp.put("applied", applied);
        resp.put("failed", failed);
        sendJsonResponse(exchange, 200, resp);
    }

    private void handleElectionStart(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        if (!validatePostHeaders(exchange)) return;

        Map<String, Object> req = readJsonBody(exchange);
        if (req == null) return;
        int nodeId;
        try {
            nodeId = ((Number) req.get("node")).intValue();
        } catch (Exception e) {
            sendError(exchange, 400, "Field 'node' must be an integer");
            return;
        }
        if (!config.getNodeIds().contains(nodeId)) {
            sendError(exchange, 400, "Unknown node ID: " + nodeId);
            return;
        }

        try {
            agentgrid.node.NodeElection election = monitor.electionOf(nodeId);
            monitor.call(() -> {
                election.startElection();
                return null;
            });
        } catch (Exception e) {
            sendError(exchange, 503, "Node " + nodeId + " is unreachable: " + e.getMessage());
            return;
        }
        monitor.expectElectionActivity(10000);

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("success", true);
        resp.put("node", nodeId);
        resp.put("requestedTrueMs", System.currentTimeMillis());
        sendJsonResponse(exchange, 200, resp);
    }

    // =========================================================================
    // JOBS (orchestrator on the leader)
    // =========================================================================

    private static final int MAX_QUERY_CHARS = 500;

    private void handleJobs(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String rest = path.length() > "/api/jobs".length() ? path.substring("/api/jobs".length()) : "";
        if (rest.equals("/")) {
            rest = "";
        }
        String method = exchange.getRequestMethod();

        if (rest.isEmpty()) {
            if (method.equalsIgnoreCase("GET")) {
                List<Map<String, Object>> list = new ArrayList<>();
                for (agentgrid.orchestrator.Job job : jobDirectory.list(50)) {
                    list.add(job.toSummaryMap());
                }
                sendJsonResponse(exchange, 200, list);
            } else if (method.equalsIgnoreCase("POST")) {
                submitJob(exchange);
            } else {
                sendMethodNotAllowed(exchange);
            }
            return;
        }

        if (!method.equalsIgnoreCase("GET")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        String jobId = rest.substring(1);
        if (jobId.isEmpty() || jobId.contains("/")) {
            sendError(exchange, 404, "Unknown jobs endpoint: " + path);
            return;
        }
        agentgrid.orchestrator.Job job = jobDirectory.get(jobId);
        if (job == null) {
            sendError(exchange, 404, "Unknown job: " + jobId);
            return;
        }
        sendJsonResponse(exchange, 200, job.toMap());
    }

    private void submitJob(HttpExchange exchange) throws IOException {
        if (!validatePostHeaders(exchange)) return;
        Map<String, Object> req = readJsonBody(exchange);
        if (req == null) return;

        Object q = req.get("query");
        if (!(q instanceof String) || ((String) q).isBlank()) {
            sendError(exchange, 400, "Field 'query' must be a non-empty string");
            return;
        }
        String query = ((String) q).trim();
        if (query.length() > MAX_QUERY_CHARS) {
            sendError(exchange, 400, "Field 'query' must be at most " + MAX_QUERY_CHARS + " characters");
            return;
        }
        Object p = req.get("policy");
        agentgrid.orchestrator.BalancingPolicy policy =
                p instanceof String ? agentgrid.orchestrator.BalancingPolicy.parse((String) p) : null;
        if (policy == null) {
            sendError(exchange, 400, "Field 'policy' must be \"ROUND_ROBIN\", \"LEAST_LOADED\" or \"WEIGHTED\"");
            return;
        }
        Object c = req.get("consistency");
        String consistency = c == null ? "EVENTUAL" : String.valueOf(c);
        if (!consistency.equals("STRONG") && !consistency.equals("EVENTUAL")) {
            sendError(exchange, 400, "Field 'consistency' must be \"STRONG\" or \"EVENTUAL\" (default EVENTUAL)");
            return;
        }
        try {
            sendJsonResponse(exchange, 200, jobDirectory.submit(query, policy.name(), consistency));
        } catch (JobDirectory.ApiException e) {
            sendError(exchange, e.getStatus(), e.getMessage());
        }
    }

    /** Parses the JSON request body; on failure sends 400 and returns null. */
    private Map<String, Object> readJsonBody(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        try {
            Map<String, Object> req = JsonUtil.parseObject(body);
            if (req == null) {
                throw new IllegalArgumentException("empty body");
            }
            return req;
        } catch (Exception e) {
            sendError(exchange, 400, "Malformed JSON request body: " + e.getMessage());
            return null;
        }
    }

    private void handleClockSync(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        if (!validatePostHeaders(exchange)) return;

        // The leader coordinates Berkeley rounds (it is included in the average); the control
        // plane only asks it to run one. The leader records the CLOCK_SYNC event.
        Object leader = monitor.getElectionTracker().toMap().get("leaderId");
        if (!(leader instanceof Integer)) {
            sendError(exchange, 409, "No agreed leader: the leader coordinates clock sync; retry after the election");
            return;
        }
        int leaderId = (Integer) leader;
        try {
            ClockCoordinatorService clock = monitor.lookup(leaderId, "clock", ClockCoordinatorService.class);
            Map<String, Object> result = monitor.call(() -> clock.runSyncRound("manual"), 8000);
            monitor.pollAll();
            sendJsonResponse(exchange, 200, result);
        } catch (Exception e) {
            sendError(exchange, 503, "Leader node " + leaderId + " could not run a Berkeley round: " + e.getMessage());
        }
    }

    /** GET: the auto clock-sync setting. POST {"enabled": bool}: set it on every UP node and for restarts. */
    private void handleClockAuto(HttpExchange exchange) throws IOException {
        if (exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("enabled", monitor.getDesiredAutoSync());
            resp.put("berkeleyIntervalMs", config.getBerkeleyIntervalMs());
            sendJsonResponse(exchange, 200, resp);
            return;
        }
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        if (!validatePostHeaders(exchange)) return;
        Map<String, Object> req = readJsonBody(exchange);
        if (req == null) return;
        if (!(req.get("enabled") instanceof Boolean)) {
            sendError(exchange, 400, "Field 'enabled' must be true or false");
            return;
        }
        boolean enabled = (Boolean) req.get("enabled");
        monitor.setDesiredAutoSync(enabled);
        processManager.setClockAutoSync(enabled);
        List<Integer> applied = new ArrayList<>();
        List<Integer> failed = new ArrayList<>();
        for (ClusterMonitor.NodeStatus status : monitor.getSnapshot()) {
            if (!status.isUp()) {
                continue;
            }
            try {
                ClockCoordinatorService clock = monitor.lookup(status.getId(), "clock", ClockCoordinatorService.class);
                monitor.call(() -> {
                    clock.setAutoSync(enabled);
                    return null;
                });
                applied.add(status.getId());
            } catch (Exception e) {
                failed.add(status.getId());
            }
        }
        eventLog.record("CLOCK_AUTO_SET", 0, "Auto clock sync (periodic and rejoin) " + (enabled ? "enabled" : "disabled")
                + " on nodes " + applied + (failed.isEmpty() ? "" : " (failed: " + failed + ")"), System.currentTimeMillis());
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("enabled", enabled);
        resp.put("berkeleyIntervalMs", config.getBerkeleyIntervalMs());
        resp.put("applied", applied);
        resp.put("failed", failed);
        sendJsonResponse(exchange, 200, resp);
    }

    /** Current clock offset of every node against the control plane's clock (RTT-compensated). */
    private void sendClockOffsets(HttpExchange exchange) throws IOException {
        List<Map<String, Object>> list = new ArrayList<>();
        for (ClusterMonitor.NodeStatus status : monitor.getSnapshot()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("node", status.getId());
            m.put("up", status.isUp());
            m.put("configuredDriftMs", config.getNode(status.getId()).getClockDriftMs());
            Long offset = null;
            if (status.isUp()) {
                try {
                    TimeService ts = monitor.lookup(status.getId(), "time", TimeService.class);
                    long t0 = System.currentTimeMillis();
                    long nodeTime = monitor.call(ts::getTime);
                    long t1 = System.currentTimeMillis();
                    offset = nodeTime - (t0 + (t1 - t0) / 2);
                } catch (Exception ignored) {
                    // reported as null
                }
            }
            m.put("offsetMs", offset);
            list.add(m);
        }
        sendJsonResponse(exchange, 200, list);
    }

    // =========================================================================
    // BLACKBOARD (Exp 5)
    // =========================================================================

    private void handleBlackboard(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        if (path.equals("/api/blackboard") || path.equals("/api/blackboard/")) {
            String prefix = queryParam(exchange, "prefix");
            sendJsonResponse(exchange, 200, blackboardApi.overview(prefix == null ? "" : prefix));
        } else if (path.equals("/api/blackboard/metrics")) {
            sendJsonResponse(exchange, 200, blackboardApi.metrics());
        } else if (path.equals("/api/blackboard/read")) {
            String node = queryParam(exchange, "node");
            String key = queryParam(exchange, "key");
            if (node == null || key == null || key.isEmpty()) {
                sendError(exchange, 400, "Query parameters 'node' and 'key' are required");
                return;
            }
            try {
                sendJsonResponse(exchange, 200, blackboardApi.read(Integer.parseInt(node.trim()), key));
            } catch (NumberFormatException e) {
                sendError(exchange, 400, "Parameter 'node' must be an integer");
            } catch (JobDirectory.ApiException e) {
                sendError(exchange, e.getStatus(), e.getMessage());
            }
        } else {
            sendError(exchange, 404, "Unknown blackboard endpoint: " + path);
        }
    }

    private void handleBlackboardWrite(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            sendMethodNotAllowed(exchange);
            return;
        }
        if (!validatePostHeaders(exchange)) return;
        Map<String, Object> req = readJsonBody(exchange);
        if (req == null) return;
        int node;
        try {
            node = ((Number) req.get("node")).intValue();
        } catch (Exception e) {
            sendError(exchange, 400, "Field 'node' must be an integer");
            return;
        }
        Object key = req.get("key");
        Object value = req.get("value");
        Object mode = req.get("mode");
        if (!(key instanceof String) || ((String) key).isEmpty() || ((String) key).length() > agentgrid.node.ClusterBlackboard.MAX_KEY_CHARS) {
            sendError(exchange, 400, "Field 'key' must be a string of 1-" + agentgrid.node.ClusterBlackboard.MAX_KEY_CHARS + " characters");
            return;
        }
        if (!(value instanceof String) || ((String) value).length() > agentgrid.node.ClusterBlackboard.MAX_VALUE_CHARS) {
            sendError(exchange, 400, "Field 'value' must be a string of at most " + agentgrid.node.ClusterBlackboard.MAX_VALUE_CHARS + " characters");
            return;
        }
        if (!"STRONG".equals(mode) && !"EVENTUAL".equals(mode)) {
            sendError(exchange, 400, "Field 'mode' must be \"STRONG\" or \"EVENTUAL\"");
            return;
        }
        try {
            sendJsonResponse(exchange, 200, blackboardApi.write(node, (String) key, (String) value, (String) mode));
        } catch (JobDirectory.ApiException e) {
            sendError(exchange, e.getStatus(), e.getMessage());
        }
    }

    /** A URL-decoded query parameter, or null. */
    private static String queryParam(HttpExchange exchange, String name) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            return null;
        }
        for (String part : query.split("&")) {
            int eq = part.indexOf('=');
            String k = eq < 0 ? part : part.substring(0, eq);
            if (k.equals(name)) {
                return java.net.URLDecoder.decode(eq < 0 ? "" : part.substring(eq + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    // =========================================================================
    // STATIC FILE SERVING
    // =========================================================================

    private void handleStatic(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path.equals("/")) {
            path = "/index.html";
        }

        // Clean relative path without leading slashes
        String subpath = path.startsWith("/") ? path.substring(1) : path;
        Path requested = frontendDir.resolve(subpath).normalize();

        // Security check: reject directory traversal
        if (!requested.startsWith(frontendDir)) {
            sendError(exchange, 403, "Access denied: Path traversal detected");
            return;
        }

        if (!Files.exists(requested) || Files.isDirectory(requested)) {
            sendError(exchange, 404, "File not found: " + path);
            return;
        }

        String mimeType = getContentType(requested);
        byte[] bytes = Files.readAllBytes(requested);

        exchange.getResponseHeaders().set("Content-Type", mimeType);
        exchange.getResponseHeaders().set("Cache-Control", "no-cache");
        exchange.sendResponseHeaders(200, bytes.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private String getContentType(Path file) {
        String name = file.getFileName().toString().toLowerCase();
        if (name.endsWith(".html")) return "text/html; charset=UTF-8";
        if (name.endsWith(".js")) return "application/javascript; charset=UTF-8";
        if (name.endsWith(".css")) return "text/css; charset=UTF-8";
        if (name.endsWith(".json")) return "application/json; charset=UTF-8";
        if (name.endsWith(".svg")) return "image/svg+xml";
        if (name.endsWith(".png")) return "image/png";
        if (name.endsWith(".ico")) return "image/x-icon";
        return "text/plain; charset=UTF-8";
    }

    // =========================================================================
    // HTTP HELPERS
    // =========================================================================

    private void sendJsonResponse(HttpExchange exchange, int status, Object data) throws IOException {
        String json = JsonUtil.toJson(data);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(status, bytes.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void sendError(HttpExchange exchange, int status, String message) throws IOException {
        Map<String, Object> err = Map.of("error", message, "status", status);
        sendJsonResponse(exchange, status, err);
    }

    private void sendMethodNotAllowed(HttpExchange exchange) throws IOException {
        sendError(exchange, 405, "Method not allowed: " + exchange.getRequestMethod());
    }

    public static void main(String[] args) {
        try {
            ControlPlaneMain main = new ControlPlaneMain();
            main.start();
        } catch (Exception e) {
            System.err.println("[ControlPlaneMain] Fatal initialization error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
