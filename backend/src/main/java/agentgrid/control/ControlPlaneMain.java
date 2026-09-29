package agentgrid.control;

import agentgrid.clock.TimeService;
import agentgrid.common.Result;
import agentgrid.common.Subtask;
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
    private final HttpServer server;
    private final Path frontendDir;

    public ControlPlaneMain() throws Exception {
        this.config = ClusterConfig.load();
        this.processManager = new NodeProcessManager();
        this.eventLog = new EventLog();
        this.monitor = new ClusterMonitor(config, eventLog);

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
                Map.of("id", "exp3", "title", "Clocks")
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
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
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
                // Send periodic 1s cluster snapshots
                String snapJson = "event: cluster\ndata: " + JsonUtil.toJson(monitor.getSnapshotAsMaps()) + "\n\n";
                synchronized (os) {
                    os.write(snapJson.getBytes(StandardCharsets.UTF_8));
                    os.flush();
                }
                Thread.sleep(1000);
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

        if (action.equals("kill")) {
            boolean killed = processManager.kill(nodeId);
            eventLog.record("NODE_KILLED", nodeId, "Node " + nodeId + " process terminated via API", System.currentTimeMillis());
            monitor.pollNode(nodeId);
            sendJsonResponse(exchange, 200, Map.of("success", true, "node", nodeId, "status", "killed", "previouslyAlive", killed));
        } else if (action.equals("restart")) {
            try {
                Process p = processManager.restart(nodeId);
                eventLog.record("NODE_RESTARTED", nodeId, "Node " + nodeId + " restarted (pid=" + p.pid() + ")", System.currentTimeMillis());
                try {
                    Thread.sleep(600);
                } catch (InterruptedException ignored) {}
                monitor.pollNode(nodeId);
                sendJsonResponse(exchange, 200, Map.of("success", true, "node", nodeId, "status", "restarted", "pid", p.pid()));
            } catch (Exception e) {
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

        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            burstPool.shutdown();
        }

        long makespan = System.currentTimeMillis() - overallStart;

        // Sort results by index
        List<Map<String, Object>> sortedResults = new ArrayList<>(callResults);
        sortedResults.sort((a, b) -> Integer.compare(
                ((Number) a.get("index")).intValue(),
                ((Number) b.get("index")).intValue()
        ));

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("node", nodeId);
        resp.put("type", subtaskType.name());
        resp.put("count", count);
        resp.put("makespanMs", makespan);
        resp.put("calls", sortedResults);

        sendJsonResponse(exchange, 200, resp);
    }

    private void handleClockDrift(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            sendMethodNotAllowed(exchange);
            return;
        }

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

    private void handleClockSync(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            sendMethodNotAllowed(exchange);
            return;
        }

        List<Integer> upNodes = new ArrayList<>();
        for (ClusterMonitor.NodeStatus status : monitor.getSnapshot()) {
            if (status.isUp()) {
                upNodes.add(status.getId());
            }
        }

        if (upNodes.isEmpty()) {
            sendError(exchange, 503, "No online nodes available for Berkeley synchronization");
            return;
        }

        try {
            BerkeleyRound.SyncResult result = BerkeleyRound.execute(config, upNodes);

            eventLog.record("CLOCK_SYNC", 0,
                    "Berkeley synchronization across " + result.getNodeCount() + " nodes: spread "
                            + result.getSpreadBefore() + " ms -> " + result.getSpreadAfter() + " ms",
                    System.currentTimeMillis());

            monitor.pollAll();

            sendJsonResponse(exchange, 200, result.toMap());

        } catch (Exception e) {
            sendError(exchange, 500, "Berkeley synchronization round failed: " + e.getMessage());
        }
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
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
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
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
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
