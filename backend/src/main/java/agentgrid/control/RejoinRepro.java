package agentgrid.control;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reproduces the rejoin clock sync of a restarted node (FIXES.md). Drives the HTTP API only
 * (127.0.0.1:8080 must be running, auto-sync on). Restarts node 2 repeatedly in three modes:
 * A: 5 s after the previous check; B: back to back, as soon as the previous restart is ready
 * (within 2 s of that rejoin round); C: right after a manual Berkeley round. For each restart it
 * reports the restart-to-ready time, clockSynced at the first ready snapshot, and whether node
 * 2's offset came within 100 ms of every other node within 5 s of ready. Exits non-zero unless
 * every restart synced within 5 s.
 *
 * Usage (from backend/): java -cp build/classes agentgrid.control.RejoinRepro [cycles]
 */
public class RejoinRepro {

    private static final String BASE = System.getProperty("agentgrid.verifier.base", "http://127.0.0.1:8080");
    private static final int NODE = 2;
    private static final long READY_TIMEOUT_MS = 20000;
    private static final long SYNC_WINDOW_MS = 5000;
    private static final long TOLERANCE_MS = 100;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    public static void main(String[] args) throws Exception {
        int cycles = args.length > 0 ? Integer.parseInt(args[0]) : 18;
        System.exit(new RejoinRepro().run(cycles) ? 0 : 1);
    }

    private boolean run(int cycles) throws Exception {
        System.out.println("=====================================================================");
        System.out.println(" RejoinRepro: restart node " + NODE + " " + cycles + " times -> " + BASE);
        System.out.println("=====================================================================");
        Map<String, Object> auto = map(get("/api/clock/auto"));
        System.out.println("auto-sync: " + auto);
        if (!Boolean.TRUE.equals(auto.get("enabled")) || !waitAllReady()) {
            System.out.println("OVERALL: FAIL (auto-sync off or cluster not 5 ready)");
            return false;
        }
        int unsyncedAtReady = 0;
        int notWithin = 0;
        Map<String, int[]> byMode = new TreeMap<>();
        for (int i = 1; i <= cycles; i++) {
            String mode = String.valueOf("ABC".charAt((i - 1) % 3));
            if (mode.equals("A")) {
                Thread.sleep(5000);
            }
            String pre = "";
            if (mode.equals("C")) {
                HttpResponse<String> s = post("/api/clock/sync");
                pre = "manual round " + s.statusCode() + " finished " + LocalTime.now() + "; ";
            }
            String startedAt = LocalTime.now().toString();
            long t0 = System.currentTimeMillis();
            HttpResponse<String> r = post("/api/nodes/" + NODE + "/restart");
            long readyMs = -1;
            Object synced = null;
            while (System.currentTimeMillis() - t0 < READY_TIMEOUT_MS) {
                Map<String, Object> n = node();
                if (n != null && Boolean.TRUE.equals(n.get("up")) && Boolean.TRUE.equals(n.get("ready"))) {
                    readyMs = System.currentTimeMillis() - t0;
                    synced = n.get("clockSynced");
                    break;
                }
                Thread.sleep(50);
            }
            long readyAt = System.currentTimeMillis();
            long withinAt = -1;
            Map<Integer, Object> last = new TreeMap<>();
            while (readyMs >= 0 && System.currentTimeMillis() - readyAt <= SYNC_WINDOW_MS) {
                last = offsets();
                if (within(last)) {
                    withinAt = System.currentTimeMillis() - readyAt;
                    break;
                }
                Thread.sleep(200);
            }
            if (!Boolean.TRUE.equals(synced)) unsyncedAtReady++;
            if (withinAt < 0) notWithin++;
            int[] m = byMode.computeIfAbsent(mode, k -> new int[3]);
            m[0]++;
            if (!Boolean.TRUE.equals(synced)) m[1]++;
            if (withinAt < 0) m[2]++;
            System.out.printf("restart %2d mode %s: %srestart requested %s (HTTP %d); ready after %s ms, clockSynced at ready=%s; "
                            + "offset within %d ms of all others %s; offsets %s%n",
                    i, mode, pre, startedAt, r.statusCode(), readyMs < 0 ? "NEVER" : String.valueOf(readyMs), synced,
                    TOLERANCE_MS, withinAt < 0 ? "NOT within " + SYNC_WINDOW_MS + " ms of ready" : withinAt + " ms after ready", last);
        }
        System.out.println();
        System.out.println("by mode {restarts, clockSynced=false at ready, not within " + TOLERANCE_MS + " ms after "
                + SYNC_WINDOW_MS + " ms}:");
        for (Map.Entry<String, int[]> e : byMode.entrySet()) {
            System.out.println("  " + e.getKey() + ": " + e.getValue()[0] + " restarts, " + e.getValue()[1] + " unsynced at ready, "
                    + e.getValue()[2] + " not within tolerance");
        }
        boolean ok = notWithin == 0;
        System.out.println((ok ? "PASS" : "FAIL") + "  " + (cycles - notWithin) + "/" + cycles + " restarts synced within "
                + SYNC_WINDOW_MS + " ms of ready; " + unsyncedAtReady + " reported clockSynced=false at ready");
        System.out.println("OVERALL: " + (ok ? "PASS" : "FAIL"));
        return ok;
    }

    /** Node 2's offset is within TOLERANCE_MS of every other node's (all 5 measured). */
    private static boolean within(Map<Integer, Object> offsets) {
        if (offsets.size() < 5 || offsets.containsValue(null)) return false;
        long mine = ((Number) offsets.get(NODE)).longValue();
        for (Map.Entry<Integer, Object> e : offsets.entrySet()) {
            if (Math.abs(((Number) e.getValue()).longValue() - mine) > TOLERANCE_MS) return false;
        }
        return true;
    }

    private Map<Integer, Object> offsets() throws Exception {
        Map<Integer, Object> out = new TreeMap<>();
        for (Map<String, Object> m : listOf(get("/api/clock/drift"))) {
            out.put(((Number) m.get("node")).intValue(), m.get("offsetMs"));
        }
        return out;
    }

    private Map<String, Object> node() throws Exception {
        for (Map<String, Object> n : listOf(map(get("/api/blackboard?prefix=none-rejoin")).get("nodes"))) {
            if (((Number) n.get("node")).intValue() == NODE) return n;
        }
        return null;
    }

    private boolean waitAllReady() throws Exception {
        long deadline = System.currentTimeMillis() + 60000;
        while (System.currentTimeMillis() < deadline) {
            int ready = 0;
            for (Map<String, Object> n : listOf(map(get("/api/blackboard?prefix=none-rejoin")).get("nodes"))) {
                if (Boolean.TRUE.equals(n.get("up")) && Boolean.TRUE.equals(n.get("ready"))) ready++;
            }
            if (ready == 5 && Boolean.TRUE.equals(map(get("/api/election")).get("agreed"))) return true;
            Thread.sleep(200);
        }
        return false;
    }

    private Object get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path)).timeout(Duration.ofSeconds(15)).GET().build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) {
            throw new IllegalStateException("GET " + path + " -> " + res.statusCode() + " " + res.body());
        }
        return JsonUtil.parse(res.body());
    }

    private HttpResponse<String> post(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object o) {
        return (Map<String, Object>) o;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> listOf(Object o) {
        return o == null ? Collections.emptyList() : (List<Map<String, Object>>) o;
    }
}
