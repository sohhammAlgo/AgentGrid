package agentgrid.control;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Stress check for the restart-port race (FIXES.md): restarts all 5 nodes at once, 10 times in a
 * row, and after each cycle waits for all 5 up with ready blackboard replicas and an agreed
 * leader. Drives the control plane's HTTP API only (127.0.0.1:8080 must be running). Prints
 * time-to-ready per cycle and min/median/max; exits non-zero unless all 10 cycles reach ready
 * and every restart request returned 200.
 *
 * Usage (from backend/): java -cp build/classes agentgrid.control.RestartStress
 */
public class RestartStress {

    private static final String BASE = System.getProperty("agentgrid.verifier.base", "http://127.0.0.1:8080");
    private static final int CYCLES = 10;
    private static final int NODES = 5;
    private static final long READY_TIMEOUT_MS = 60000;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    public static void main(String[] args) throws Exception {
        System.exit(new RestartStress().run() ? 0 : 1);
    }

    private boolean run() throws Exception {
        System.out.println("=====================================================================");
        System.out.println(" RestartStress: restart all " + NODES + " nodes at once, " + CYCLES + " cycles -> " + BASE);
        System.out.println("=====================================================================");
        long initial = waitReady(System.currentTimeMillis());
        System.out.println("initial state: " + (initial < 0 ? "NOT all ready" : "all " + NODES + " up and ready, leader " + leader()));
        if (initial < 0) {
            System.out.println("OVERALL: FAIL (cluster not ready before the first cycle)");
            return false;
        }

        List<Long> times = new ArrayList<>();
        int readyCycles = 0;
        int failedRestarts = 0;
        for (int cycle = 1; cycle <= CYCLES; cycle++) {
            Map<Integer, String> responses = new ConcurrentHashMap<>();
            CountDownLatch done = new CountDownLatch(NODES);
            long t0 = System.currentTimeMillis();
            for (int n = 1; n <= NODES; n++) {
                final int node = n;
                new Thread(() -> {
                    try {
                        HttpResponse<String> r = post("/api/nodes/" + node + "/restart");
                        responses.put(node, r.statusCode() == 200 ? "200" : r.statusCode() + " " + r.body());
                    } catch (Exception e) {
                        responses.put(node, "EXCEPTION " + e);
                    } finally {
                        done.countDown();
                    }
                }).start();
            }
            done.await();
            long requestsDone = System.currentTimeMillis() - t0;
            int bad = 0;
            for (String v : responses.values()) {
                if (!"200".equals(v)) bad++;
            }
            failedRestarts += bad;
            long ready = waitReady(t0);
            if (ready >= 0) {
                readyCycles++;
                times.add(ready);
            }
            System.out.printf("cycle %2d: restart responses %s (all returned after %d ms); %s%n", cycle,
                    new java.util.TreeMap<>(responses), requestsDone,
                    ready < 0 ? "NOT all ready within " + READY_TIMEOUT_MS + " ms, not ready: " + notReady()
                            : "all " + NODES + " up and ready with agreed leader " + leader() + " after " + ready + " ms");
        }

        Collections.sort(times);
        System.out.println();
        System.out.println("time to ready per cycle (ms, sorted): " + times);
        if (!times.isEmpty()) {
            System.out.println("min " + times.get(0) + " ms, median " + times.get(times.size() / 2) + " ms, max "
                    + times.get(times.size() - 1) + " ms");
        }
        boolean ok = readyCycles == CYCLES && failedRestarts == 0;
        System.out.println((ok ? "PASS" : "FAIL") + "  " + readyCycles + "/" + CYCLES + " cycles reached ready; "
                + failedRestarts + " of " + (CYCLES * NODES) + " restart requests failed");
        System.out.println("OVERALL: " + (ok ? "PASS" : "FAIL"));
        return ok;
    }

    /** Milliseconds from t0 until all nodes are up and ready with an agreed leader, or -1. */
    private long waitReady(long t0) throws Exception {
        while (System.currentTimeMillis() - t0 < READY_TIMEOUT_MS) {
            if (notReady().isEmpty() && leader() != null) {
                return System.currentTimeMillis() - t0;
            }
            Thread.sleep(100);
        }
        return -1;
    }

    private List<Integer> notReady() throws Exception {
        List<Integer> out = new ArrayList<>();
        for (int n = 1; n <= NODES; n++) out.add(n);
        Map<String, Object> o = map(get("/api/blackboard?prefix=none-stress"));
        for (Map<String, Object> n : listOf(o.get("nodes"))) {
            if (Boolean.TRUE.equals(n.get("up")) && Boolean.TRUE.equals(n.get("ready"))) {
                out.remove(Integer.valueOf(((Number) n.get("node")).intValue()));
            }
        }
        return out;
    }

    /** The agreed leader id, or null while there is none. */
    private Object leader() throws Exception {
        Map<String, Object> el = map(get("/api/election"));
        return Boolean.TRUE.equals(el.get("agreed")) ? el.get("leaderId") : null;
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
