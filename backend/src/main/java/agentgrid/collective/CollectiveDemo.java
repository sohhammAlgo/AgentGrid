package agentgrid.collective;

import agentgrid.control.JsonUtil;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Exp 9 terminal demo (for screenshots): runs the collectives on the live cluster through the control
 * plane's HTTP API and prints one labelled section per step, ending with VERIFIED lines. Exits
 * non-zero if a check fails. Output is ASCII so a Windows console shows it as is.
 *
 * Usage (from backend/, with the control plane running): java -cp build/classes agentgrid.collective.CollectiveDemo
 */
public final class CollectiveDemo {

    private static final String BASE = System.getProperty("agentgrid.verifier.base", "http://127.0.0.1:8080");
    private static final String MESSAGE = "Hello from the root: one message, every rank gets the same bytes";

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final List<String> verdicts = new ArrayList<>();
    private boolean allOk = true;

    public static void main(String[] args) throws Exception {
        CollectiveDemo demo = new CollectiveDemo();
        demo.run();
        System.exit(demo.allOk ? 0 : 1);
    }

    private void run() throws Exception {
        long start = System.currentTimeMillis();
        System.out.println("=====================================================================");
        System.out.println(" AgentGrid-Lite Exp 9: MPI-style collectives on the live cluster");
        System.out.println(" control plane " + BASE);
        System.out.println("=====================================================================");

        section("1. Communicator: the current members sorted by node id (rank = index)");
        Map<String, Object> comm = json(send("GET", "/api/collective", null));
        int n = num(comm.get("size"));
        System.out.printf("epoch %s, size %d, default root = the leader's rank %s (node %s), timeout %s ms%n",
                comm.get("epoch"), n, comm.get("root"), comm.get("rootNode"), comm.get("timeoutMs"));
        System.out.printf("  %-5s %-5s %-6s %-5s %s%n", "rank", "node", "port", "up", "role");
        for (Map<String, Object> r : maps(comm.get("ranks"))) {
            System.out.printf("  %-5s %-5s %-6s %-5s %s%n", r.get("rank"), r.get("node"), r.get("port"),
                    Boolean.TRUE.equals(r.get("up")) ? "yes" : "NO", r.get("role"));
        }

        section("2. Broadcast (MPI_Bcast): the root sends the same bytes to every rank");
        post("/api/collective/broadcast", Map.of("message", "warm-up", "algorithm", "TREE"));
        System.out.println("(one warm-up broadcast first, not shown: the first message through each JVM pays for class loading)");
        String sha = CollectiveNode.sha256(MESSAGE.getBytes(StandardCharsets.UTF_8));
        System.out.println("message: \"" + MESSAGE + "\" (" + MESSAGE.getBytes(StandardCharsets.UTF_8).length
                + " bytes, sha256 " + sha.substring(0, 16) + "...)");
        Map<String, Map<String, Object>> byAlgorithm = new LinkedHashMap<>();
        boolean identical = true;
        for (String algorithm : List.of("LINEAR", "TREE")) {
            Map<String, Object> b = post("/api/collective/broadcast", Map.of("message", MESSAGE, "algorithm", algorithm));
            byAlgorithm.put(algorithm, b);
            System.out.println();
            System.out.println("-- " + algorithm + " from rank " + b.get("root") + " (node " + b.get("rootNode") + ")");
            for (Map<String, Object> r : maps(b.get("ranks"))) {
                boolean same = sha.equals(r.get("sha256"));
                identical &= same;
                System.out.printf("  rank %s / node %s %s %s bytes, sha256 %s... %s%n", r.get("rank"), r.get("node"),
                        r.get("from") == null ? "(root) holds" : "received from rank " + r.get("from") + " in round " + r.get("round") + ":",
                        r.get("bytes"), String.valueOf(r.get("sha256")).substring(0, 16), same ? "identical" : "DIFFERENT");
            }
            printTrace(b);
        }
        System.out.println();
        System.out.printf("  %-8s %-9s %-7s %-16s %s%n", "algorithm", "messages", "rounds", "expected rounds", "time (first send -> last receipt)");
        for (Map.Entry<String, Map<String, Object>> e : byAlgorithm.entrySet()) {
            Map<String, Object> b = e.getValue();
            System.out.printf("  %-8s %-9s %-7s %-16s %s ms%n", e.getKey(), b.get("messages"), b.get("rounds"),
                    b.get("expectedRounds"), b.get("timeMs"));
        }
        verify(identical, "broadcast: all " + n + " ranks hold identical bytes (SHA-256 " + sha.substring(0, 16)
                + "...) with LINEAR and with TREE");
        int linearRounds = num(byAlgorithm.get("LINEAR").get("rounds"));
        int treeRounds = num(byAlgorithm.get("TREE").get("rounds"));
        int log2 = 32 - Integer.numberOfLeadingZeros(n - 1);
        verify(linearRounds == n - 1 && treeRounds == log2 && (n < 4 || treeRounds < linearRounds),
                "broadcast: TREE took " + treeRounds + " rounds = ceil(log2 " + n + "), LINEAR " + linearRounds
                        + " = n - 1, both " + byAlgorithm.get("TREE").get("messages") + " messages");

        int total = 20 % n == 0 ? 20 : 4 * n;
        section("3. Scatter (MPI_Scatter) of 1.." + total + ": chunk i goes to rank i");
        if (total != 20) {
            System.out.println("(20 does not divide by " + n + " ranks, so 1.." + total + " is scattered instead)");
        }
        List<Integer> data = new ArrayList<>();
        for (int i = 1; i <= total; i++) {
            data.add(i);
        }
        Map<String, Object> scatter = post("/api/collective/scatter", Map.of("data", data));
        int chunk = num(scatter.get("chunkSize"));
        boolean chunksOk = true;
        for (Map<String, Object> r : maps(scatter.get("ranks"))) {
            int rank = num(r.get("rank"));
            List<Integer> expected = data.subList(rank * chunk, (rank + 1) * chunk);
            boolean same = expected.equals(ints(r.get("chunk")));
            chunksOk &= same;
            System.out.printf("  rank %d / node %s %s %s %s%n", rank, r.get("node"),
                    rank == num(scatter.get("root")) ? "(root) keeps" : "received", r.get("chunk"), same ? "" : "WRONG");
        }
        printTrace(scatter);
        verify(chunksOk, "scatter: rank i holds exactly elements " + "[i*" + chunk + ", (i+1)*" + chunk + ") of 1.." + total);

        section("4. Gather (MPI_Gather) with a random delay per rank");
        Map<String, Object> gather = post("/api/collective/gather", Map.of("randomDelay", true));
        System.out.printf("  %-5s %-5s %-10s %-9s %s%n", "rank", "node", "delay ms", "arrived", "chunk sent");
        for (Map<String, Object> r : maps(gather.get("ranks"))) {
            System.out.printf("  %-5s %-5s %-10s %-9s %s%n", r.get("rank"), r.get("node"), r.get("delayMs"),
                    r.get("arrived") == null ? "root" : "#" + r.get("arrived"), r.get("chunk"));
        }
        System.out.println("  arrival order at the root : " + ranks(gather.get("arrivalOrder")));
        System.out.println("  final buffer order        : " + ranks(gather.get("finalOrder")));
        System.out.println("  root buffer               : " + gather.get("result"));
        boolean gatherOk = data.equals(ints(gather.get("result"))) && Boolean.TRUE.equals(gather.get("inRankOrder"));
        verify(gatherOk, "gather: the root's buffer is 1.." + total + " in rank order although the chunks arrived as "
                + ranks(gather.get("arrivalOrder")) + (Boolean.TRUE.equals(gather.get("outOfOrder")) ? "" : " (in order this time)"));

        section("5. Scatter -> compute -> gather: every rank squares its own chunk");
        Map<String, Object> demo = post("/api/collective/demo", Map.of());
        int big = num(demo.get("n"));
        for (Map<String, Object> r : maps(map(demo.get("compute")).get("ranks"))) {
            System.out.printf("  rank %s / node %s received %s -> squared %s%n", r.get("rank"), r.get("node"), r.get("before"), r.get("after"));
        }
        System.out.println("  gathered at rank " + demo.get("root") + " (node " + demo.get("rootNode") + "): " + map(demo.get("gather")).get("result"));
        long local = 0;
        for (int i = 1; i <= big; i++) {
            local += (long) i * i;
        }
        long sum = ((Number) demo.get("sumOfSquares")).longValue();
        System.out.println("  sum of the gathered squares = " + sum + "; computed here: 1^2 + ... + " + big + "^2 = " + local);
        verify(sum == local && Boolean.TRUE.equals(demo.get("verified")),
                "scatter-compute-gather: sum of squares " + sum + " equals the local computation " + local);

        int odd = total + 2;
        section("6. A Scatter of " + odd + " ints over " + n + " ranks (not divisible) is refused");
        List<Integer> oddData = new ArrayList<>();
        for (int i = 1; i <= odd; i++) {
            oddData.add(i);
        }
        HttpResponse<String> refused = send("POST", "/api/collective/scatter", JsonUtil.toJson(Map.of("data", oddData)));
        Object error = json(refused).get("error");
        System.out.println("  HTTP " + refused.statusCode() + ": " + error);
        verify(refused.statusCode() == 400 && String.valueOf(error).contains("remainder of " + (odd % n)),
                "a Scatter of " + odd + " ints over " + n + " ranks is refused with 400 (remainder " + (odd % n) + "), nothing padded");

        section("Result");
        verdicts.forEach(System.out::println);
        System.out.printf("%n%s in %.1f s%n", allOk ? "ALL VERIFIED" : "SOME CHECKS FAILED", (System.currentTimeMillis() - start) / 1000.0);
    }

    private void printTrace(Map<String, Object> r) {
        System.out.println("  trace (ms after the start):  round  from -> to          bytes  sent      received");
        for (Map<String, Object> t : maps(r.get("trace"))) {
            System.out.printf("                               %-6s rank %s -> rank %-5s %-6s %-9s %s%n", t.get("round"),
                    t.get("from"), t.get("to"), t.get("bytes"), t.get("sentAtMs"), t.get("receivedAtMs"));
        }
        System.out.println("  " + r.get("messages") + " messages, " + r.get("bytesSent") + " bytes, " + r.get("rounds")
                + " rounds, " + r.get("timeMs") + " ms; event Lamport " + r.get("lamport"));
    }

    private void verify(boolean ok, String what) {
        allOk &= ok;
        verdicts.add((ok ? "VERIFIED " : "NOT VERIFIED ") + what);
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("---------------------------------------------------------------------");
        System.out.println(title);
        System.out.println("---------------------------------------------------------------------");
    }

    private Map<String, Object> post(String path, Map<String, Object> body) throws Exception {
        HttpResponse<String> res = send("POST", path, JsonUtil.toJson(body));
        if (res.statusCode() != 200) {
            throw new IllegalStateException("POST " + path + " -> " + res.statusCode() + " " + res.body());
        }
        return json(res);
    }

    private HttpResponse<String> send(String method, String path, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(BASE + path)).timeout(Duration.ofSeconds(15));
        if (body == null) {
            b.GET();
        } else {
            b.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body));
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static Map<String, Object> json(HttpResponse<String> res) {
        return JsonUtil.parseObject(res.body());
    }

    private static String ranks(Object order) {
        List<String> out = new ArrayList<>();
        for (Object o : (List<?>) order) {
            out.add("rank " + o);
        }
        return String.join(", ", out);
    }

    private static List<Integer> ints(Object list) {
        List<Integer> out = new ArrayList<>();
        for (Object o : (List<?>) list) {
            out.add(((Number) o).intValue());
        }
        return out;
    }

    private static int num(Object o) {
        return ((Number) o).intValue();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object o) {
        return (Map<String, Object>) o;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> maps(Object o) {
        return (List<Map<String, Object>>) o;
    }
}
