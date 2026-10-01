package agentgrid.control;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * End-to-end verifier for Exp 7 in the control plane: MapReduce runs (POST /api/mapreduce/run)
 * and INDEX retrieval for jobs. Drives the control plane's HTTP API only (127.0.0.1:8080 must be
 * running with its 5 nodes). Each check runs in its own try/catch and prints PASS, FAIL or SKIP;
 * the last line is OVERALL.
 *
 * MR3 needs a fresh control plane started without mapreduce/out/inverted_index.json (it is run
 * first; otherwise it is skipped with a message). MR5 needs -Dagentgrid.testHooks=true on the
 * control plane (otherwise skipped). The control plane's pythonCommand must have pyspark.
 *
 * Usage (from backend/): java -cp build/classes agentgrid.control.MapReduceVerifier
 */
public class MapReduceVerifier {

    private static final String BASE = System.getProperty("agentgrid.verifier.base", "http://127.0.0.1:8080");
    private static final String QUERY = "How do leader election and failure detectors handle a crashed node?";
    private static final long JOB_TIMEOUT_MS = 60000;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final Map<String, String> verdicts = new LinkedHashMap<>();
    private final Map<String, String> results = new LinkedHashMap<>();
    private CompletableFuture<Res> firstRun;
    private long firstRunStart;

    /** An HTTP response: status and body. */
    private record Res(int status, String body) {
        Map<String, Object> json() {
            return JsonUtil.parseObject(body);
        }
    }

    private interface Check {
        void run() throws Exception;
    }

    public static void main(String[] args) {
        boolean ok = new MapReduceVerifier().run();
        System.exit(ok ? 0 : 1);
    }

    private boolean run() {
        line("=====================================================================");
        line(" AgentGrid-Lite Exp 7 MapReduce verifier (HTTP API only) -> " + BASE);
        line("=====================================================================");
        guard("setup", () -> {
            line("modules: " + get("/api/modules").body());
            line("mapreduce status: " + get("/api/mapreduce/status").body());
            waitForLeader();
        });

        guard("MR3", this::mr3);
        guard("MR1", this::mr1start);
        guard("MR2", this::mr2);
        guard("MR1", this::mr1finish);
        guard("MR4", this::mr4);
        guard("MR5", this::mr5);
        guard("MR6", this::mr6);

        line("");
        line("=====================================================================");
        line(" RESULT");
        line("=====================================================================");
        boolean all = true;
        int skipped = 0;
        for (Map.Entry<String, String> e : verdicts.entrySet()) {
            System.out.printf("%-4s  %-46s %s%n", e.getValue(), e.getKey(), results.get(e.getKey()));
            all &= !e.getValue().equals("FAIL");
            skipped += e.getValue().equals("SKIP") ? 1 : 0;
        }
        line("OVERALL: " + (all ? "PASS" : "FAIL") + " (" + verdicts.size() + " checks, " + skipped + " skipped)");
        return all;
    }

    /** Runs one check; an exception is that check's FAIL, never the end of the run. */
    private void guard(String name, Check check) {
        try {
            check.run();
        } catch (Exception e) {
            String key = verdicts.keySet().stream().filter(k -> k.startsWith(name + " ")).findFirst().orElse(name + " (exception)");
            verdicts.put(key, "FAIL");
            results.put(key, "exception: " + e);
            line("!! " + name + " threw " + e);
        }
    }

    // =========================================================================
    // MR3: INDEX job before any index exists -> 409
    // =========================================================================

    private void mr3() throws Exception {
        header("MR3 an INDEX job before any MapReduce index exists is refused with 409");
        String name = "MR3 INDEX job without an index -> 409";
        Map<String, Object> ix = get("/api/mapreduce/index").json();
        line("GET /api/mapreduce/index -> " + ix);
        if (Boolean.TRUE.equals(ix.get("available"))) {
            skip(name, "an index file was already loaded at start (" + ix.get("file") + "); MR3 needs a fresh control "
                    + "plane started without mapreduce/out/inverted_index.json");
            return;
        }
        Res r = submit(QUERY, "INDEX");
        line("POST /api/jobs retrieval=INDEX -> " + r.status() + " " + r.body());
        boolean ok = r.status() == 409 && r.body().contains("run MapReduce first");
        check(name, ok, "status " + r.status() + ", message mentions \"run MapReduce first\": " + r.body().contains("run MapReduce first"));
    }

    // =========================================================================
    // MR1 / MR2: a run, and a second run during it
    // =========================================================================

    private void mr1start() {
        header("MR1 run the index job (POST /api/mapreduce/run, in the background)");
        firstRunStart = System.currentTimeMillis();
        firstRun = CompletableFuture.supplyAsync(() -> {
            try {
                return post("/api/mapreduce/run", "{\"job\":\"index\",\"partitions\":4,\"compare\":false}", 330);
            } catch (Exception e) {
                return new Res(-1, String.valueOf(e));
            }
        });
    }

    private void mr2() throws Exception {
        header("MR2 a second run while the first is in progress is refused with 409");
        String name = "MR2 concurrent run -> 409";
        Map<String, Object> st = null;
        for (int i = 0; i < 50 && (st == null || !Boolean.TRUE.equals(st.get("running"))); i++) {
            Thread.sleep(100);
            st = get("/api/mapreduce/status").json();
        }
        line("status during the first run: running=" + st.get("running") + " current=" + st.get("current"));
        Res r = post("/api/mapreduce/run", "{\"job\":\"wordcount\"}", 30);
        line("second POST /api/mapreduce/run -> " + r.status() + " " + r.body());
        check(name, Boolean.TRUE.equals(st.get("running")) && r.status() == 409,
                "status running=" + st.get("running") + ", second run -> " + r.status()
                        + " after " + (System.currentTimeMillis() - firstRunStart) + " ms");
    }

    private void mr1finish() throws Exception {
        String name = "MR1 index run: verified, top docs, ~30 s";
        Res r = firstRun.get();
        long ms = System.currentTimeMillis() - firstRunStart;
        if (r.status() != 200) {
            line("POST /api/mapreduce/run -> " + r.status() + " " + r.body());
            check(name, false, "status " + r.status() + " after " + ms + " ms: " + r.body());
            return;
        }
        Map<String, Object> m = r.json();
        line("result (" + ms + " ms over HTTP): job=" + m.get("job") + " partitions=" + m.get("partitions")
                + " verified=" + m.get("verified") + " docCount=" + m.get("docCount") + " builtAt=" + m.get("builtAt"));
        line("  stages:  " + m.get("stages"));
        line("  timings: " + m.get("timings") + "  wallMs=" + m.get("wallMs"));
        line("  checks:  " + m.get("checks"));
        line("  topTerms: " + m.get("topTerms"));
        List<Object> top = listOf(m.get("topDocsForQuery"));
        for (Object o : top) {
            line("  top doc: " + o);
        }
        Map<String, Object> ix = map(m.get("index"));
        line("  index after the run: " + ix);
        Res last = get("/api/mapreduce/last");
        line("GET /api/mapreduce/last -> " + last.status() + " (builtAt " + (last.status() == 200 ? last.json().get("builtAt") : "-") + ")");
        boolean ok = Boolean.TRUE.equals(m.get("verified")) && !top.isEmpty()
                && ix != null && Boolean.TRUE.equals(ix.get("usable")) && last.status() == 200;
        check(name, ok, "verified=" + m.get("verified") + ", " + top.size() + " top docs ("
                + (top.isEmpty() ? "-" : map(top.get(0)).get("docId")) + " first), index usable=" + (ix == null ? null : ix.get("usable"))
                + ", " + String.format("%.1f", ms / 1000.0) + " s end to end (expected about 30 s)");
    }

    // =========================================================================
    // MR4: SCAN vs INDEX
    // =========================================================================

    private void mr4() throws Exception {
        header("MR4 a SCAN job and an INDEX job: both COMPLETE, same ranking and cited documents, INDEX touches fewer docs");
        String name = "MR4 SCAN vs INDEX equivalent, fewer docs";
        Map<String, Object> scan = runJob(QUERY, "SCAN");
        Map<String, Object> index = runJob(QUERY, "INDEX");
        Map<String, Object> rs = map(scan.get("retrieval"));
        Map<String, Object> ri = map(index.get("retrieval"));
        List<String> rankScan = ranking(scan);
        List<String> rankIndex = ranking(index);
        List<String> citedScan = cited(scan);
        List<String> citedIndex = cited(index);
        for (Map<String, Object> job : List.of(scan, index)) {
            Map<String, Object> r = map(job.get("retrieval"));
            line(job.get("jobId") + " " + r.get("mode") + ": status " + job.get("status") + ", "
                    + job.get("completedSubtasks") + "/" + job.get("subtasks") + " subtasks, makespan " + job.get("makespanMs")
                    + " ms; docs touched " + r.get("docsTouched") + "/" + r.get("docsTotal") + ", RETRIEVE subtasks "
                    + r.get("retrieveSubtasks") + ", retrievalMs " + r.get("retrievalMs"));
        }
        line("INDEX query terms: " + ri.get("queryTerms") + ", index docs: " + ri.get("indexDocs"));
        line("ranking SCAN : " + rankScan);
        line("ranking INDEX: " + rankIndex);
        line("top 5 SCAN : " + head(rankScan, 5));
        line("top 5 INDEX: " + head(rankIndex, 5));
        line("cited SCAN : " + citedScan);
        line("cited INDEX: " + citedIndex);
        boolean sameAnswer = String.valueOf(scan.get("answer")).equals(String.valueOf(index.get("answer")));
        line("answers identical: " + sameAnswer);
        boolean ok = "COMPLETE".equals(scan.get("status")) && "COMPLETE".equals(index.get("status"))
                && "SCAN".equals(rs.get("mode")) && "INDEX".equals(ri.get("mode"))
                && !rankScan.isEmpty() && rankScan.equals(rankIndex)
                && !citedScan.isEmpty() && citedScan.equals(citedIndex) && sameAnswer
                && num(ri.get("docsTouched")) < num(rs.get("docsTouched"));
        check(name, ok, "both " + scan.get("status") + "/" + index.get("status") + ", top 5 " + head(rankIndex, 5)
                + " equal=" + head(rankScan, 5).equals(head(rankIndex, 5)) + ", cited equal=" + citedScan.equals(citedIndex)
                + ", docs touched INDEX " + ri.get("docsTouched") + " vs SCAN " + rs.get("docsTouched")
                + ", RETRIEVE subtasks " + ri.get("retrieveSubtasks") + " vs " + rs.get("retrieveSubtasks"));
    }

    // =========================================================================
    // MR5: stale index
    // =========================================================================

    private void mr5() throws Exception {
        header("MR5 a corrupted fingerprint expectation (test hook) makes the index stale: INDEX -> 409 \"stale\"");
        String name = "MR5 stale index -> 409";
        Res hook = post("/api/mapreduce/index/test-hook", "{\"corruptFingerprint\":true}", 15);
        line("POST /api/mapreduce/index/test-hook {corruptFingerprint:true} -> " + hook.status());
        if (hook.status() == 404) {
            skip(name, "test hooks are disabled; start the control plane with -Dagentgrid.testHooks=true");
            return;
        }
        try {
            Map<String, Object> ix = get("/api/mapreduce/index").json();
            line("index while corrupted: usable=" + ix.get("usable") + " stale=" + ix.get("stale") + " reason=" + ix.get("reason"));
            Res r = submit(QUERY, "INDEX");
            line("POST /api/jobs retrieval=INDEX -> " + r.status() + " " + r.body());
            boolean refused = r.status() == 409 && r.body().contains("stale");
            Res restore = post("/api/mapreduce/index/test-hook", "{\"corruptFingerprint\":false}", 15);
            Map<String, Object> after = get("/api/mapreduce/index").json();
            line("hook restored -> " + restore.status() + "; index usable=" + after.get("usable") + " stale=" + after.get("stale"));
            check(name, Boolean.TRUE.equals(ix.get("stale")) && refused && Boolean.TRUE.equals(after.get("usable")),
                    "stale=" + ix.get("stale") + ", INDEX -> " + r.status() + " (mentions \"stale\": " + r.body().contains("stale")
                            + "), usable again after restore: " + after.get("usable"));
        } finally {
            post("/api/mapreduce/index/test-hook", "{\"corruptFingerprint\":false}", 15);
        }
    }

    // =========================================================================
    // MR6: no retrieval field = SCAN
    // =========================================================================

    private void mr6() throws Exception {
        header("MR6 a job without a retrieval field runs as SCAN");
        String name = "MR6 default job is SCAN";
        Res r = post("/api/jobs", "{\"query\":\"" + QUERY + "\",\"policy\":\"WEIGHTED\"}", 15);
        line("POST /api/jobs (no retrieval) -> " + r.status() + " " + r.body());
        Map<String, Object> job = waitJob((String) r.json().get("jobId"));
        Map<String, Object> rt = map(job.get("retrieval"));
        line(job.get("jobId") + ": status " + job.get("status") + ", retrieval " + rt);
        check(name, r.status() == 200 && "SCAN".equals(r.json().get("retrieval")) && "COMPLETE".equals(job.get("status"))
                        && "SCAN".equals(rt.get("mode")) && num(rt.get("docsTouched")) == num(rt.get("docsTotal"))
                        && num(rt.get("retrieveSubtasks")) == 20,
                "status " + job.get("status") + ", mode " + rt.get("mode") + ", docs touched " + rt.get("docsTouched") + "/"
                        + rt.get("docsTotal") + ", " + rt.get("retrieveSubtasks") + " RETRIEVE subtasks");
    }

    // =========================================================================
    // helpers
    // =========================================================================

    private Res submit(String query, String retrieval) throws Exception {
        return post("/api/jobs", "{\"query\":\"" + query + "\",\"policy\":\"WEIGHTED\",\"retrieval\":\"" + retrieval + "\"}", 15);
    }

    private Map<String, Object> runJob(String query, String retrieval) throws Exception {
        Res r = submit(query, retrieval);
        if (r.status() != 200) {
            throw new IllegalStateException("POST /api/jobs retrieval=" + retrieval + " -> " + r.status() + " " + r.body());
        }
        return waitJob((String) r.json().get("jobId"));
    }

    private Map<String, Object> waitJob(String jobId) throws Exception {
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
        Map<String, Object> job;
        do {
            Thread.sleep(150);
            job = get("/api/jobs/" + jobId).json();
        } while (("RUNNING".equals(job.get("status")) || "QUEUED".equals(job.get("status")))
                && System.currentTimeMillis() < deadline);
        return job;
    }

    /** The RANK stage's order as the SUMMARIZE inputs show it: "doc-09 (rank 1, tf-idf 0.9181)" -> "doc-09 0.9181". */
    private static List<String> ranking(Map<String, Object> job) {
        List<String> out = new ArrayList<>();
        Pattern p = Pattern.compile("(doc-\\d+) \\(rank \\d+, tf-idf ([0-9.]+)\\)");
        for (Object st : listOf(job.get("stages"))) {
            Map<String, Object> stage = map(st);
            if (!"SUMMARIZE".equals(stage.get("type"))) {
                continue;
            }
            for (Object s : listOf(stage.get("subtasks"))) {
                Matcher m = p.matcher(String.valueOf(map(s).get("input")));
                if (m.find()) {
                    out.add(m.group(1) + " " + m.group(2));
                }
            }
        }
        return out;
    }

    /** Document ids cited in the answer, in order. */
    private static List<String> cited(Map<String, Object> job) {
        List<String> out = new ArrayList<>();
        Matcher m = Pattern.compile("\\[(doc-\\d+)\\]").matcher(String.valueOf(job.get("answer")));
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    private static List<String> head(List<String> list, int n) {
        return list.subList(0, Math.min(n, list.size()));
    }

    private void waitForLeader() throws Exception {
        long deadline = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> el = get("/api/election").json();
            int up = 0;
            for (Object n : listOf(JsonUtil.parse(get("/api/cluster").body()))) {
                up += Boolean.TRUE.equals(map(n).get("up")) ? 1 : 0;
            }
            if (el.get("leaderId") != null && Boolean.TRUE.equals(el.get("agreed")) && up == 5) {
                line("cluster: " + up + " nodes up, agreed leader " + el.get("leaderId"));
                return;
            }
            Thread.sleep(500);
        }
        line("cluster: no agreed leader with 5 nodes up after 30 s (continuing)");
    }

    private Res get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path)).timeout(Duration.ofSeconds(15)).GET().build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        return new Res(res.statusCode(), res.body());
    }

    private Res post(String path, String body, int timeoutSeconds) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        return new Res(res.statusCode(), res.body());
    }

    private void check(String name, boolean ok, String detail) {
        verdicts.put(name, ok ? "PASS" : "FAIL");
        results.put(name, detail);
        line((ok ? "PASS " : "FAIL ") + name + ": " + detail);
    }

    private void skip(String name, String why) {
        verdicts.put(name, "SKIP");
        results.put(name, why);
        line("SKIP " + name + ": " + why);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object o) {
        return o instanceof Map ? (Map<String, Object>) o : new LinkedHashMap<>();
    }

    @SuppressWarnings("unchecked")
    private static List<Object> listOf(Object o) {
        return o instanceof List ? (List<Object>) o : new ArrayList<>();
    }

    private static long num(Object o) {
        return o instanceof Number n ? n.longValue() : -1;
    }

    private static void header(String title) {
        line("");
        line("---------------------------------------------------------------------");
        line(title);
        line("---------------------------------------------------------------------");
    }

    private static void line(String s) {
        System.out.println(s);
    }
}
