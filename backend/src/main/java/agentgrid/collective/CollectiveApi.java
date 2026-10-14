package agentgrid.collective;

import agentgrid.control.ClusterMonitor;
import agentgrid.control.EventLog;
import agentgrid.control.JsonUtil;
import agentgrid.node.ClusterConfig;
import agentgrid.node.MemberSpec;
import agentgrid.node.Membership;
import agentgrid.node.TimeoutSocketFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntFunction;

/**
 * Exp 9: the control plane's side of the MPI-style collectives, behind GET /api/collective and
 * POST /api/collective/{broadcast, scatter, gather, demo}.
 *
 * Communicator: the current members sorted by node id; rank = index, size = n, plus the membership
 * epoch. A member that is down stays in it: as in MPI, a dead process fails the collective (naming
 * its rank) instead of silently shifting every rank after it. The root defaults to the leader's rank.
 *
 * An operation first connects to every rank's collective port, so an unreachable rank fails it before
 * any rank has started; then it sends each rank its START frame and collects every rank's answer.
 * Ranks bound their own waits by the timeout and get REPLY_SLACK_MS more to report; a rank whose
 * connection drops (its process died) fails the operation at once. One operation runs at a time.
 */
public final class CollectiveApi {

    public static final int MAX_MESSAGE_CHARS = 1024;
    public static final int MAX_INTS = 1000;
    /** Gather's optional random delays are a shuffle of 0, 60, 120, ... ms: random, but no two ranks tie. */
    static final long DELAY_STEP_MS = 60;
    static final long REPLY_SLACK_MS = 500;

    /** An HTTP status and message for the handler. */
    public static final class ApiError extends Exception {
        private static final long serialVersionUID = 1L;
        private final int status;

        ApiError(int status, String message) {
            super(message);
            this.status = status;
        }

        public int getStatus() {
            return status;
        }
    }

    private record Rank(int rank, int node, int port, int rmiPort, boolean up) {}

    /** The communicator an operation runs over; leaderRank is 0 while no leader is agreed. */
    private record Comm(long epoch, List<Rank> ranks, Integer leader, int leaderRank) {
        int size() {
            return ranks.size();
        }

        int node(int rank) {
            return ranks.get(rank).node();
        }

        String who(int rank) {
            return "rank " + rank + " (node " + node(rank) + ")";
        }
    }

    /** One rank's answer, or why there is none; gone means its connection dropped mid-operation. */
    private record Answer(int rank, Map<String, Object> result, String error, boolean gone) {}

    private interface Body {
        Map<String, Object> run(Comm c) throws ApiError;
    }

    private final class Call {
        final long id = opIds.incrementAndGet();
        final byte op;
        final Comm comm;
        final int root;
        final IntFunction<Map<String, Object>> params;
        final long startNs = System.nanoTime();
        long t0Ns;

        /** extra(rank) adds a rank's own START parameters to the common ones. */
        Call(byte op, Comm comm, int root, java.util.function.BiConsumer<Integer, Map<String, Object>> extra) {
            this.op = op;
            this.comm = comm;
            this.root = root;
            this.params = rank -> {
                Map<String, Object> p = new LinkedHashMap<>();
                p.put("root", root);
                List<Integer> nodes = new ArrayList<>();
                List<Integer> ports = new ArrayList<>();
                for (Rank r : comm.ranks()) {
                    nodes.add(r.node());
                    ports.add(r.port());
                }
                p.put("nodes", nodes);
                p.put("ports", ports);
                p.put("timeoutMs", config.getCollectiveTimeoutMs());
                extra.accept(rank, p);
                return p;
            };
        }
    }

    private final ClusterConfig config;
    private final ClusterMonitor monitor;
    private final EventLog eventLog;
    private final ExecutorService pool = Executors.newCachedThreadPool(CollectiveNode.daemonThreads("collective-call-"));
    private final ReentrantLock lock = new ReentrantLock();
    /** Seeded with the clock so op ids keep growing across control-plane restarts (nodes drop older ones' stragglers). */
    private final AtomicLong opIds = new AtomicLong(System.currentTimeMillis());
    private volatile String running;

    public CollectiveApi(ClusterConfig config, ClusterMonitor monitor, EventLog eventLog) {
        this.config = config;
        this.monitor = monitor;
        this.eventLog = eventLog;
    }

    private Comm communicator() {
        Membership m = config.membership();
        Map<Integer, Boolean> up = new HashMap<>();
        for (ClusterMonitor.NodeStatus s : monitor.getSnapshot()) {
            up.put(s.getId(), s.isUp());
        }
        List<Rank> ranks = new ArrayList<>();
        for (MemberSpec s : m.getMembers().values()) {
            ranks.add(new Rank(ranks.size(), s.getId(), s.getPort() + config.getCollectivePortOffset(), s.getPort(),
                    up.getOrDefault(s.getId(), false)));
        }
        Object l = monitor.getElectionTracker().toMap().get("leaderId");
        Integer leader = l instanceof Integer && m.contains((Integer) l) ? (Integer) l : null;
        int leaderRank = leader == null ? 0 : m.ids().indexOf(leader);
        return new Comm(m.getEpoch(), Collections.unmodifiableList(ranks), leader, leaderRank);
    }

    /** GET /api/collective: the communicator (ranks, nodes, ports, root, epoch) and the limits. */
    public Map<String, Object> view() {
        Comm c = communicator();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("epoch", c.epoch());
        out.put("size", c.size());
        out.put("root", c.leaderRank());
        out.put("rootNode", c.node(c.leaderRank()));
        out.put("leader", c.leader());
        out.put("timeoutMs", config.getCollectiveTimeoutMs());
        out.put("portOffset", config.getCollectivePortOffset());
        out.put("maxMessageChars", MAX_MESSAGE_CHARS);
        out.put("maxInts", MAX_INTS);
        List<Map<String, Object>> ranks = new ArrayList<>();
        for (Rank r : c.ranks()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("rank", r.rank());
            m.put("node", r.node());
            m.put("port", r.port());
            m.put("rmiPort", r.rmiPort());
            m.put("up", r.up());
            m.put("role", r.rank() == c.leaderRank() ? "ROOT" : "");
            ranks.add(m);
        }
        out.put("ranks", ranks);
        out.put("running", running);
        return out;
    }

    /** POST /api/collective/{op}: validates the request (400) and runs the operation. */
    public Map<String, Object> run(String op, Map<String, Object> req) throws ApiError {
        switch (op) {
            case "broadcast" -> {
                Object m = req.get("message");
                if (!(m instanceof String) || ((String) m).isEmpty() || ((String) m).length() > MAX_MESSAGE_CHARS) {
                    throw new ApiError(400, "Field 'message' must be a string of 1-" + MAX_MESSAGE_CHARS + " characters"
                            + (m instanceof String ? " (got " + ((String) m).length() + ")" : ""));
                }
                String algorithm = req.get("algorithm") == null ? "TREE" : String.valueOf(req.get("algorithm")).trim().toUpperCase();
                if (!algorithm.equals("LINEAR") && !algorithm.equals("TREE")) {
                    throw new ApiError(400, "Field 'algorithm' must be \"LINEAR\" or \"TREE\" (default TREE)");
                }
                return locked("Broadcast", c -> broadcast(c, root(req, c), (String) m, algorithm));
            }
            case "scatter" -> {
                int[] data = data(req.get("data"));
                return locked("Scatter", c -> scatter(c, root(req, c), divisible(data, c)));
            }
            case "gather" -> {
                Object d = req.get("randomDelay");
                if (d != null && !(d instanceof Boolean)) {
                    throw new ApiError(400, "Field 'randomDelay' must be true or false (default false)");
                }
                return locked("Gather", c -> gather(c, root(req, c), Boolean.TRUE.equals(d)));
            }
            case "demo" -> {
                return locked("scatter-compute-gather demo", c -> demo(c, root(req, c)));
            }
            default -> throw new ApiError(404, "Unknown collective endpoint: /api/collective/" + op
                    + " (broadcast, scatter, gather, demo)");
        }
    }

    private Map<String, Object> locked(String what, Body body) throws ApiError {
        if (!lock.tryLock()) {
            throw new ApiError(409, "Another collective is running (" + running + "); try again when it has finished");
        }
        try {
            running = what;
            return body.run(communicator());
        } finally {
            running = null;
            lock.unlock();
        }
    }

    private static int root(Map<String, Object> req, Comm c) throws ApiError {
        Object r = req.get("root");
        if (r == null) {
            return c.leaderRank();
        }
        if (!(r instanceof Long) || (Long) r < 0 || (Long) r >= c.size()) {
            throw new ApiError(400, "Field 'root' must be a rank from 0 to " + (c.size() - 1)
                    + " (default: the leader's rank, " + c.leaderRank() + ")");
        }
        return ((Long) r).intValue();
    }

    private static int[] data(Object d) throws ApiError {
        if (!(d instanceof List<?> list) || list.isEmpty()) {
            throw new ApiError(400, "Field 'data' must be a non-empty array of integers");
        }
        if (list.size() > MAX_INTS) {
            throw new ApiError(400, "Field 'data' has " + list.size() + " ints; at most " + MAX_INTS + " are allowed");
        }
        int[] out = new int[list.size()];
        for (int i = 0; i < out.length; i++) {
            if (!(list.get(i) instanceof Long v) || v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new ApiError(400, "Field 'data' must hold 32-bit integers; element " + i + " is " + list.get(i));
            }
            out[i] = v.intValue();
        }
        return out;
    }

    /** MPI_Scatter sends every rank the same count: a length that does not divide is refused, never padded. */
    private static int[] divisible(int[] data, Comm c) throws ApiError {
        int n = c.size();
        int rem = data.length % n;
        if (rem != 0) {
            int lower = data.length - rem;
            int upper = lower + n;
            String sizes = lower == 0 ? String.valueOf(upper) : upper > MAX_INTS ? String.valueOf(lower) : lower + " or " + upper;
            throw new ApiError(400, "Scatter needs a length divisible by the communicator size: " + data.length
                    + " ints over " + n + " ranks leaves a remainder of " + rem + ". Nothing is padded; send "
                    + sizes + " ints.");
        }
        return data;
    }

    // =========================================================================
    // Operations
    // =========================================================================

    private Map<String, Object> broadcast(Comm c, int root, String message, String algorithm) throws ApiError {
        Call call = new Call(Frame.BCAST, c, root, (rank, p) -> {
            p.put("algorithm", algorithm);
            if (rank == root) {
                p.put("message", message);   // only the root starts with the data
            }
        });
        Map<Integer, Map<String, Object>> results = execute(call);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        String sha = CollectiveNode.sha256(bytes);
        boolean identical = true;
        List<Map<String, Object>> ranks = new ArrayList<>();
        for (Map.Entry<Integer, Map<String, Object>> e : results.entrySet()) {
            Map<String, Object> r = e.getValue();
            boolean same = sha.equals(r.get("sha256"));
            identical &= same;
            Map<String, Object> got = maps(r.get("received")).isEmpty() ? Map.of() : maps(r.get("received")).get(0);
            Map<String, Object> m = rankEntry(c, e.getKey());
            m.put("received", r.get("text"));
            m.put("bytes", r.get("bytes"));
            m.put("sha256", r.get("sha256"));
            m.put("identical", same);
            m.put("from", got.get("from"));
            m.put("round", got.get("round"));
            ranks.add(m);
        }
        Map<String, Object> out = header(call, "BROADCAST", "MPI_Bcast");
        out.put("algorithm", algorithm);
        out.put("message", message);
        out.put("bytes", bytes.length);
        out.put("sha256", sha);
        out.put("identical", identical);
        out.put("expectedRounds", algorithm.equals("TREE") ? 32 - Integer.numberOfLeadingZeros(c.size() - 1) : c.size() - 1);
        out.put("ranks", ranks);
        totals(call, out, results);
        return finish(call, out, "MPI_Bcast " + algorithm + " of " + bytes.length + " bytes from " + c.who(root) + " to "
                + c.size() + " ranks: " + out.get("messages") + " messages in " + out.get("rounds") + " rounds, "
                + out.get("timeMs") + " ms; " + (identical ? "every rank holds identical bytes" : "ranks DIFFER"));
    }

    private Map<String, Object> scatter(Comm c, int root, int[] data) throws ApiError {
        Call call = new Call(Frame.SCATTER, c, root, (rank, p) -> {
            if (rank == root) {
                p.put("data", data);
            }
        });
        Map<Integer, Map<String, Object>> results = execute(call);
        int chunk = data.length / c.size();
        boolean correct = true;
        List<Map<String, Object>> ranks = new ArrayList<>();
        for (Map.Entry<Integer, Map<String, Object>> e : results.entrySet()) {
            List<Object> got = list(e.getValue().get("chunk"));
            boolean ok = got.size() == chunk;
            for (int i = 0; ok && i < chunk; i++) {
                ok = ((Number) got.get(i)).intValue() == data[e.getKey() * chunk + i];
            }
            correct &= ok;
            Map<String, Object> m = rankEntry(c, e.getKey());
            m.put("chunk", got);
            m.put("correct", ok);
            ranks.add(m);
        }
        Map<String, Object> out = header(call, "SCATTER", "MPI_Scatter");
        out.put("data", data);
        out.put("chunkSize", chunk);
        out.put("correct", correct);
        out.put("ranks", ranks);
        totals(call, out, results);
        return finish(call, out, "MPI_Scatter of " + data.length + " ints from " + c.who(root) + ": a chunk of " + chunk
                + " to each of " + c.size() + " ranks in rank order, " + out.get("messages") + " messages, "
                + out.get("timeMs") + " ms; " + (correct ? "rank i holds chunk i" : "chunks WRONG"));
    }

    private Map<String, Object> gather(Comm c, int root, boolean randomDelay) throws ApiError {
        long[] delays = new long[c.size()];
        if (randomDelay) {
            List<Long> steps = new ArrayList<>();
            for (int i = 0; i < c.size() - 1; i++) {
                steps.add(i * DELAY_STEP_MS);
            }
            Collections.shuffle(steps);
            for (int r = 0, i = 0; r < c.size(); r++) {
                if (r != root) {
                    delays[r] = steps.get(i++);
                }
            }
        }
        Call call = new Call(Frame.GATHER, c, root, (rank, p) -> p.put("delayMs", delays[rank]));
        Map<Integer, Map<String, Object>> results = execute(call);
        List<Object> arrival = list(results.get(root).get("arrival"));
        List<Object> result = list(results.get(root).get("result"));
        // The root's buffer must be every rank's chunk in rank order, whatever the arrival order was.
        List<Object> inRankOrder = new ArrayList<>();
        List<Map<String, Object>> ranks = new ArrayList<>();
        for (Map.Entry<Integer, Map<String, Object>> e : results.entrySet()) {
            int r = e.getKey();
            inRankOrder.addAll(list(e.getValue().get("chunk")));
            Map<String, Object> m = rankEntry(c, r);
            m.put("chunk", e.getValue().get("chunk"));
            m.put("delayMs", delays[r]);
            m.put("arrived", r == root ? null : arrival.indexOf((long) r) + 1);
            ranks.add(m);
        }
        List<Integer> finalOrder = new ArrayList<>();
        for (int r = 0; r < c.size(); r++) {
            finalOrder.add(r);
        }
        boolean outOfOrder = false;
        for (int i = 1; i < arrival.size(); i++) {
            outOfOrder |= ((Number) arrival.get(i)).intValue() < ((Number) arrival.get(i - 1)).intValue();
        }
        Map<String, Object> out = header(call, "GATHER", "MPI_Gather");
        out.put("randomDelay", randomDelay);
        out.put("arrivalOrder", arrival);
        out.put("finalOrder", finalOrder);
        out.put("outOfOrder", outOfOrder);
        out.put("result", result);
        out.put("inRankOrder", result.equals(inRankOrder));
        out.put("ranks", ranks);
        totals(call, out, results);
        return finish(call, out, "MPI_Gather to " + c.who(root) + " of " + result.size() + " ints from " + c.size()
                + " ranks" + (randomDelay ? " with random delays" : "") + ": arrival order " + arrival
                + ", buffer in rank order " + finalOrder + "; " + (result.equals(inRankOrder) ? "correct" : "WRONG ORDER"));
    }

    /** Scatter 1..N (N = 4 x size), every rank squares its chunk, gather: the full scatter-compute-gather pipeline. */
    private Map<String, Object> demo(Comm c, int root) throws ApiError {
        int total = 4 * c.size();
        int[] data = new int[total];
        for (int i = 0; i < total; i++) {
            data[i] = i + 1;
        }
        Map<String, Object> scatter = scatter(c, root, data);

        Call call = new Call(Frame.COMPUTE, c, root, (rank, p) -> { });
        Map<Integer, Map<String, Object>> results = execute(call);
        List<Map<String, Object>> computed = new ArrayList<>();
        for (Map.Entry<Integer, Map<String, Object>> e : results.entrySet()) {
            Map<String, Object> m = rankEntry(c, e.getKey());
            m.put("before", e.getValue().get("before"));
            m.put("after", e.getValue().get("chunk"));
            computed.add(m);
        }
        Map<String, Object> compute = new LinkedHashMap<>();
        compute.put("step", "every rank squares the chunk it holds, locally (no messages)");
        compute.put("ranks", computed);
        compute.put("totalMs", ms(System.nanoTime() - call.startNs));

        Map<String, Object> gather = gather(c, root, false);
        long sum = 0;
        boolean squares = true;
        List<Object> result = list(gather.get("result"));
        for (int i = 0; i < result.size(); i++) {
            long v = ((Number) result.get(i)).longValue();
            sum += v;
            squares &= v == (long) (i + 1) * (i + 1);
        }
        long expected = (long) total * (total + 1) * (2L * total + 1) / 6;
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("op", "SCATTER_COMPUTE_GATHER");
        out.put("n", total);
        out.put("root", root);
        out.put("rootNode", c.node(root));
        out.put("scatter", scatter);
        out.put("compute", compute);
        out.put("gather", gather);
        out.put("sumOfSquares", sum);
        out.put("expected", expected);
        out.put("verified", squares && result.size() == total && sum == expected);
        return out;
    }

    // =========================================================================
    // Running an operation on every rank
    // =========================================================================

    /** Every rank's answer by rank; an ApiError (also recorded as the operation's event) names the rank at fault. */
    private Map<Integer, Map<String, Object>> execute(Call call) throws ApiError {
        Comm c = call.comm;
        int n = c.size();
        List<Future<Socket>> connecting = new ArrayList<>();
        for (Rank r : c.ranks()) {
            connecting.add(pool.submit(() -> connect(r.port())));
        }
        Socket[] sockets = new Socket[n];
        String unreachable = null;
        for (int i = 0; i < n; i++) {
            try {
                sockets[i] = connecting.get(i).get();
            } catch (ExecutionException e) {
                if (unreachable == null) {
                    unreachable = c.who(i) + " did not respond: " + CollectiveNode.reason(e.getCause());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                unreachable = "interrupted";
            }
        }
        if (unreachable != null) {
            closeAll(sockets);
            throw failed(call, 503, unreachable);
        }

        long waitMs = config.getCollectiveTimeoutMs() + REPLY_SLACK_MS;
        call.t0Ns = System.nanoTime();
        CompletionService<Answer> answers = new ExecutorCompletionService<>(pool);
        for (int i = 0; i < n; i++) {
            int rank = i;
            answers.submit(() -> exchange(sockets[rank], call, rank, waitMs));
        }
        Map<Integer, Map<String, Object>> results = new TreeMap<>();
        TreeMap<Integer, String> silent = new TreeMap<>();
        try {
            for (int k = 0; k < n; k++) {
                // Each exchange is bounded by its socket timeout; this bound only guards against a stuck task.
                Future<Answer> f = answers.poll(waitMs + REPLY_SLACK_MS, TimeUnit.MILLISECONDS);
                if (f == null) {
                    break;
                }
                Answer a = f.get();
                if (a.gone()) {
                    closeAll(sockets);
                    throw failed(call, 503, c.who(a.rank()) + " did not respond: " + a.error());
                }
                if (a.result() == null) {
                    silent.put(a.rank(), a.error());
                } else {
                    results.put(a.rank(), a.result());
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            closeAll(sockets);
            throw failed(call, 500, "interrupted while waiting for the ranks: " + e);
        }
        for (int r = 0; r < n; r++) {
            if (!results.containsKey(r)) {
                silent.putIfAbsent(r, "no answer within " + waitMs + " ms");
            }
        }
        if (!silent.isEmpty()) {
            closeAll(sockets);
            throw failed(call, 503, c.who(silent.firstKey()) + " did not respond: " + silent.firstEntry().getValue());
        }
        // Every rank answered: the most telling failure wins (a membership change explains the others).
        Map<String, Object> worst = null;
        for (Map<String, Object> r : results.values()) {
            if (!Boolean.TRUE.equals(r.get("ok")) && (worst == null || severity(r) > severity(worst))) {
                worst = r;
            }
        }
        if (worst != null) {
            int status = switch (String.valueOf(worst.get("code"))) {
                case "NO_RESPONSE" -> 503;
                case "EPOCH", "NO_CHUNK" -> 409;
                default -> 500;
            };
            throw failed(call, status, String.valueOf(worst.get("error")));
        }
        long now = config.membership().getEpoch();
        if (now != c.epoch()) {
            throw failed(call, 409, "the membership changed during the operation (epoch " + c.epoch() + " -> " + now
                    + "), so the ranks may have shifted; run it again");
        }
        return results;
    }

    private static int severity(Map<String, Object> r) {
        return switch (String.valueOf(r.get("code"))) {
            case "EPOCH" -> 3;
            case "NO_CHUNK" -> 2;
            case "NO_RESPONSE" -> 1;
            default -> 0;
        };
    }

    private static Socket connect(int port) throws IOException {
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), TimeoutSocketFactory.CONNECT_TIMEOUT_MS);
            s.setTcpNoDelay(true);
            return s;
        } catch (IOException e) {
            s.close();
            throw e;
        }
    }

    /** START to one rank and its answer, on the connection opened for it. */
    private Answer exchange(Socket socket, Call call, int rank, long waitMs) {
        try (socket) {
            socket.setSoTimeout((int) waitMs);
            new Frame(call.id, call.op, call.comm.epoch(), Frame.CONTROL, rank, 0,
                    JsonUtil.toJson(call.params.apply(rank)).getBytes(StandardCharsets.UTF_8))
                    .write(new DataOutputStream(new BufferedOutputStream(socket.getOutputStream())));
            Frame reply = Frame.read(new DataInputStream(new BufferedInputStream(socket.getInputStream())));
            return new Answer(rank, JsonUtil.parseObject(new String(reply.payload(), StandardCharsets.UTF_8)), null, false);
        } catch (SocketTimeoutException e) {
            return new Answer(rank, null, "no answer within " + waitMs + " ms", false);
        } catch (IOException e) {
            return new Answer(rank, null, "its connection dropped mid-operation (" + CollectiveNode.reason(e)
                    + "; process died?)", true);
        } catch (RuntimeException e) {
            return new Answer(rank, null, "unreadable answer (" + e.getMessage() + ")", false);
        }
    }

    private static void closeAll(Socket[] sockets) {
        for (Socket s : sockets) {
            try {
                if (s != null) {
                    s.close();
                }
            } catch (IOException ignored) {
            }
        }
    }

    // =========================================================================
    // Results, traces, events
    // =========================================================================

    private static Map<String, Object> header(Call call, String op, String mpi) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("op", op);
        out.put("mpi", mpi);
        out.put("opId", call.id);
        out.put("epoch", call.comm.epoch());
        out.put("size", call.comm.size());
        out.put("root", call.root);
        out.put("rootNode", call.comm.node(call.root));
        return out;
    }

    private static Map<String, Object> rankEntry(Comm c, int rank) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("rank", rank);
        m.put("node", c.node(rank));
        return m;
    }

    /**
     * The message trace (each data message with its sender's and receiver's stamps, joined on
     * from, to and round, in send order, in ms after the START frames went out) and its totals:
     * messages, bytes, rounds, and time from the first send to the last receipt.
     */
    private static void totals(Call call, Map<String, Object> out, Map<Integer, Map<String, Object>> results) {
        Map<String, Long> receivedAt = new HashMap<>();
        for (Map.Entry<Integer, Map<String, Object>> e : results.entrySet()) {
            for (Map<String, Object> m : maps(e.getValue().get("received"))) {
                receivedAt.put(m.get("from") + ">" + e.getKey() + ">" + m.get("round"), ((Number) m.get("atNs")).longValue());
            }
        }
        List<Map<String, Object>> trace = new ArrayList<>();
        long bytes = 0;
        long rounds = 0;
        double first = Double.MAX_VALUE;
        double last = 0;
        for (Map.Entry<Integer, Map<String, Object>> e : results.entrySet()) {
            for (Map<String, Object> m : maps(e.getValue().get("sent"))) {
                int from = e.getKey();
                int to = ((Number) m.get("to")).intValue();
                long round = ((Number) m.get("round")).longValue();
                Long rec = receivedAt.get(from + ">" + to + ">" + round);
                Map<String, Object> t = new LinkedHashMap<>();
                t.put("from", from);
                t.put("fromNode", call.comm.node(from));
                t.put("to", to);
                t.put("toNode", call.comm.node(to));
                t.put("bytes", m.get("bytes"));
                t.put("round", round);
                double sentAt = ms(((Number) m.get("atNs")).longValue() - call.t0Ns);
                t.put("sentAtMs", sentAt);
                t.put("receivedAtMs", rec == null ? null : ms(rec - call.t0Ns));
                trace.add(t);
                bytes += ((Number) m.get("bytes")).longValue();
                rounds = Math.max(rounds, round);
                first = Math.min(first, sentAt);
                if (rec != null) {
                    last = Math.max(last, ms(rec - call.t0Ns));
                }
            }
        }
        trace.sort(Comparator.comparingDouble(t -> (Double) t.get("sentAtMs")));
        out.put("messages", trace.size());
        out.put("bytesSent", bytes);
        out.put("rounds", rounds);
        out.put("timeMs", trace.isEmpty() ? 0.0 : Math.round((last - first) * 1000) / 1000.0);
        out.put("trace", trace);
    }

    private Map<String, Object> finish(Call call, Map<String, Object> out, String details) {
        out.put("totalMs", ms(System.nanoTime() - call.startNs));
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("opId", call.id);
        fields.put("ok", true);
        fields.put("epoch", call.comm.epoch());
        fields.put("root", call.root);
        fields.put("messages", out.get("messages"));
        fields.put("rounds", out.get("rounds"));
        fields.put("timeMs", out.get("timeMs"));
        EventLog.Event e = record(call, details, fields);
        if (e != null) {
            out.put("lamport", e.getLamport());
            out.put("eventSeq", e.getSeq());
        }
        return out;
    }

    private ApiError failed(Call call, int status, String why) {
        String what = switch (call.op) {
            case Frame.BCAST -> "MPI_Bcast";
            case Frame.SCATTER -> "MPI_Scatter";
            case Frame.GATHER -> "MPI_Gather";
            default -> "the compute step";
        };
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("opId", call.id);
        fields.put("ok", false);
        fields.put("epoch", call.comm.epoch());
        fields.put("root", call.root);
        record(call, what + " from " + call.comm.who(call.root) + " FAILED: " + why, fields);
        return new ApiError(status, what + " failed: " + why);
    }

    /** COLLECTIVE_BCAST / _SCATTER / _GATHER, stamped by the control plane's Lamport clock like its other events. */
    private EventLog.Event record(Call call, String details, Map<String, Object> fields) {
        String type = switch (call.op) {
            case Frame.BCAST -> "COLLECTIVE_BCAST";
            case Frame.SCATTER -> "COLLECTIVE_SCATTER";
            case Frame.GATHER -> "COLLECTIVE_GATHER";
            default -> null;
        };
        return type == null ? null
                : eventLog.record(type, call.comm.node(call.root), details, System.currentTimeMillis(), fields);
    }

    /** Nanoseconds as milliseconds with microsecond precision. */
    private static double ms(long ns) {
        return Math.round(ns / 1000.0) / 1000.0;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object o) {
        return o == null ? Collections.emptyList() : (List<Object>) o;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> maps(Object o) {
        return o == null ? Collections.emptyList() : (List<Map<String, Object>>) o;
    }
}
