package agentgrid.collective;

import agentgrid.control.JsonUtil;
import agentgrid.node.TimeoutSocketFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * Exp 9: one node's endpoint for MPI-style collectives (Broadcast, Scatter, Gather) over plain TCP
 * on its RMI port + collectivePortOffset (1701, 1702, ...), loopback only. Every accepted
 * connection gets one reader thread that blocks on the socket; nothing polls.
 *
 * The control plane plays mpirun: it sends every rank of the communicator a START frame naming
 * the operation, the rank (dstRank), the root and the communicator. Each rank then runs its own part
 * of the algorithm, exchanging data frames directly with its peers, and answers with what it now
 * holds and every message it sent and received, stamped with System.nanoTime() (the OS monotonic
 * clock, shared by every JVM on one machine). No rank waits longer than the operation's timeout.
 */
public final class CollectiveNode {

    /** A connection that sends nothing for this long is dropped, so it cannot hold a reader thread forever. */
    private static final int IDLE_TIMEOUT_MS = 60000;

    /** What a rank keeps between operations: its Scatter chunk (squared by COMPUTE), tagged with that Scatter's op id. */
    private record Held(long epoch, long tag, int[] chunk) {}

    private record Arrival(Frame frame, long atNs) {}

    /** A rank's part failed; code NO_RESPONSE, EPOCH, NO_CHUNK or PROTOCOL, culprit the rank at fault. */
    private static final class Failure extends Exception {
        private static final long serialVersionUID = 1L;
        final String code;
        final int culprit;

        Failure(String code, int culprit, String message) {
            super(message);
            this.code = code;
            this.culprit = culprit;
        }
    }

    /** One operation as this rank sees it. */
    private final class Op {
        final long id;
        final byte code;
        final long epoch;
        final int me;
        final int root;
        final int n;
        final List<Integer> nodes;
        final List<Integer> ports;
        final long timeoutMs;
        final long deadlineNs;
        final Map<String, Object> params;
        final Map<String, Object> result;
        final BlockingQueue<Arrival> box;
        final List<Map<String, Object>> sent = new ArrayList<>();
        final List<Map<String, Object>> received = new ArrayList<>();

        Op(Frame start, Map<String, Object> params, Map<String, Object> result) {
            this.id = start.opId();
            this.code = start.op();
            this.epoch = start.epoch();
            this.me = start.dstRank();
            this.root = ((Number) params.get("root")).intValue();
            this.nodes = ints(params.get("nodes"));
            this.ports = ints(params.get("ports"));
            this.n = nodes.size();
            this.timeoutMs = ((Number) params.get("timeoutMs")).longValue();
            this.deadlineNs = System.nanoTime() + timeoutMs * 1_000_000L;
            this.params = params;
            this.result = result;
            this.box = mailbox(id);
        }

        String who(int rank) {
            return "rank " + rank + " (node " + nodes.get(rank) + ")";
        }
    }

    private final int nodeId;
    private final int port;
    private final LongSupplier epoch;
    private final ExecutorService readers = Executors.newCachedThreadPool(daemonThreads("collective-reader-"));
    /** Data frames by operation. A peer's frame can arrive before this rank's own START, so either side creates the queue. */
    private final Map<Long, BlockingQueue<Arrival>> mailboxes = new ConcurrentHashMap<>();
    private volatile Held held;

    /** membershipEpoch: the node's current membership epoch; an operation of another epoch is refused. */
    public CollectiveNode(int nodeId, int port, LongSupplier membershipEpoch) {
        this.nodeId = nodeId;
        this.port = port;
        this.epoch = membershipEpoch;
    }

    public void start() throws IOException {
        ServerSocket server = new ServerSocket(port, 50, InetAddress.getLoopbackAddress());
        Thread acceptor = new Thread(() -> acceptLoop(server), "collective-accept-" + nodeId);
        acceptor.setDaemon(true);
        acceptor.start();
    }

    private void acceptLoop(ServerSocket server) {
        while (true) {
            try {
                Socket s = server.accept();
                readers.execute(() -> serve(s));
            } catch (IOException e) {
                System.err.println("[Node " + nodeId + "] collective accept failed: " + e.getMessage());
                sleep(100);   // never spin on a persistent accept error
            }
        }
    }

    /** One connection: a START from the control plane runs this rank's part and is answered; data frames go to their operation. */
    private void serve(Socket socket) {
        try (socket) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(IDLE_TIMEOUT_MS);
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            while (true) {
                Frame f = Frame.read(in);
                long at = System.nanoTime();
                if (f.srcRank() == Frame.CONTROL) {
                    run(f).write(out);
                } else {
                    mailbox(f.opId()).add(new Arrival(f, at));
                }
            }
        } catch (EOFException e) {
            // the sender closed the connection after its frame(s)
        } catch (IOException e) {
            // a peer or the control plane went away; whoever waits for it times out or has already given up
        }
    }

    private BlockingQueue<Arrival> mailbox(long opId) {
        return mailboxes.computeIfAbsent(opId, k -> new LinkedBlockingQueue<>());
    }

    private Frame run(Frame start) {
        Map<String, Object> result = new LinkedHashMap<>();
        Op op = null;
        // Operations run one at a time, so a queue of an older one only holds stragglers.
        mailboxes.keySet().removeIf(id -> id < start.opId());
        try {
            op = new Op(start, JsonUtil.parseObject(new String(start.payload(), StandardCharsets.UTF_8)), result);
            if (op.me < 0 || op.me >= op.n || op.root < 0 || op.root >= op.n || op.nodes.get(op.me) != nodeId) {
                throw new Failure("PROTOCOL", op.me, "node " + nodeId + " got a START for rank " + op.me + " of " + op.nodes);
            }
            long mine = epoch.getAsLong();
            if (mine != op.epoch) {
                throw new Failure("EPOCH", op.me, op.who(op.me) + " is at membership epoch " + mine
                        + ", the operation at epoch " + op.epoch);
            }
            switch (start.op()) {
                case Frame.BCAST -> broadcast(op);
                case Frame.SCATTER -> scatter(op);
                case Frame.GATHER -> gather(op);
                default -> compute(op);
            }
            result.put("ok", true);
        } catch (Failure f) {
            result.put("ok", false);
            result.put("code", f.code);
            result.put("culprit", f.culprit);
            result.put("error", f.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.put("ok", false);
            result.put("code", "PROTOCOL");
            result.put("error", "node " + nodeId + " was interrupted");
        } catch (RuntimeException e) {
            result.put("ok", false);
            result.put("code", "PROTOCOL");
            result.put("error", "node " + nodeId + " could not run " + Frame.name(start.op()) + ": " + e);
        } finally {
            mailboxes.remove(start.opId());
        }
        result.put("node", nodeId);
        if (op != null) {
            result.put("sent", op.sent);
            result.put("received", op.received);
        }
        return new Frame(start.opId(), start.op(), start.epoch(), start.dstRank(), Frame.CONTROL, 0,
                JsonUtil.toJson(result).getBytes(StandardCharsets.UTF_8));
    }

    // =========================================================================
    // The four operations, as one rank runs them
    // =========================================================================

    private void broadcast(Op op) throws Failure, InterruptedException {
        boolean tree = "TREE".equals(op.params.get("algorithm"));
        int vr = (op.me - op.root + op.n) % op.n;   // rank relative to the root
        byte[] data;
        if (vr == 0) {
            data = ((String) op.params.get("message")).getBytes(StandardCharsets.UTF_8);
        } else {
            // TREE: the parent is this relative rank minus its highest set bit; LINEAR: always the root.
            int parent = tree ? (vr - Integer.highestOneBit(vr) + op.root) % op.n : op.root;
            data = await(op, parent, true).payload();
        }
        if (tree) {
            // MPI_Bcast, binomial tree: in round k every rank that already holds the data sends it to the
            // rank 2^(k-1) further on (relative to the root), so ceil(log2 n) rounds reach all n ranks.
            for (int bit = vr == 0 ? 1 : Integer.highestOneBit(vr) << 1; vr + bit < op.n; bit <<= 1) {
                send(op, (vr + bit + op.root) % op.n, data, Integer.numberOfTrailingZeros(bit) + 1);
            }
        } else if (vr == 0) {
            // MPI_Bcast, linear: the root sends to every other rank in turn, one message per round.
            int round = 0;
            for (int r = 0; r < op.n; r++) {
                if (r != op.root) {
                    send(op, r, data, ++round);
                }
            }
        }
        op.result.put("text", new String(data, StandardCharsets.UTF_8));
        op.result.put("bytes", data.length);
        op.result.put("sha256", sha256(data));
    }

    private void scatter(Op op) throws Failure, InterruptedException {
        int[] chunk;
        if (op.me == op.root) {
            int[] data = ints(op.params.get("data")).stream().mapToInt(Integer::intValue).toArray();
            if (data.length == 0 || data.length % op.n != 0) {
                throw new Failure("PROTOCOL", op.me, "a Scatter of " + data.length + " ints cannot be split into "
                        + op.n + " equal chunks");
            }
            int c = data.length / op.n;
            // MPI_Scatter (linear): chunk i, data[i*c, (i+1)*c), goes to rank i, in rank order.
            int round = 0;
            for (int r = 0; r < op.n; r++) {
                if (r != op.root) {
                    send(op, r, Frame.ints(Arrays.copyOfRange(data, r * c, (r + 1) * c)), ++round);
                }
            }
            chunk = Arrays.copyOfRange(data, op.root * c, (op.root + 1) * c);
        } else {
            chunk = Frame.ints(await(op, op.root, true).payload(), 0);
        }
        held = new Held(op.epoch, op.id, chunk);
        op.result.put("chunk", chunk);
    }

    private void gather(Op op) throws Failure, InterruptedException {
        Held h = held;
        boolean have = h != null && h.epoch() == op.epoch;
        int[] mine = have ? h.chunk() : new int[0];
        long tag = have ? h.tag() : 0;
        op.result.put("chunk", mine);
        if (op.me != op.root) {
            long delayMs = ((Number) op.params.getOrDefault("delayMs", 0L)).longValue();
            op.result.put("delayMs", delayMs);
            Thread.sleep(delayMs);   // the optional random delay, so chunks reach the root out of rank order
            send(op, op.root, ByteBuffer.allocate(8 + mine.length * 4).putLong(tag).put(Frame.ints(mine)).array(), 1);
            return;
        }
        // MPI_Gather: the root puts rank i's chunk at position i of its buffer, whatever order they arrive in.
        int[][] slots = new int[op.n][];
        long[] tags = new long[op.n];
        slots[op.me] = mine;
        tags[op.me] = tag;
        List<Integer> arrival = new ArrayList<>();
        for (int k = 1; k < op.n; k++) {
            int missing = 0;
            while (slots[missing] != null) {
                missing++;
            }
            Frame f = await(op, missing, false);
            int src = f.srcRank();
            if (slots[src] != null) {
                throw new Failure("PROTOCOL", src, "a second GATHER chunk came from " + op.who(src));
            }
            tags[src] = ByteBuffer.wrap(f.payload()).getLong();
            slots[src] = Frame.ints(f.payload(), 8);
            arrival.add(src);
        }
        for (int r = 0; r < op.n; r++) {
            if (slots[r].length == 0) {
                throw new Failure("NO_CHUNK", r, op.who(r) + " holds no chunk of membership epoch " + op.epoch
                        + " (it restarted, or nothing was scattered since the membership changed): run Scatter first");
            }
            if (tags[r] != tags[op.root]) {
                throw new Failure("NO_CHUNK", r, op.who(r) + " holds a chunk of another Scatter (op " + tags[r]
                        + ") than the root (op " + tags[op.root] + "): run Scatter again");
            }
        }
        // Equal tags: every chunk comes from the same Scatter, so all have its chunk size.
        int c = slots[0].length;
        int[] all = new int[c * op.n];
        for (int r = 0; r < op.n; r++) {
            System.arraycopy(slots[r], 0, all, r * c, c);
        }
        op.result.put("result", all);
        op.result.put("arrival", arrival);
    }

    /** The "compute" step of the scatter-compute-gather demo: this rank squares the chunk it holds. */
    private void compute(Op op) throws Failure {
        Held h = held;
        if (h == null || h.epoch() != op.epoch) {
            throw new Failure("NO_CHUNK", op.me, op.who(op.me) + " holds no chunk of membership epoch " + op.epoch
                    + ": run Scatter first");
        }
        int[] squared = new int[h.chunk().length];
        for (int i = 0; i < squared.length; i++) {
            squared[i] = h.chunk()[i] * h.chunk()[i];
        }
        held = new Held(h.epoch(), h.tag(), squared);
        op.result.put("before", h.chunk());
        op.result.put("chunk", squared);
    }

    // =========================================================================
    // Messages
    // =========================================================================

    /** Sends one data frame to rank dst on a new connection; a peer that cannot be reached fails the operation, naming it. */
    private void send(Op op, int dst, byte[] payload, int round) throws Failure {
        long at = System.nanoTime();
        try (Socket s = new Socket()) {
            // Untimed on purpose: on Windows the JDK's timed connect (non-blocking connect + poll) stalls
            // 10-20 ms now and then, which showed up in the traces. On loopback a connect succeeds at once or
            // is refused; a rank that was down before the operation is caught by the control plane's pre-check.
            s.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), op.ports.get(dst)));
            s.setTcpNoDelay(true);
            new Frame(op.id, op.code, op.epoch, op.me, dst, round, payload)
                    .write(new DataOutputStream(new BufferedOutputStream(s.getOutputStream())));
        } catch (IOException e) {
            throw new Failure("NO_RESPONSE", dst, op.who(dst) + " did not respond: " + reason(e));
        }
        op.sent.add(stamp("to", dst, payload.length, round, at));
    }

    /**
     * The next data frame of this operation. Once the timeout has passed it fails with NO_RESPONSE
     * naming waitingFor; only is true when the frame must come from waitingFor itself.
     */
    private Frame await(Op op, int waitingFor, boolean only) throws Failure, InterruptedException {
        Arrival a = op.box.poll(Math.max(0, op.deadlineNs - System.nanoTime()), TimeUnit.NANOSECONDS);
        if (a == null) {
            throw new Failure("NO_RESPONSE", waitingFor, op.who(waitingFor) + " did not respond: " + op.who(op.me)
                    + " got no " + Frame.name(op.code) + " message from it within " + op.timeoutMs + " ms");
        }
        Frame f = a.frame();
        if (f.op() != op.code || f.dstRank() != op.me || f.srcRank() < 0 || f.srcRank() >= op.n
                || (only && f.srcRank() != waitingFor)) {
            throw new Failure("PROTOCOL", f.srcRank(), op.who(op.me) + " expected a " + Frame.name(op.code)
                    + " frame from rank " + waitingFor + ", got " + Frame.name(f.op()) + " from rank " + f.srcRank());
        }
        if (f.epoch() != op.epoch) {
            throw new Failure("EPOCH", f.srcRank(), "a frame from " + op.who(f.srcRank()) + " carries membership epoch "
                    + f.epoch() + ", the operation epoch " + op.epoch);
        }
        op.received.add(stamp("from", f.srcRank(), f.payload().length, f.seq(), a.atNs()));
        return f;
    }

    private static Map<String, Object> stamp(String peerKey, int peer, int bytes, int round, long atNs) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(peerKey, peer);
        m.put("bytes", bytes);
        m.put("round", round);
        m.put("atNs", atNs);
        return m;
    }

    static String reason(Throwable e) {
        if (e instanceof SocketTimeoutException) {
            return "no connection within " + TimeoutSocketFactory.CONNECT_TIMEOUT_MS + " ms (process down?)";
        }
        if (e instanceof ConnectException) {
            return "connection refused (process down?)";
        }
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }

    static String sha256(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);   // every JDK ships SHA-256
        }
    }

    private static List<Integer> ints(Object list) {
        List<Integer> out = new ArrayList<>();
        for (Object o : (List<?>) list) {
            out.add(((Number) o).intValue());
        }
        return out;
    }

    static ThreadFactory daemonThreads(String prefix) {
        AtomicInteger count = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + count.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
