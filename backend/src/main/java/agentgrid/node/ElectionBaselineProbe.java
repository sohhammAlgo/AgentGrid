package agentgrid.node;

import agentgrid.election.BullyElectionNode;
import agentgrid.election.ElectionMetrics;
import agentgrid.election.ElectionNodeService;
import agentgrid.election.RingMessage;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * "Before" measurements of fixes D, E and F against the SUBMITTED BullyElectionNode
 * (unmodified), in one JVM, on ports 1711-1715 and 1721-1725.
 *
 *  D: ElectionMetrics' reported convergence vs the true time from startElection() on node 1
 *     until nodes 1-4 all name the new leader.
 *  F: after the leader crashes, nothing starts an election unless someone calls it by hand.
 *  E: with an 800 ms delay injected before node 4 handles ELECTION (a slow or overloaded
 *     higher node), lower nodes claim leadership on the 500 ms timeout while node 4 is alive.
 *
 * Usage (from backend/): java -cp build/classes agentgrid.node.ElectionBaselineProbe
 */
public class ElectionBaselineProbe {

    private static final int N = 5;

    public static void main(String[] args) throws Exception {
        probeDandF();
        probeE();
        System.exit(0);
    }

    private static void probeDandF() throws Exception {
        System.out.println("=== D and F: submitted BullyElectionNode, shared ElectionMetrics (as in ElectionDemo) ===");
        Map<Integer, Integer> ports = new LinkedHashMap<>();
        for (int i = 1; i <= N; i++) {
            ports.put(i, 1710 + i);
        }
        ElectionMetrics metrics = new ElectionMetrics();
        List<BullyElectionNode> nodes = new ArrayList<>();
        for (int i = 1; i <= N; i++) {
            BullyElectionNode node = new BullyElectionNode(i, ports.get(i), ports, metrics);
            Registry r = LocateRegistry.createRegistry(ports.get(i));
            r.rebind("election-node-" + i, node);
            nodes.add(node);
        }
        nodes.get(0).startElection();
        Thread.sleep(1500);
        System.out.println("initial views: " + views(nodes));

        // F: crash the leader and do NOT trigger an election.
        nodes.get(4).simulateCrash();
        long crashAt = System.currentTimeMillis();
        for (int s = 1; s <= 5; s++) {
            Thread.sleep(1000);
            System.out.println("F: +" + (System.currentTimeMillis() - crashAt) + " ms after crash of node 5, "
                    + "no manual trigger: views=" + views(nodes));
        }

        // D: 5 failover runs, reported vs true convergence.
        for (int run = 1; run <= 5; run++) {
            metrics.reset();
            long t0 = System.nanoTime();
            nodes.get(0).startElection();
            long trueMs = -1;
            while ((System.nanoTime() - t0) / 1_000_000 < 5000) {
                boolean all4 = true;
                for (int i = 0; i < 4; i++) {
                    all4 &= nodes.get(i).getLeaderId() == 4;
                }
                if (all4) {
                    trueMs = (System.nanoTime() - t0) / 1_000_000;
                    break;
                }
                Thread.sleep(1);
            }
            Thread.sleep(1500);
            System.out.println("D run " + run + ": true convergence (startElection on node 1 -> nodes 1-4 name 4) = "
                    + trueMs + " ms; ElectionMetrics reported = " + metrics.getConvergenceTimeMs() + " ms; "
                    + "messages = " + metrics.getMessageCount());
            // Put node 5 back as leader for the next run, then crash it again.
            nodes.get(4).recover();
            Thread.sleep(1500);
            nodes.get(4).simulateCrash();
        }
        for (BullyElectionNode node : nodes) {
            node.shutdown();
            UnicastRemoteObject.unexportObject(node, true);
        }
    }

    private static void probeE() throws Exception {
        System.out.println();
        System.out.println("=== E: submitted BullyElectionNode, node 4 handles ELECTION 800 ms late, node 5 crashed ===");
        Map<Integer, Integer> ports = new LinkedHashMap<>();
        for (int i = 1; i <= N; i++) {
            ports.put(i, 1720 + i);
        }
        List<BullyElectionNode> nodes = new ArrayList<>();
        List<Object> keep = new ArrayList<>();
        for (int i = 1; i <= N; i++) {
            BullyElectionNode node = new BullyElectionNode(i, ports.get(i), ports, null);
            Registry r = LocateRegistry.createRegistry(ports.get(i));
            if (i == 4) {
                SlowElectionProxy proxy = new SlowElectionProxy(node, 800);
                keep.add(proxy);
                r.rebind("election-node-" + i, proxy);
            } else {
                r.rebind("election-node-" + i, node);
            }
            nodes.add(node);
        }
        nodes.get(4).simulateCrash();

        int[] last = new int[N];
        java.util.Arrays.fill(last, -1);
        List<String> timeline = new ArrayList<>();
        java.util.Set<Integer> selfClaims = new java.util.TreeSet<>();
        java.util.Set<Integer> named = new java.util.TreeSet<>();
        long t0 = System.nanoTime();
        nodes.get(0).startElection();
        while ((System.nanoTime() - t0) / 1_000_000 < 3000) {
            for (int i = 0; i < 4; i++) {
                int v = nodes.get(i).getLeaderId();
                if (v != last[i]) {
                    last[i] = v;
                    timeline.add(String.format("  +%4d ms  node %d view -> %d%s", (System.nanoTime() - t0) / 1_000_000,
                            i + 1, v, v == i + 1 ? "   (claims leadership)" : ""));
                    if (v == i + 1) {
                        selfClaims.add(i + 1);
                    }
                    if (v > 0) {
                        named.add(v);
                    }
                }
            }
            Thread.sleep(1);
        }
        timeline.forEach(System.out::println);
        System.out.println("E: nodes seen naming themselves leader: " + selfClaims
                + "; leaders named by any node during the run: " + named + " (correct leader: 4)");
        System.out.println("E: final views: " + views(nodes));
        for (BullyElectionNode node : nodes) {
            node.shutdown();
        }
    }

    private static String views(List<BullyElectionNode> nodes) {
        StringBuilder sb = new StringBuilder("{");
        for (BullyElectionNode n : nodes) {
            sb.append(n.getNodeId()).append("=").append(n.getLeaderId()).append(n.getNodeId() < nodes.size() ? ", " : "");
        }
        return sb.append("}").toString();
    }

    /** Delegates to a submitted node, handing it ELECTION messages delayMs late. */
    static final class SlowElectionProxy extends UnicastRemoteObject implements ElectionNodeService {
        private static final long serialVersionUID = 1L;
        private final transient BullyElectionNode target;
        private final long delayMs;
        private final transient ScheduledExecutorService delay = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "slow-proxy");
            t.setDaemon(true);
            return t;
        });

        SlowElectionProxy(BullyElectionNode target, long delayMs) throws RemoteException {
            this.target = target;
            this.delayMs = delayMs;
        }

        @Override
        public void receiveBullyElection(int senderId) {
            delay.schedule(() -> target.receiveBullyElection(senderId), delayMs, TimeUnit.MILLISECONDS);
        }

        @Override public void receiveBullyAnswer(int responderId) { target.receiveBullyAnswer(responderId); }
        @Override public void receiveBullyCoordinator(int leaderId) { target.receiveBullyCoordinator(leaderId); }
        @Override public void receiveRingMessage(RingMessage message) { target.receiveRingMessage(message); }
        @Override public int getNodeId() { return target.getNodeId(); }
        @Override public int getLeaderId() { return target.getLeaderId(); }
        @Override public boolean isLeader() { return target.isLeader(); }
        @Override public boolean isAlive() { return target.isAlive(); }
        @Override public void simulateCrash() { target.simulateCrash(); }
        @Override public void recover() { target.recover(); }
        @Override public void startElection() { target.startElection(); }
    }
}
