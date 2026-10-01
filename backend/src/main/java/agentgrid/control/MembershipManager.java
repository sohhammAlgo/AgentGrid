package agentgrid.control;

import agentgrid.node.BlackboardState;
import agentgrid.node.ClockCoordinatorService;
import agentgrid.node.ClusterBlackboardService;
import agentgrid.node.ClusterConfig;
import agentgrid.node.MemberSpec;
import agentgrid.node.Membership;
import agentgrid.node.NodeAgent;
import agentgrid.node.NodeProcessManager;
import agentgrid.node.TimeoutSocketFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The single authority for cluster membership (elastic add / remove of nodes).
 *
 * The current membership lives in the control plane's ClusterConfig; this class is the only
 * code that creates a new epoch. Changes are serialised: a change requested while another one
 * is running is rejected with 409. Membership is not persisted: a control-plane restart starts
 * again from cluster.properties.
 *
 * ADD: allocate the next id (ids are never reused, not even after a failed add), start the
 * process with a joining view, wait until it is UP and its blackboard is ready (snapshot pulled
 * from live peers), and only then bump the epoch and push it to every live member, then ask
 * the leader for a Berkeley round. Any failure before the bump kills the new process and leaves
 * the membership unchanged.
 *
 * REMOVE: bump the epoch and push it to the remaining live members first, then stop the
 * process and drop it from the monitor. If it was the leader, the members' failure detectors
 * elect a new one as for any dead leader.
 */
public final class MembershipManager {

    static final int DEFAULT_POOL_SIZE = 4;
    static final int DEFAULT_WEIGHT = 4;
    static final int MAX_POOL_SIZE = 32;
    static final int MAX_WEIGHT = 100;
    static final long UP_TIMEOUT_MS = 20000;
    static final long READY_TIMEOUT_MS = 20000;
    static final long LEADER_WAIT_MS = 10000;
    private static final long PUSH_TIMEOUT_MS = 1500;
    private static final long ROUND_TIMEOUT_MS = 8000;

    private final ClusterConfig config;
    private final NodeProcessManager processManager;
    private final ClusterMonitor monitor;
    private final EventLog eventLog;
    private final JobDirectory jobDirectory;
    private final ReentrantLock changeLock = new ReentrantLock();
    private final AtomicInteger nextId;
    private volatile String changeInProgress;

    public MembershipManager(ClusterConfig config, NodeProcessManager processManager, ClusterMonitor monitor,
                             EventLog eventLog, JobDirectory jobDirectory) {
        this.config = config;
        this.processManager = processManager;
        this.monitor = monitor;
        this.eventLog = eventLog;
        this.jobDirectory = jobDirectory;
        int max = 0;
        for (int id : config.membership().ids()) {
            max = Math.max(max, id);
        }
        this.nextId = new AtomicInteger(max + 1);
    }

    /** Epoch, quorum, member specs, the next id an add will use, and the limits. */
    public Map<String, Object> view() {
        Map<String, Object> m = new LinkedHashMap<>(config.membership().toMap());
        m.put("nextId", nextId.get());
        m.put("minMembers", Membership.MIN_MEMBERS);
        m.put("maxMembers", Membership.MAX_MEMBERS);
        m.put("changeInProgress", changeInProgress);
        // Node processes the control plane started and tracks (by node id): a failed add or a
        // removed node must not leave one behind.
        Map<String, Object> pids = new LinkedHashMap<>();
        processManager.pids().forEach((id, pid) -> pids.put(String.valueOf(id), pid));
        m.put("processes", pids);
        return m;
    }

    // =========================================================================
    // ADD
    // =========================================================================

    public Map<String, Object> add(Integer poolSize, Integer weight) throws JobDirectory.ApiException {
        int pool = poolSize == null ? DEFAULT_POOL_SIZE : poolSize;
        int w = weight == null ? DEFAULT_WEIGHT : weight;
        if (pool < 1 || pool > MAX_POOL_SIZE) {
            throw new JobDirectory.ApiException(400, "poolSize must be 1-" + MAX_POOL_SIZE + ", got " + pool);
        }
        if (w < 1 || w > MAX_WEIGHT) {
            throw new JobDirectory.ApiException(400, "weight must be 1-" + MAX_WEIGHT + ", got " + w);
        }
        lockOrConflict("add");
        try {
            Membership current = config.membership();
            if (current.size() >= Membership.MAX_MEMBERS) {
                throw new JobDirectory.ApiException(409, "the cluster already has " + current.size()
                        + " members, the maximum is " + Membership.MAX_MEMBERS);
            }
            int id = nextId.getAndIncrement();
            MemberSpec spec = MemberSpec.of(id, pool, w);
            changeInProgress = "adding node " + id;
            long t0 = System.currentTimeMillis();

            long readyMs;
            String snapshotSource;
            try {
                Process process = processManager.startJoining(spec, current.joining(spec));
                waitUp(spec, process);
                BlackboardState state = waitReady(spec, process);
                snapshotSource = state.getSnapshotSource();
                if (snapshotSource != null && snapshotSource.startsWith("none") && anyLiveMember(current)) {
                    throw new IllegalStateException("snapshot pull from live peers failed: " + snapshotSource);
                }
                readyMs = System.currentTimeMillis() - t0;
            } catch (Exception e) {
                String reason = rootMessage(e);
                boolean killed = processManager.forget(id);
                eventLog.record("NODE_ADD_FAILED", id, "Adding node " + id + " (port " + spec.getPort() + ") failed: "
                        + reason + "; process " + (killed ? "stopped" : "not running") + "; membership unchanged ("
                        + current + ")", System.currentTimeMillis(), fields("reason", reason, "epoch", current.getEpoch()));
                throw new JobDirectory.ApiException(500, "adding node " + id + " failed: " + reason
                        + " (membership unchanged, epoch " + current.getEpoch() + ")");
            }

            // The node is ready: only now does it become a member.
            Membership next = current.with(spec);
            config.applyMembership(next);
            Map<String, Object> push = push(next);
            eventLog.record("NODE_ADDED", id, "Node " + id + " added (port " + spec.getPort() + ", pool " + pool
                    + ", weight " + w + ") after " + readyMs + " ms; snapshot from " + snapshotSource,
                    System.currentTimeMillis(), spec.toMap());
            recordMembershipChanged(next, "added node " + id);
            monitor.expectElectionActivity(15000);
            monitor.pollAll();

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("success", true);
            resp.put("node", id);
            resp.putAll(spec.toMap());
            resp.put("readyMs", readyMs);
            resp.put("snapshotSource", snapshotSource);
            resp.put("membership", next.toMap());
            resp.put("pushed", push);
            resp.put("clockRound", berkeleyRoundAfterJoin(id));
            resp.put("totalMs", System.currentTimeMillis() - t0);
            return resp;
        } finally {
            changeInProgress = null;
            changeLock.unlock();
        }
    }

    private void waitUp(MemberSpec spec, Process process) throws Exception {
        long deadline = System.currentTimeMillis() + UP_TIMEOUT_MS;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            if (!process.isAlive()) {
                throw new IllegalStateException("node process exited with code " + process.exitValue()
                        + " before it came UP (see build/logs/node-" + spec.getId() + ".log)");
            }
            try {
                NodeAgent agent = (NodeAgent) registry(spec.getPort()).lookup("agent");
                if (monitor.call(agent::ping)) {
                    return;
                }
            } catch (Exception e) {
                last = e;
            }
            Thread.sleep(150);
        }
        throw new IllegalStateException("node did not come UP within " + UP_TIMEOUT_MS + " ms"
                + (last == null ? "" : " (last error: " + rootMessage(last) + ")"));
    }

    private BlackboardState waitReady(MemberSpec spec, Process process) throws Exception {
        long deadline = System.currentTimeMillis() + READY_TIMEOUT_MS;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            if (!process.isAlive()) {
                throw new IllegalStateException("node process exited with code " + process.exitValue()
                        + " during its snapshot pull (see build/logs/node-" + spec.getId() + ".log)");
            }
            try {
                ClusterBlackboardService bb = (ClusterBlackboardService) registry(spec.getPort())
                        .lookup("blackboard-node-node-" + spec.getId());
                BlackboardState state = monitor.call(() -> bb.snapshot("\u0000membership-probe"));
                if (state.isReady()) {
                    return state;
                }
            } catch (Exception e) {
                last = e;
            }
            Thread.sleep(150);
        }
        throw new IllegalStateException("blackboard not ready (snapshot pull) within " + READY_TIMEOUT_MS + " ms"
                + (last == null ? "" : " (last error: " + rootMessage(last) + ")"));
    }

    private boolean anyLiveMember(Membership m) {
        for (ClusterMonitor.NodeStatus s : monitor.getSnapshot()) {
            if (s.isUp() && m.contains(s.getId())) {
                return true;
            }
        }
        return false;
    }

    /** Waits for an agreed leader among the members, then asks it for a Berkeley round. */
    private Map<String, Object> berkeleyRoundAfterJoin(int joined) {
        Map<String, Object> out = new LinkedHashMap<>();
        long deadline = System.currentTimeMillis() + LEADER_WAIT_MS;
        Integer leader = null;
        while (System.currentTimeMillis() < deadline) {
            monitor.pollAll();
            Object l = monitor.getElectionTracker().toMap().get("leaderId");
            if (l instanceof Integer && config.membership().contains((Integer) l)) {
                leader = (Integer) l;
                break;
            }
            sleep(200);
        }
        if (leader == null) {
            out.put("error", "no agreed leader within " + LEADER_WAIT_MS + " ms; the periodic or rejoin round will sync node " + joined);
            return out;
        }
        try {
            ClockCoordinatorService clock = monitor.lookup(leader, "clock", ClockCoordinatorService.class);
            Map<String, Object> r = monitor.call(() -> clock.runSyncRound("join of node " + joined), ROUND_TIMEOUT_MS);
            out.putAll(r);
        } catch (Exception e) {
            out.put("error", "Berkeley round on leader " + leader + " failed: " + rootMessage(e));
        }
        return out;
    }

    // =========================================================================
    // REMOVE
    // =========================================================================

    public Map<String, Object> remove(int id) throws JobDirectory.ApiException {
        lockOrConflict("remove node " + id);
        try {
            Membership current = config.membership();
            if (!current.contains(id)) {
                throw new JobDirectory.ApiException(404, "node " + id + " is not a member (members " + current.ids() + ")");
            }
            if (current.size() - 1 < Membership.MIN_MEMBERS) {
                throw new JobDirectory.ApiException(409, "removing node " + id + " would leave " + (current.size() - 1)
                        + " members; the minimum is " + Membership.MIN_MEMBERS);
            }
            String activeJob = jobDirectory.activeJobId();
            if (activeJob != null) {
                throw new JobDirectory.ApiException(409, "job " + activeJob + " is running; remove a node when no job is running");
            }
            Membership next = current.without(id);
            monitor.pollAll();
            boolean removedLive = false;
            List<Integer> liveAfter = new ArrayList<>();
            for (ClusterMonitor.NodeStatus s : monitor.getSnapshot()) {
                if (!s.isUp()) continue;
                if (s.getId() == id) {
                    removedLive = true;
                } else if (next.contains(s.getId())) {
                    liveAfter.add(s.getId());
                }
            }
            if (removedLive && liveAfter.size() < next.quorum()) {
                throw new JobDirectory.ApiException(409, "removing live node " + id + " would leave " + liveAfter.size()
                        + " live members " + liveAfter + ", fewer than the quorum " + next.quorum() + " of the new "
                        + next.size() + "-member cluster");
            }
            changeInProgress = "removing node " + id;
            boolean wasLeader = Integer.valueOf(id).equals(monitor.getElectionTracker().toMap().get("leaderId"));

            // New epoch to the remaining members first, then stop the process, then forget it.
            config.applyMembership(next);
            Map<String, Object> push = push(next);
            boolean killed = processManager.forget(id);
            monitor.forget(id);
            eventLog.record("NODE_REMOVED", id, "Node " + id + " removed (" + (removedLive ? "was live" : "was down")
                    + (wasLeader ? ", was the leader" : "") + "); process " + (killed ? "stopped" : "not running"),
                    System.currentTimeMillis(), fields("wasLive", removedLive, "wasLeader", wasLeader));
            recordMembershipChanged(next, "removed node " + id);
            monitor.expectElectionActivity(15000);

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("success", true);
            resp.put("node", id);
            resp.put("wasLive", removedLive);
            resp.put("wasLeader", wasLeader);
            resp.put("membership", next.toMap());
            resp.put("pushed", push);
            return resp;
        } finally {
            changeInProgress = null;
            changeLock.unlock();
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private void lockOrConflict(String what) throws JobDirectory.ApiException {
        if (!changeLock.tryLock()) {
            String busy = changeInProgress;
            throw new JobDirectory.ApiException(409, "cannot " + what + ": another membership change is in progress"
                    + (busy == null ? "" : " (" + busy + ")"));
        }
    }

    /** Pushes m to every member that answers; returns {applied, alreadyCurrent, failed}. */
    private Map<String, Object> push(Membership m) {
        List<Integer> applied = new ArrayList<>();
        List<Integer> current = new ArrayList<>();
        List<Integer> failed = new ArrayList<>();
        for (MemberSpec s : m.getMembers().values()) {
            try {
                NodeAgent agent = (NodeAgent) registry(s.getPort()).lookup("agent");
                boolean ok = monitor.call(() -> agent.applyMembership(m), PUSH_TIMEOUT_MS);
                long epoch = monitor.call(agent::getMembershipEpoch);
                if (ok) {
                    applied.add(s.getId());
                } else if (epoch >= m.getEpoch()) {
                    current.add(s.getId());
                } else {
                    failed.add(s.getId());
                }
            } catch (Exception e) {
                failed.add(s.getId());   // down: it gets the membership when it restarts or at its next poll
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("applied", applied);
        out.put("alreadyCurrent", current);
        out.put("notReached", failed);
        return out;
    }

    private void recordMembershipChanged(Membership m, String why) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("epoch", m.getEpoch());
        f.put("members", m.ids());
        f.put("quorum", m.quorum());
        eventLog.record("MEMBERSHIP_CHANGED", 0, "Membership epoch " + m.getEpoch() + ": members " + m.ids()
                + " (quorum " + m.quorum() + "), " + why, System.currentTimeMillis(), f);
    }

    private static Registry registry(int port) throws Exception {
        return LocateRegistry.getRegistry("localhost", port, TimeoutSocketFactory.INSTANCE);
    }

    private static Map<String, Object> fields(Object... kv) {
        Map<String, Object> f = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            f.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return f;
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        String m = c.getMessage();
        return m == null ? c.getClass().getSimpleName() : m;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
