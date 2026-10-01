package agentgrid.node;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Heartbeat failure detector of one node.
 *
 * Every PING_INTERVAL_MS the node pings the leader it currently recognises. A ping
 * counts as a miss when the call fails, or when the pinged node no longer names itself
 * leader. MISS_THRESHOLD consecutive misses emit LEADER_LOST (through the lifecycle
 * registry) and start an election.
 *
 * Boot gate: a freshly started node starts its first election only after its first
 * sync() call from the control plane, or BOOT_GATE_MS after boot, whichever comes first.
 * Afterwards, a node left without a leader and without a running election for
 * NO_LEADER_RETRY_MS starts one again.
 */
public final class FailureDetector {

    public static final long PING_INTERVAL_MS = 500L;
    public static final int MISS_THRESHOLD = 3;
    public static final long BOOT_GATE_MS = 3000L;
    public static final long NO_LEADER_RETRY_MS = 3000L;

    private final ElectionNode node;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(ElectionNode.daemon("failure-detector-"));
    private final long bootMs = System.currentTimeMillis();
    private final AtomicBoolean gateOpen = new AtomicBoolean(false);
    private final AtomicBoolean held = new AtomicBoolean(false);

    private int watchedLeader = -1;
    private int misses = 0;
    private long noLeaderSinceMs = -1;

    public FailureDetector(ElectionNode node) {
        this.node = node;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::tick, PING_INTERVAL_MS, PING_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /** Called on every sync() from the control plane; the first one opens the boot gate. */
    public void onSync() {
        if (held.get()) {
            return;   // a joining node waits for the membership that includes it
        }
        openGate("first sync() from control plane");
    }

    /**
     * For a node that is joining the cluster: its first election must wait until the
     * membership that includes it has been applied, otherwise it would elect itself among
     * members that do not know it yet. Neither sync() nor the BOOT_GATE_MS fallback opens the
     * gate until releaseBootGate().
     */
    public void holdBootGate() {
        held.set(true);
    }

    public void releaseBootGate(String why) {
        if (held.compareAndSet(true, false)) {
            openGate(why);
        }
    }

    private void openGate(String why) {
        if (gateOpen.compareAndSet(false, true)) {
            System.out.println("[Node " + node.id() + "] boot gate open: " + why);
            node.exec().submit(() -> node.start("boot (" + why + ")"));
        }
    }

    private void tick() {
        try {
            if (!gateOpen.get()) {
                if (!held.get() && System.currentTimeMillis() - bootMs >= BOOT_GATE_MS) {
                    openGate(BOOT_GATE_MS + " ms since boot without sync()");
                }
                return;
            }
            if (!node.alive()) {
                return;
            }
            int leader = node.leader();
            if (leader != watchedLeader) {
                watchedLeader = leader;
                misses = 0;
            }
            if (leader < 0) {
                checkNoLeader();
                return;
            }
            noLeaderSinceMs = -1;
            if (leader == node.id()) {
                return;
            }

            Integer view = node.ping(leader);
            if (view != null && view == leader) {
                misses = 0;
                return;
            }
            if (node.leader() != leader) {
                return;  // view changed while pinging
            }
            misses++;
            Map<String, Object> f = new LinkedHashMap<>();
            f.put("leader", leader);
            f.put("misses", misses);
            node.events().record("HEARTBEAT_MISS", "no heartbeat from leader " + leader
                    + (view == null ? " (unreachable)" : " (it reports leader " + view + ")")
                    + ", miss " + misses + "/" + MISS_THRESHOLD, f);
            if (misses >= MISS_THRESHOLD) {
                misses = 0;
                if (node.clearLeaderIf(leader)) {
                    watchedLeader = -1;
                    node.lifecycle().onLeaderLost(node.id(), leader);
                    node.start("leader " + leader + " lost after " + MISS_THRESHOLD + " missed heartbeats");
                }
            }
        } catch (RuntimeException e) {
            System.err.println("[Node " + node.id() + "] failure detector error: " + e);
        }
    }

    private void checkNoLeader() {
        if (node.isElecting()) {
            noLeaderSinceMs = -1;
            return;
        }
        long now = System.currentTimeMillis();
        if (noLeaderSinceMs < 0) {
            noLeaderSinceMs = now;
        } else if (now - noLeaderSinceMs >= NO_LEADER_RETRY_MS) {
            noLeaderSinceMs = -1;
            node.start("no leader and no election for " + NO_LEADER_RETRY_MS + " ms");
        }
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }
}
