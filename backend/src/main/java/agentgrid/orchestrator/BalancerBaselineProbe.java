package agentgrid.orchestrator;

import agentgrid.clock.LamportClock;
import agentgrid.common.Subtask;
import agentgrid.node.ClusterConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Before/after measurements for fixes G and H, run from its own JVM against the live cluster
 * (start the control plane first; submit no jobs meanwhile). It drives the same JobRunner the
 * leader uses, with the baseline behaviours switched back on:
 *  G: LEAST_LOADED with the submitted LeastLoadedBalancer's raw queue depth, with depth/pool,
 *     and with depth/pool including in-flight dispatches (the orchestrator's setting).
 *  H: subtask timestamps = loop index with Result times ignored (the submitted demo), versus
 *     the Lamport chain. Counts subtasks whose timestamp is not greater than every result of
 *     the stage they depend on.
 *
 * Usage (from backend/): java -cp build/classes agentgrid.orchestrator.BalancerBaselineProbe
 */
public class BalancerBaselineProbe {

    private static final String QUERY = "How do leader election and failure detectors handle a crashed node?";
    private static final int RUNS = 3;

    public static void main(String[] args) throws Exception {
        ClusterConfig config = ClusterConfig.load();
        ClusterView cluster = new ClusterView(config);
        System.out.println("live workers: " + cluster.liveWorkers() + ", simulated work "
                + config.getSimulatedWorkMs() + " ms per subtask");
        System.out.println("query: " + QUERY);

        System.out.println();
        System.out.println("=== G: LEAST_LOADED load view, " + RUNS + " runs each (ROUND_ROBIN for reference) ===");
        measure(cluster, BalancingPolicy.ROUND_ROBIN, WorkerNode.LoadView.RAW, "ROUND_ROBIN");
        measure(cluster, BalancingPolicy.LEAST_LOADED, WorkerNode.LoadView.RAW, "LEAST_LOADED raw depth (submitted)");
        measure(cluster, BalancingPolicy.LEAST_LOADED, WorkerNode.LoadView.NORMALIZED, "LEAST_LOADED depth/pool");
        measure(cluster, BalancingPolicy.LEAST_LOADED, WorkerNode.LoadView.NORMALIZED_IN_FLIGHT,
                "LEAST_LOADED depth/pool + in-flight (fix G)");

        System.out.println();
        System.out.println("=== H: timestamps carried by subtasks ===");
        causality(cluster, true);
        causality(cluster, false);
        System.exit(0);
    }

    private static Job run(ClusterView cluster, BalancingPolicy policy, WorkerNode.LoadView view,
                           boolean loopIndex, String id) {
        LamportClock clock = new LamportClock();
        JobRunner.EventSink sink = new JobRunner.EventSink() {
            @Override
            public long record(String type, String details, Map<String, Object> fields) {
                return clock.tick();
            }

            @Override
            public void receive(long lamport) {
                clock.update(lamport);
            }
        };
        JobRunner runner = new JobRunner(cluster, sink, view, loopIndex);
        Job job = new Job(id, QUERY, policy.name(), 0, System.currentTimeMillis());
        try {
            runner.run(job);
        } finally {
            runner.shutdown();
        }
        return job;
    }

    private static void measure(ClusterView cluster, BalancingPolicy policy, WorkerNode.LoadView view, String label) {
        List<Long> makespans = new ArrayList<>();
        for (int i = 1; i <= RUNS; i++) {
            Job job = run(cluster, policy, view, false, "probe-g-" + i);
            Map<String, Object> m = job.toMap();
            makespans.add(job.makespanMs() == null ? -1 : job.makespanMs());
            System.out.printf("%-46s run %d: %s makespan=%d ms per-node subtasks=%s peak queue=%s%n",
                    label, i, job.getStatus(), job.makespanMs(), m.get("subtasksPerNode"), m.get("peakQueueDepth"));
        }
        Collections.sort(makespans);
        System.out.printf("%-46s median makespan=%d ms%n", label, makespans.get(makespans.size() / 2));
    }

    private static void causality(ClusterView cluster, boolean loopIndex) {
        Job job = run(cluster, BalancingPolicy.WEIGHTED, WorkerNode.LoadView.RAW, loopIndex,
                loopIndex ? "probe-h-before" : "probe-h-after");
        int checked = 0;
        int violations = 0;
        String example = null;
        long previousMax = -1;
        for (Subtask.Type type : TaskGraph.PIPELINE) {
            JobStage stage = job.getGraph().stage(type);
            long stageMax = -1;
            for (JobStage.SubtaskState s : stage.getSubtasks()) {
                if (previousMax >= 0) {
                    checked++;
                    if (s.getDispatchLamport() <= previousMax) {
                        violations++;
                        if (example == null) {
                            example = type + " " + s.getSubtaskId() + " carried ts=" + s.getDispatchLamport()
                                    + " but a " + "result it depends on has ts=" + previousMax;
                        }
                    }
                }
                if (s.getResultLamport() != null) {
                    stageMax = Math.max(stageMax, s.getResultLamport());
                }
            }
            previousMax = stageMax;
        }
        System.out.printf("%-48s %s: %d of %d dependent subtasks carry a timestamp <= a result they depend on%s%n",
                loopIndex ? "loop index as timestamp (submitted demo)" : "Lamport chain (fix H)",
                job.getStatus(), violations, checked, example == null ? "" : "; e.g. " + example);
    }
}
