package agentgrid.clock;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.util.ArrayList;
import java.util.List;

/**
 * Berkeley clock-synchronization coordinator for the AgentGrid-Lite node set.
 *
 * <p><b>Why this exists in AgentGrid-Lite.</b> The grid has no external time
 * authority — the project is offline and zero-cost, so there is no NTP peer to
 * discipline anyone's clock against. Berkeley's algorithm is built for exactly that
 * situation: rather than pulling everyone toward an authoritative "true" time, it
 * polls the group, computes the average, and tells each node how far to move to
 * meet the others. Absolute correctness is sacrificed; mutual agreement is bought.
 *
 * <p>Mutual agreement is all this system needs. Experiment 5's blackboard resolves
 * competing agent findings by comparing the timestamps on writes made from
 * different nodes, and Experiment 8's task-graph mirroring decides which replica is
 * fresher the same way. Both break when clocks disagree by seconds and both work
 * fine when they agree within milliseconds, regardless of whether the shared value
 * matches an atomic clock. See {@link TimeService} for the longer argument.
 *
 * <p><b>Why polls are round-trip compensated.</b> The naive formulation reads every
 * node's clock, averages the raw readings, and sends each node
 * {@code average - nodeTime}. That is wrong in a subtle way: the readings are taken
 * at <i>different</i> instants, so a node polled 5 ms later reports a time 5 ms
 * larger for no reason but poll order, and the error lands in the average and in
 * every correction. This coordinator instead converts each reading into that node's
 * offset relative to the coordinator's own clock,
 * {@code offset = nodeTime - (t0 + t1) / 2}, which cancels both poll ordering and
 * roughly half the RMI round-trip. Comparing all nodes at one common instant makes
 * {@code correction = averageOffset - nodeOffset} — the same formula, evaluated
 * where it is actually meaningful. Without this the residual spread is dominated by
 * RMI call latency rather than by clock error.
 *
 * <p>Usage:
 * <pre>
 *   java agentgrid.clock.BerkeleySyncCoordinator node-a:1100 node-b:1101 node-c:1102
 * </pre>
 */
public class BerkeleySyncCoordinator {

    public static void main(String[] args) {

        if (args.length == 0) {

            System.err.println(
                    "Usage: java agentgrid.clock.BerkeleySyncCoordinator "
                    + "<nodeId:port> [<nodeId:port> ...]"
            );

            return;
        }

        try {

            List<NodeHandle> nodes =
                    connectAll(args);

            System.out.println(
                    "Berkeley sync across "
                    + nodes.size()
                    + " node(s)"
            );

            /*
             * ============================
             * BEFORE
             * ============================
             */

            List<Reading> before =
                    pollAll(nodes);

            long averageOffsetBefore =
                    averageOffset(before);

            report(
                    "BEFORE SYNC",
                    before,
                    averageOffsetBefore
            );

            long spreadBefore =
                    spread(before);

            /*
             * ============================
             * CORRECT
             * ============================
             */

            System.out.println();
            System.out.println("--- corrections sent ---");

            for (Reading reading : before) {

                long correction =
                        averageOffsetBefore - reading.offsetMillis;

                reading.node.service.adjustTime(correction);

                System.out.println(
                        pad(reading.node.nodeId)
                        + "  correction "
                        + signed(correction)
                        + " ms"
                );
            }

            /*
             * ============================
             * AFTER
             * ============================
             */

            List<Reading> after =
                    pollAll(nodes);

            long averageOffsetAfter =
                    averageOffset(after);

            report(
                    "AFTER SYNC",
                    after,
                    averageOffsetAfter
            );

            long spreadAfter =
                    spread(after);

            /*
             * ============================
             * SUMMARY
             * ============================
             */

            System.out.println();
            System.out.println("--- summary ---");

            System.out.println(
                    "Max clock spread before: "
                    + spreadBefore
                    + " ms"
            );

            System.out.println(
                    "Max clock spread after:  "
                    + spreadAfter
                    + " ms"
            );

            System.out.println(
                    "Spread reduced by:       "
                    + (spreadBefore - spreadAfter)
                    + " ms"
            );

        } catch (Exception e) {

            System.err.println(
                    "Berkeley sync failed: "
                    + e.getMessage()
            );

            e.printStackTrace();
        }
    }

    /**
     * Resolves each {@code nodeId:port} argument to a live remote clock.
     *
     * @param specs command-line node specifications
     * @return connected node handles, in argument order
     * @throws Exception if any node cannot be reached
     */
    private static List<NodeHandle> connectAll(String[] specs)
            throws Exception {

        List<NodeHandle> nodes =
                new ArrayList<>();

        for (String spec : specs) {

            int separator = spec.lastIndexOf(':');

            if (separator < 1) {

                throw new IllegalArgumentException(
                        "Expected <nodeId:port>, got '" + spec + "'"
                );
            }

            String nodeId =
                    spec.substring(0, separator);

            int port =
                    Integer.parseInt(
                            spec.substring(separator + 1)
                    );

            Registry registry =
                    LocateRegistry.getRegistry("localhost", port);

            TimeService service =
                    (TimeService) registry.lookup(nodeId);

            nodes.add(
                    new NodeHandle(
                            service.getNodeId(),
                            service
                    )
            );
        }

        return nodes;
    }

    /**
     * Reads every node's clock, converting each raw reading into an offset
     * relative to the coordinator's clock so all nodes become comparable at one
     * instant.
     *
     * @param nodes connected nodes
     * @return one reading per node, in the same order
     * @throws Exception if a node becomes unreachable mid-poll
     */
    private static List<Reading> pollAll(List<NodeHandle> nodes)
            throws Exception {

        List<Reading> readings =
                new ArrayList<>();

        for (NodeHandle node : nodes) {

            long t0 = System.currentTimeMillis();

            long nodeTime = node.service.getTime();

            long t1 = System.currentTimeMillis();

            long roundTrip = t1 - t0;

            /*
             * Best estimate of the coordinator's clock at the instant the node
             * sampled its own: midpoint of the round trip.
             */
            long coordinatorMid = t0 + (roundTrip / 2);

            readings.add(
                    new Reading(
                            node,
                            nodeTime,
                            nodeTime - coordinatorMid,
                            roundTrip
                    )
            );
        }

        return readings;
    }

    /**
     * Berkeley's averaging step, computed over round-trip-compensated offsets
     * rather than raw readings.
     */
    private static long averageOffset(List<Reading> readings) {

        long total = 0L;

        for (Reading reading : readings) {

            total += reading.offsetMillis;
        }

        return total / readings.size();
    }

    /**
     * Widest disagreement between any two nodes, evaluated at a common instant.
     * This is the headline number for the lab report.
     */
    private static long spread(List<Reading> readings) {

        long min = Long.MAX_VALUE;

        long max = Long.MIN_VALUE;

        for (Reading reading : readings) {

            min = Math.min(min, reading.offsetMillis);

            max = Math.max(max, reading.offsetMillis);
        }

        return max - min;
    }

    /**
     * Prints one poll round: each node's own time, its offset from the group
     * average, and the round-trip cost of reading it.
     */
    private static void report(
            String heading,
            List<Reading> readings,
            long averageOffset) {

        System.out.println();
        System.out.println("--- " + heading + " ---");

        System.out.println(
                pad("node")
                + "  " + padTime("reported time")
                + "  " + pad10("vs average")
                + "  rtt"
        );

        for (Reading reading : readings) {

            long relative =
                    reading.offsetMillis - averageOffset;

            System.out.println(
                    pad(reading.node.nodeId)
                    + "  " + padTime(String.valueOf(reading.rawTimeMillis))
                    + "  " + pad10(signed(relative) + " ms")
                    + "  " + reading.roundTripMillis + " ms"
            );
        }
    }

    private static String signed(long millis) {

        return (millis >= 0 ? "+" : "") + millis;
    }

    private static String pad(String text) {

        return String.format("%-10s", text);
    }

    private static String pad10(String text) {

        return String.format("%-12s", text);
    }

    private static String padTime(String text) {

        return String.format("%-15s", text);
    }

    /**
     * A connected remote clock.
     */
    private static final class NodeHandle {

        private final String nodeId;

        private final TimeService service;

        private NodeHandle(
                String nodeId,
                TimeService service) {

            this.nodeId = nodeId;
            this.service = service;
        }
    }

    /**
     * One round-trip-compensated sample of a node's clock.
     */
    private static final class Reading {

        private final NodeHandle node;

        /** Raw value the node reported. */
        private final long rawTimeMillis;

        /** Node clock minus coordinator clock, round-trip compensated. */
        private final long offsetMillis;

        private final long roundTripMillis;

        private Reading(
                NodeHandle node,
                long rawTimeMillis,
                long offsetMillis,
                long roundTripMillis) {

            this.node = node;
            this.rawTimeMillis = rawTimeMillis;
            this.offsetMillis = offsetMillis;
            this.roundTripMillis = roundTripMillis;
        }
    }
}
