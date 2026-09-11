package agentgrid.clock;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.util.ArrayList;
import java.util.List;

/**
 * Berkeley clock-synchronization coordinator for the AgentGrid-Lite node set.
 *
 * Why this exists in AgentGrid-Lite:
 * The grid has no external time authority. Berkeley's algorithm polls the group,
 * computes the average offset, and sends signed corrections to each node.
 *
 * Why polls are round-trip compensated:
 * Readings are taken sequentially, so network RMI round-trip time would bias raw
 * readings. Converting readings to coordinator offsets and estimating the midpoint
 * cancels round-trip latency bias.
 *
 * Usage:
 *   java agentgrid.clock.BerkeleySyncCoordinator node-a:1100 node-b:1101 node-c:1102
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
            List<NodeHandle> nodes = connectAll(args);
            System.out.println("Berkeley sync across " + nodes.size() + " node(s)");

            // BEFORE SYNC
            List<Reading> before = pollAll(nodes);
            long averageOffsetBefore = averageOffset(before);
            report("BEFORE SYNC", before, averageOffsetBefore);
            long spreadBefore = spread(before);

            // APPLY CORRECTIONS
            System.out.println();
            System.out.println("--- corrections sent ---");

            for (Reading reading : before) {
                long correction = averageOffsetBefore - reading.offsetMillis;
                reading.node.service.adjustTime(correction);
                System.out.println(pad(reading.node.nodeId) + "  correction " + signed(correction) + " ms");
            }

            // AFTER SYNC
            List<Reading> after = pollAll(nodes);
            long averageOffsetAfter = averageOffset(after);
            report("AFTER SYNC", after, averageOffsetAfter);
            long spreadAfter = spread(after);

            // SUMMARY
            System.out.println();
            System.out.println("--- summary ---");
            System.out.println("Max clock spread before: " + spreadBefore + " ms");
            System.out.println("Max clock spread after:  " + spreadAfter + " ms");
            System.out.println("Spread reduced by:       " + (spreadBefore - spreadAfter) + " ms");

        } catch (Exception e) {
            System.err.println("Berkeley sync failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static List<NodeHandle> connectAll(String[] specs) throws Exception {
        List<NodeHandle> nodes = new ArrayList<>();

        for (String spec : specs) {
            int separator = spec.lastIndexOf(':');
            if (separator < 1) {
                throw new IllegalArgumentException("Expected <nodeId:port>, got '" + spec + "'");
            }

            String nodeId = spec.substring(0, separator);
            int port = Integer.parseInt(spec.substring(separator + 1));

            Registry registry = LocateRegistry.getRegistry("localhost", port);
            TimeService service = (TimeService) registry.lookup(nodeId);

            nodes.add(new NodeHandle(service.getNodeId(), service));
        }

        return nodes;
    }

    private static List<Reading> pollAll(List<NodeHandle> nodes) throws Exception {
        List<Reading> readings = new ArrayList<>();

        for (NodeHandle node : nodes) {
            long t0 = System.currentTimeMillis();
            long nodeTime = node.service.getTime();
            long t1 = System.currentTimeMillis();
            long roundTrip = t1 - t0;
            long coordinatorMid = t0 + (roundTrip / 2);

            readings.add(new Reading(node, nodeTime, nodeTime - coordinatorMid, roundTrip));
        }

        return readings;
    }

    private static long averageOffset(List<Reading> readings) {
        long total = 0L;
        for (Reading reading : readings) {
            total += reading.offsetMillis;
        }
        return total / readings.size();
    }

    private static long spread(List<Reading> readings) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        for (Reading reading : readings) {
            min = Math.min(min, reading.offsetMillis);
            max = Math.max(max, reading.offsetMillis);
        }

        return max - min;
    }

    private static void report(String heading, List<Reading> readings, long averageOffset) {
        System.out.println();
        System.out.println("--- " + heading + " ---");
        System.out.println(
                pad("node")
                + "  " + padTime("reported time")
                + "  " + pad10("vs average")
                + "  rtt"
        );

        for (Reading reading : readings) {
            long relative = reading.offsetMillis - averageOffset;
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

    private static final class NodeHandle {
        private final String nodeId;
        private final TimeService service;

        private NodeHandle(String nodeId, TimeService service) {
            this.nodeId = nodeId;
            this.service = service;
        }
    }

    private static final class Reading {
        private final NodeHandle node;
        private final long rawTimeMillis;
        private final long offsetMillis;
        private final long roundTripMillis;

        private Reading(NodeHandle node, long rawTimeMillis, long offsetMillis, long roundTripMillis) {
            this.node = node;
            this.rawTimeMillis = rawTimeMillis;
            this.offsetMillis = offsetMillis;
            this.roundTripMillis = roundTripMillis;
        }
    }
}
