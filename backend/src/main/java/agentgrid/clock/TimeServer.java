package agentgrid.clock;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Bootstraps one time-synchronization node as its own JVM process.
 *
 * Usage:
 *   java agentgrid.clock.TimeServer <nodeId> <port> <driftMillis>
 *
 * Example:
 *   java agentgrid.clock.TimeServer node-a 1100 3000
 *   java agentgrid.clock.TimeServer node-b 1101 -2000
 *   java agentgrid.clock.TimeServer node-c 1102 500
 */
public class TimeServer {

    private static final String DEFAULT_NODE_ID = "node-a";
    private static final int DEFAULT_PORT = 1100;
    private static final long DEFAULT_DRIFT_MILLIS = 0L;

    public static void main(String[] args) {
        String nodeId = args.length > 0 ? args[0] : DEFAULT_NODE_ID;
        int port;
        long driftMillis;

        try {
            port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;
            driftMillis = args.length > 2 ? Long.parseLong(args[2]) : DEFAULT_DRIFT_MILLIS;
        } catch (NumberFormatException e) {
            System.err.println("Usage: java agentgrid.clock.TimeServer <nodeId> <port> <driftMillis>");
            return;
        }

        try {
            TimeServiceImpl node = new TimeServiceImpl(nodeId, driftMillis);
            Registry registry = LocateRegistry.createRegistry(port);
            registry.rebind(nodeId, node);

            System.out.println(
                    "[" + nodeId + "] time node ready on port " + port
                    + " | simulated drift " + (driftMillis >= 0 ? "+" : "") + driftMillis + " ms"
            );

            System.out.println(
                    "[" + nodeId + "] lookup name: rmi://localhost:" + port + "/" + nodeId
                    + " | initial time " + node.getTime()
            );

        } catch (Exception e) {
            System.err.println("TimeServer failed to start: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
