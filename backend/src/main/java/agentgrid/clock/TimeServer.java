package agentgrid.clock;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Bootstraps one time-synchronization node as its own JVM process.
 *
 * <p><b>Why this exists in AgentGrid-Lite.</b> The project runs on a single laptop
 * with no cloud and no paid services, so "a distributed grid" is simulated by
 * starting several of these processes on different ports. Each one owns its own
 * {@link TimeServiceImpl} with its own artificial drift, which is what makes
 * {@link BerkeleySyncCoordinator} a real cross-process synchronization rather than
 * an in-memory simulation: corrections genuinely travel over RMI to a separate JVM
 * that genuinely holds a divergent clock.
 *
 * <p>Mirrors the pattern already used by {@code rmi.AgentServer} — create a
 * registry on the given port and rebind under the node id — so a node can host an
 * agent and a clock on separate ports without the two bootstraps interfering.
 *
 * <p>Usage:
 * <pre>
 *   java agentgrid.clock.TimeServer &lt;nodeId&gt; &lt;port&gt; &lt;driftMillis&gt;
 * </pre>
 * For example, three nodes skewed by +3s, -2s and +0.5s:
 * <pre>
 *   java agentgrid.clock.TimeServer node-a 1100 3000
 *   java agentgrid.clock.TimeServer node-b 1101 -2000
 *   java agentgrid.clock.TimeServer node-c 1102 500
 * </pre>
 */
public class TimeServer {

    private static final String DEFAULT_NODE_ID = "node-a";

    private static final int DEFAULT_PORT = 1100;

    private static final long DEFAULT_DRIFT_MILLIS = 0L;

    public static void main(String[] args) {

        String nodeId =
                args.length > 0
                        ? args[0]
                        : DEFAULT_NODE_ID;

        int port;

        long driftMillis;

        try {

            port =
                    args.length > 1
                            ? Integer.parseInt(args[1])
                            : DEFAULT_PORT;

            driftMillis =
                    args.length > 2
                            ? Long.parseLong(args[2])
                            : DEFAULT_DRIFT_MILLIS;

        } catch (NumberFormatException e) {

            System.err.println(
                    "Usage: java agentgrid.clock.TimeServer "
                    + "<nodeId> <port> <driftMillis>"
            );

            return;
        }

        try {

            TimeServiceImpl node =
                    new TimeServiceImpl(nodeId, driftMillis);

            Registry registry =
                    LocateRegistry.createRegistry(port);

            registry.rebind(nodeId, node);

            System.out.println(
                    "[" + nodeId
                    + "] time node ready on port "
                    + port
                    + " | simulated drift "
                    + (driftMillis >= 0 ? "+" : "")
                    + driftMillis
                    + " ms"
            );

            System.out.println(
                    "[" + nodeId
                    + "] lookup name: rmi://localhost:"
                    + port
                    + "/"
                    + nodeId
                    + " | initial time "
                    + node.getTime()
            );

        } catch (Exception e) {

            System.err.println(
                    "TimeServer failed to start: "
                    + e.getMessage()
            );

            e.printStackTrace();
        }
    }
}
