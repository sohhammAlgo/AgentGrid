package agentgrid.election;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bootstraps an Election Node process on a distinct port for RMI discovery.
 * Usage:
 *   java agentgrid.election.ElectionServer <nodeId> <port> <algorithm: bully|ring>
 */
public class ElectionServer {

    private static final int DEFAULT_NODE_ID = 1;
    private static final int DEFAULT_PORT = 1301;
    private static final String DEFAULT_ALGO = "bully";

    public static void main(String[] args) {
        int nodeId = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_NODE_ID;
        int port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;
        String algo = args.length > 2 ? args[2].toLowerCase() : DEFAULT_ALGO;

        // Default topology for standalone cluster bootstrapping
        Map<Integer, Integer> defaultNodePortMap = new HashMap<>();
        defaultNodePortMap.put(1, 1301);
        defaultNodePortMap.put(2, 1302);
        defaultNodePortMap.put(3, 1303);
        defaultNodePortMap.put(4, 1304);
        defaultNodePortMap.put(5, 1305);

        List<Integer> defaultRingOrder = new ArrayList<>(defaultNodePortMap.keySet());

        try {
            ElectionNodeService node;
            if ("ring".equals(algo)) {
                node = new RingElectionNode(nodeId, port, defaultRingOrder, defaultNodePortMap, null);
            } else {
                node = new BullyElectionNode(nodeId, port, defaultNodePortMap, null);
            }

            Registry registry = LocateRegistry.createRegistry(port);
            registry.rebind("election-node-" + nodeId, node);

            System.out.println("[Election Node " + nodeId + "] Ready on port " + port + " (" + algo.toUpperCase() + " mode)");

        } catch (Exception e) {
            System.err.println("ElectionServer failed to start: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
