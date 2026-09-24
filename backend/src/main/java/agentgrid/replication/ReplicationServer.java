package agentgrid.replication;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.util.HashMap;
import java.util.Map;

/**
 * Bootstraps one Replicated Blackboard Node process on a distinct RMI port.
 * Usage:
 *   java agentgrid.replication.ReplicationServer <nodeId> <port>
 */
public class ReplicationServer {

    private static final String DEFAULT_NODE_ID = "node-1";
    private static final int DEFAULT_PORT = 1401;

    public static void main(String[] args) {
        String nodeId = args.length > 0 ? args[0] : DEFAULT_NODE_ID;
        int port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;

        Map<String, Integer> defaultPeerMap = new HashMap<>();
        defaultPeerMap.put("node-1", 1401);
        defaultPeerMap.put("node-2", 1402);
        defaultPeerMap.put("node-3", 1403);

        try {
            ReplicatedBlackboardNode node = new ReplicatedBlackboardNode(nodeId, port, defaultPeerMap);
            Registry registry = LocateRegistry.createRegistry(port);
            registry.rebind("blackboard-node-" + nodeId, node);

            System.out.println("[" + nodeId + "] Replicated Blackboard Node ready on port " + port);

        } catch (Exception e) {
            System.err.println("ReplicationServer failed to start: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
