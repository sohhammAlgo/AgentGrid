package agentgrid.rmi;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class AgentServer {

    public static void main(String[] args) {
        String agentId = args.length > 0 ? args[0] : "agent-1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 1099;
        int poolSize = args.length > 2 ? Integer.parseInt(args[2]) : 4;

        try {
            AgentServiceImpl agent = new AgentServiceImpl(agentId, poolSize);
            Registry registry = LocateRegistry.createRegistry(port);
            registry.rebind(agentId, agent);

            System.out.println("[" + agentId + "] bound on port " + port
                    + " with pool size " + poolSize);
        } catch (Exception e) {
            System.err.println("AgentServer failed to start: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
