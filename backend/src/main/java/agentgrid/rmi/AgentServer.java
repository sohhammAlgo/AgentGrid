package agentgrid.rmi;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class AgentServer {

    public static void main(String[] args) {

        String agentId =
                args.length > 0
                        ? args[0]
                        : "agent-1";

        int port =
                args.length > 1
                        ? Integer.parseInt(args[1])
                        : 1099;

        try {

            AgentServiceImpl agent =
                    new AgentServiceImpl(agentId);

            Registry registry =
                    LocateRegistry.createRegistry(port);

            registry.rebind(agentId, agent);

            /*
             * Release the worker thread pool on Ctrl-C / JVM shutdown.
             */
            Runtime.getRuntime().addShutdownHook(
                    new Thread(agent::shutdown)
            );

            System.out.println(
                    "[" + agentId
                    + "] bound and ready on port "
                    + port
            );

            System.out.println(
                    "[" + agentId
                    + "] lookup name: rmi://localhost:"
                    + port
                    + "/"
                    + agentId
            );

        } catch (Exception e) {

            System.err.println(
                    "AgentServer failed to start: "
                    + e.getMessage()
            );

            e.printStackTrace();
        }
    }
}
