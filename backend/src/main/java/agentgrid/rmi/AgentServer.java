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
<<<<<<< HEAD
                    new AgentServiceImpl(agentId);
=======
                    new AgentServiceImpl(agentId, 4);
>>>>>>> bf3b5a39342d188269adf8991709e660f7025aee

            Registry registry =
                    LocateRegistry.createRegistry(port);

            registry.rebind(agentId, agent);

            System.out.println(
<<<<<<< HEAD
                    "[" + agentId +
                    "] bound and ready on port " +
                    port
            );

            System.out.println(
                    "[" + agentId +
                    "] lookup name: rmi://localhost:" +
                    port + "/" + agentId
=======
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
>>>>>>> bf3b5a39342d188269adf8991709e660f7025aee
            );

        } catch (Exception e) {

            System.err.println(
<<<<<<< HEAD
                    "AgentServer failed to start: " +
                    e.getMessage()
=======
                    "AgentServer failed to start: "
                    + e.getMessage()
>>>>>>> bf3b5a39342d188269adf8991709e660f7025aee
            );

            e.printStackTrace();
        }
    }
}