package agentgrid.rmi;

import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class AgentClient {

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

            Registry registry =
                    LocateRegistry.getRegistry(
                            "localhost",
                            port
                    );

            AgentService agent =
                    (AgentService) registry.lookup(agentId);

            System.out.println(
                    "Connected to remote agent: " +
                    agent.getAgentId()
            );

            System.out.println(
                    "Ping: " +
                    agent.ping()
            );

            Subtask subtask = new Subtask(
                    "task-1",
                    "subtask-1",
                    Subtask.Type.SUMMARIZE,
                    "Distributed systems combine multiple computers to act as one.",
                    1L
            );

            Result result =
                    agent.execute(subtask);

            System.out.println(
                    "Received: " + result
            );

        } catch (Exception e) {

            System.err.println(
                    "AgentClient failed: " +
                    e.getMessage()
            );

            e.printStackTrace();
        }
    }
}