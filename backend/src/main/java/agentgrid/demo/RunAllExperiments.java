package agentgrid.demo;

import agentgrid.agent.ConcurrencyBenchmarkClient;
import agentgrid.balancer.LoadBalancerDemo;
import agentgrid.clock.BerkeleySyncCoordinator;
import agentgrid.clock.TimeServer;
import agentgrid.election.ElectionDemo;
import agentgrid.replication.ReplicationDemo;
import agentgrid.rmi.AgentClient;
import agentgrid.rmi.AgentServer;

/**
 * Runs Experiments 1 through 6 back-to-back in a single JVM.
 *
 * Why this works without separate terminals:
 * AgentServer / TimeServer bind an RMI registry and return immediately;
 * they do not block. The registry keeps listening on background RMI
 * threads for the rest of the process. So we can call each *Server.main()
 * to stand up a node, then call the matching *Client.main() / *Demo.main()
 * right after, all inside this one main() method, exactly as if each had
 * been started in its own terminal.
 *
 * Usage:
 *   java -cp build/classes agentgrid.demo.RunAllExperiments
 *
 * Every experiment's real output (the same text you'd see running each
 * piece separately) prints between the ">>> EXPERIMENT N" banners below.
 */
public class RunAllExperiments {

    public static void main(String[] args) throws Exception {

        banner("EXPERIMENT 1 - RMI (single remote call)");
        AgentServer.main(new String[] { "agent-1", "1099", "4" });
        sleep(500);
        AgentClient.main(new String[] { "agent-1", "1099" });

        banner("EXPERIMENT 2 - Multithreading (serial vs. thread-pooled)");
        ConcurrencyBenchmarkClient.main(new String[] { "agent-1", "1099", "8" });
        System.out.println(
                "\n(Lamport timestamps for Experiment 3A are visible in the\n"
                + "'executing Subtask{...} | lamport=' lines printed above, on\n"
                + "agent-1's own console output from Experiments 1 and 2.)");

        banner("EXPERIMENT 3B - Berkeley physical clock synchronization");
        TimeServer.main(new String[] { "node-a", "1100", "3000" });
        TimeServer.main(new String[] { "node-b", "1101", "-2000" });
        TimeServer.main(new String[] { "node-c", "1102", "500" });
        sleep(500);
        BerkeleySyncCoordinator.main(new String[] { "node-a:1100", "node-b:1101", "node-c:1102" });

        banner("EXPERIMENT 4 - Leader election (Bully vs. Ring)");
        ElectionDemo.main(new String[0]);

        banner("EXPERIMENT 5 - Replication & consistency (Strong vs. Eventual)");
        ReplicationDemo.main(new String[0]);

        banner("EXPERIMENT 6 - Load balancing (RoundRobin / Weighted / LeastLoaded)");
        AgentServer.main(new String[] { "agent-weak", "1201", "2" });
        AgentServer.main(new String[] { "agent-medium", "1202", "4" });
        AgentServer.main(new String[] { "agent-strong", "1203", "6" });
        sleep(500);
        LoadBalancerDemo.main(new String[0]);

        System.out.println("Exiting (background RMI threads are still up; forcing exit).");
        System.exit(0);
    }

    private static void banner(String title) {
        System.out.println();
        System.out.println("=".repeat(70));
        System.out.println(">>> " + title);
        System.out.println("=".repeat(70));
    }

    private static void sleep(long ms) throws InterruptedException {
        Thread.sleep(ms);
    }
}
