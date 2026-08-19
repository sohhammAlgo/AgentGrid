
package agentgrid.agent;

import agentgrid.common.Result;
import agentgrid.common.Subtask;
import agentgrid.rmi.AgentService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.util.ArrayList;
import java.util.List;

/**
 * Experiment 2 demonstration:
 *
 * 1. Execute subtasks one at a time using execute()
 * 2. Execute the same subtasks using executeBatch()
 * 3. Compare execution times
 * 4. Calculate speedup
 */
public class ConcurrencyBenchmarkClient {

    public static void main(String[] args)
            throws Exception {

        String agentId =
                args.length > 0
                        ? args[0]
                        : "agent-1";

        int port =
                args.length > 1
                        ? Integer.parseInt(args[1])
                        : 1099;

        int n =
                args.length > 2
                        ? Integer.parseInt(args[2])
                        : 8;

        /*
         * Connect to RMI registry.
         */
        Registry registry =
                LocateRegistry.getRegistry(
                        "localhost",
                        port
                );

        /*
         * Look up remote agent.
         */
        AgentService agent =
                (AgentService) registry.lookup(agentId);

        /*
         * Create N subtasks.
         */
        List<Subtask> batch =
                buildBatch(n);

        /*
         * ============================
         * SERIAL EXECUTION
         * ============================
         */

        long serialStart =
                System.currentTimeMillis();

        for (Subtask s : batch) {

            agent.execute(s);
        }

        long serialMs =
                System.currentTimeMillis()
                - serialStart;

        /*
         * ============================
         * POOLED EXECUTION
         * ============================
         */

        long pooledStart =
                System.currentTimeMillis();

        List<Result> results =
                agent.executeBatch(batch);

        long pooledMs =
                System.currentTimeMillis()
                - pooledStart;

        /*
         * ============================
         * OUTPUT
         * ============================
         */

        System.out.println(
                "Subtasks:             " + n
        );

        System.out.println(
                "Serial execution:     "
                + serialMs
                + " ms"
        );

        System.out.println(
                "Thread-pooled batch:  "
                + pooledMs
                + " ms"
        );

        System.out.printf(
                "Speedup:              %.2fx%n",
                (double) serialMs / pooledMs
        );

        System.out.println(
                "Sample result:        "
                + results.get(0)
        );
    }

    private static List<Subtask> buildBatch(
            int n) {

        List<Subtask> batch =
                new ArrayList<>();

        for (int i = 0; i < n; i++) {

            batch.add(
                    new Subtask(
                            "task-1",
                            "subtask-" + i,
                            Subtask.Type.SUMMARIZE,
                            "chunk-" + i,
                            i
                    )
            );
        }

        return batch;
    }
}