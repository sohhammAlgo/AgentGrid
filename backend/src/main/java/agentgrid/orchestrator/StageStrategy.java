package agentgrid.orchestrator;

import agentgrid.common.Result;
import agentgrid.common.Subtask;

/**
 * The work a node does for one pipeline stage. Nodes look the strategy up by
 * Subtask.Type in a StrategyRegistry, so a later phase can swap in a different
 * implementation (MapReduce SUMMARIZE in Exp 7, parallel-matmul RANK in Exp 10) by
 * registering it; the pipeline, the payload format and the orchestrator do not change.
 *
 * The returned Result carries the output and success flag; the node re-stamps it with its
 * own agent id and Lamport time before sending it back.
 */
public interface StageStrategy {

    Result run(Subtask subtask);
}
