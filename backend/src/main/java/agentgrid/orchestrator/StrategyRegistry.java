package agentgrid.orchestrator;

import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.util.EnumMap;
import java.util.Map;

/**
 * Maps each Subtask.Type to the StageStrategy a node runs for it. Later phases replace a
 * stage by calling register() (e.g. a MapReduce SUMMARIZE or a parallel-matmul RANK);
 * nothing else in the pipeline changes.
 */
public final class StrategyRegistry {

    private final Map<Subtask.Type, StageStrategy> strategies = new EnumMap<>(Subtask.Type.class);

    /** The local implementations over the bundled corpus. */
    public static StrategyRegistry defaults(Corpus corpus) {
        StrategyRegistry r = new StrategyRegistry();
        r.register(Subtask.Type.RETRIEVE, new RetrieveStrategy(corpus));
        r.register(Subtask.Type.RANK, new RankStrategy(corpus));
        r.register(Subtask.Type.SUMMARIZE, new SummarizeStrategy(corpus));
        r.register(Subtask.Type.SYNTHESIZE, new SynthesizeStrategy(corpus));
        return r;
    }

    public synchronized StrategyRegistry register(Subtask.Type type, StageStrategy strategy) {
        strategies.put(type, strategy);
        return this;
    }

    public synchronized StageStrategy get(Subtask.Type type) {
        return strategies.get(type);
    }

    /** Runs the registered strategy; a missing strategy or an exception yields a failed Result. */
    public Result run(Subtask subtask) {
        StageStrategy s = get(subtask.getType());
        if (s == null) {
            return new Result(subtask.getSubtaskId(), "registry",
                    "no strategy registered for " + subtask.getType(), subtask.getLamportTimestamp(), false);
        }
        try {
            return s.run(subtask);
        } catch (RuntimeException e) {
            return new Result(subtask.getSubtaskId(), "registry",
                    subtask.getType() + " failed: " + e, subtask.getLamportTimestamp(), false);
        }
    }
}
