package agentgrid.orchestrator;

import agentgrid.common.Subtask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The ordered stages of a job: RETRIEVE -> RANK -> SUMMARIZE -> SYNTHESIZE.
 * A stage's subtasks are created when the stage starts, from the previous stage's outputs.
 */
public class TaskGraph implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final List<Subtask.Type> PIPELINE = List.of(
            Subtask.Type.RETRIEVE, Subtask.Type.RANK, Subtask.Type.SUMMARIZE, Subtask.Type.SYNTHESIZE);

    private final ArrayList<JobStage> stages = new ArrayList<>();

    public TaskGraph() {
        for (Subtask.Type t : PIPELINE) {
            stages.add(new JobStage(t));
        }
    }

    public List<JobStage> getStages() {
        return stages;
    }

    public JobStage stage(Subtask.Type type) {
        for (JobStage s : stages) {
            if (s.getType() == type) {
                return s;
            }
        }
        throw new IllegalArgumentException("no stage " + type);
    }

    public int subtaskCount() {
        return stages.stream().mapToInt(s -> s.getSubtasks().size()).sum();
    }

    public long completedCount() {
        return stages.stream().mapToLong(JobStage::completedCount).sum();
    }

    List<Map<String, Object>> toList() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (JobStage s : stages) {
            list.add(s.toMap());
        }
        return list;
    }
}
