package agentgrid.orchestrator;

import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SYNTHESIZE: merges the partial summaries into one answer. Sentences from all partials
 * are scored against the query, duplicates dropped, and the best ones (default 4) are
 * written in score order, each followed by the id of the document it came from.
 * Payload: query, answerSentences (optional), p0, p1, ... (encoded SUMMARIZE outputs).
 */
public final class SynthesizeStrategy implements StageStrategy {

    private final Corpus corpus;

    public SynthesizeStrategy(Corpus corpus) {
        this.corpus = corpus;
    }

    @Override
    public Result run(Subtask subtask) {
        Payload p = Payload.decode(subtask.getPayload());
        String query = p.get("query", "");
        List<String> terms = Corpus.terms(query);
        int want = Math.max(1, p.getInt("answerSentences", 4));

        List<Object[]> candidates = new ArrayList<>();  // {sentence, docId, score, partialIndex}
        List<String> partials = p.list("p");
        for (int i = 0; i < partials.size(); i++) {
            Payload part = Payload.decode(partials.get(i));
            for (String s : part.list("s")) {
                candidates.add(new Object[] {s, part.get("doc", "?"), corpus.sentenceScore(s, terms), i});
            }
        }
        candidates.sort((a, b) -> {
            int c = Double.compare((Double) b[2], (Double) a[2]);
            return c != 0 ? c : Integer.compare((Integer) a[3], (Integer) b[3]);
        });

        StringBuilder answer = new StringBuilder();
        Set<String> seen = new HashSet<>();
        int used = 0;
        for (Object[] c : candidates) {
            if (used >= want) {
                break;
            }
            String sentence = (String) c[0];
            if (!seen.add(sentence.toLowerCase())) {
                continue;
            }
            if (answer.length() > 0) {
                answer.append(' ');
            }
            answer.append(sentence).append(" [").append(c[1]).append(']');
            used++;
        }
        if (used == 0) {
            return new Result(subtask.getSubtaskId(), "synthesize",
                    "No document in the corpus matches \"" + query + "\".", subtask.getLamportTimestamp(), true);
        }
        return new Result(subtask.getSubtaskId(), "synthesize", answer.toString(), subtask.getLamportTimestamp(), true);
    }
}
