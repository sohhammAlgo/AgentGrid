package agentgrid.orchestrator;

import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.util.ArrayList;
import java.util.List;

/**
 * SUMMARIZE: extractive summary of one document - the sentences that cover the most
 * (idf-weighted) query terms, kept in document order.
 * Payload: query, doc, rank (optional), sentences (default 2).
 * Output: an encoded Payload with doc, title, rank and s0, s1, ... sentences.
 */
public final class SummarizeStrategy implements StageStrategy {

    private final Corpus corpus;

    public SummarizeStrategy(Corpus corpus) {
        this.corpus = corpus;
    }

    @Override
    public Result run(Subtask subtask) {
        Payload p = Payload.decode(subtask.getPayload());
        List<String> terms = Corpus.terms(p.get("query", ""));
        Corpus.Doc d = corpus.get(p.get("doc", ""));
        if (d == null) {
            return new Result(subtask.getSubtaskId(), "summarize",
                    "no document " + p.get("doc", "(none)") + " in the corpus", subtask.getLamportTimestamp(), false);
        }
        int want = Math.max(1, p.getInt("sentences", 2));

        List<String> sentences = d.getSentences();
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < sentences.size(); i++) {
            order.add(i);
        }
        order.sort((a, b) -> {
            int c = Double.compare(corpus.sentenceScore(sentences.get(b), terms),
                    corpus.sentenceScore(sentences.get(a), terms));
            return c != 0 ? c : Integer.compare(a, b);
        });
        List<Integer> picked = new ArrayList<>(order.subList(0, Math.min(want, order.size())));
        picked.sort(Integer::compare);

        Payload out = new Payload().put("doc", d.getId()).put("title", d.getTitle()).put("rank", p.get("rank", "0"));
        for (int i = 0; i < picked.size(); i++) {
            out.put("s" + i, sentences.get(picked.get(i)));
        }
        return new Result(subtask.getSubtaskId(), "summarize", out.encode(), subtask.getLamportTimestamp(), true);
    }
}
