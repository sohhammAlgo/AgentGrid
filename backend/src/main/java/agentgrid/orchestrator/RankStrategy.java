package agentgrid.orchestrator;

import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * RANK: TF-IDF score of each candidate document against the query.
 * Payload: query, candidates (comma separated ids), topK. Output: one line per document,
 * "docId TAB score TAB title", highest score first, at most topK lines.
 * score(d) = sum over query terms t of (count(t, d) / length(d)) * idf(t).
 */
public final class RankStrategy implements StageStrategy {

    private final Corpus corpus;

    public RankStrategy(Corpus corpus) {
        this.corpus = corpus;
    }

    @Override
    public Result run(Subtask subtask) {
        Payload p = Payload.decode(subtask.getPayload());
        List<String> terms = Corpus.terms(p.get("query", ""));
        int topK = Math.max(1, p.getInt("topK", 10));
        String candidates = p.get("candidates", "");

        List<Object[]> scored = new ArrayList<>();
        for (String id : candidates.split(",")) {
            Corpus.Doc d = corpus.get(id.trim());
            if (d == null) {
                continue;
            }
            double score = 0;
            for (String t : terms) {
                score += ((double) d.count(t) / d.length()) * corpus.idf(t);
            }
            scored.add(new Object[] {d, score});
        }
        scored.sort((a, b) -> {
            int c = Double.compare((Double) b[1], (Double) a[1]);
            return c != 0 ? c : ((Corpus.Doc) a[0]).getId().compareTo(((Corpus.Doc) b[0]).getId());
        });

        StringBuilder out = new StringBuilder();
        for (int i = 0; i < Math.min(topK, scored.size()); i++) {
            Corpus.Doc d = (Corpus.Doc) scored.get(i)[0];
            if (out.length() > 0) {
                out.append('\n');
            }
            out.append(d.getId()).append('\t')
                    .append(String.format(Locale.ROOT, "%.4f", (Double) scored.get(i)[1])).append('\t')
                    .append(d.getTitle());
        }
        return new Result(subtask.getSubtaskId(), "rank", out.toString(), subtask.getLamportTimestamp(), true);
    }

    /** (docId, score) pairs of a RANK output, in rank order. */
    public static List<String[]> parse(String output) {
        List<String[]> rows = new ArrayList<>();
        if (output == null || output.isBlank()) {
            return rows;
        }
        for (String line : output.split("\n")) {
            String[] parts = line.split("\t");
            if (parts.length >= 2) {
                rows.add(new String[] {parts[0].trim(), parts[1].trim()});
            }
        }
        return rows;
    }
}
