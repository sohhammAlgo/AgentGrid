package agentgrid.orchestrator;

import agentgrid.common.Result;
import agentgrid.common.Subtask;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * RETRIEVE: keyword match of the query over one chunk of the corpus.
 * Payload: query, chunk (0-based), chunks, and optionally docs (comma separated ids): with docs
 * (INDEX retrieval, Exp 7) only those documents of the chunk are read, otherwise all of them.
 * Output: one line per matching document, "docId TAB matchedTerms TAB title", best match first.
 */
public final class RetrieveStrategy implements StageStrategy {

    private final Corpus corpus;

    public RetrieveStrategy(Corpus corpus) {
        this.corpus = corpus;
    }

    @Override
    public Result run(Subtask subtask) {
        Payload p = Payload.decode(subtask.getPayload());
        List<String> terms = Corpus.terms(p.get("query", ""));
        int chunks = Math.max(1, p.getInt("chunks", 1));
        int chunk = Math.min(Math.max(0, p.getInt("chunk", 0)), chunks - 1);
        String docs = p.get("docs");
        Set<String> only = docs == null ? null : new HashSet<>(List.of(docs.split(",")));

        List<String[]> hits = new ArrayList<>();
        for (Corpus.Doc d : corpus.chunk(chunk, chunks)) {
            if (only != null && !only.contains(d.getId())) {
                continue;
            }
            int matched = 0;
            for (String t : terms) {
                if (d.contains(t)) {
                    matched++;
                }
            }
            if (matched > 0) {
                hits.add(new String[] {d.getId(), String.valueOf(matched), d.getTitle()});
            }
        }
        hits.sort((a, b) -> Integer.parseInt(b[1]) != Integer.parseInt(a[1])
                ? Integer.parseInt(b[1]) - Integer.parseInt(a[1]) : a[0].compareTo(b[0]));

        StringBuilder out = new StringBuilder();
        for (String[] h : hits) {
            if (out.length() > 0) {
                out.append('\n');
            }
            out.append(h[0]).append('\t').append(h[1]).append('\t').append(h[2]);
        }
        return new Result(subtask.getSubtaskId(), "retrieve", out.toString(), subtask.getLamportTimestamp(), true);
    }

    /** Document ids in a RETRIEVE output. */
    public static List<String> parseDocIds(String output) {
        List<String> ids = new ArrayList<>();
        if (output == null || output.isBlank()) {
            return ids;
        }
        for (String line : output.split("\n")) {
            String id = line.split("\t", 2)[0].trim();
            if (!id.isEmpty()) {
                ids.add(id);
            }
        }
        return ids;
    }
}
