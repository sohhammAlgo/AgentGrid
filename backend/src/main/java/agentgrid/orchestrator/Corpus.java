package agentgrid.orchestrator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The document corpus bundled on the classpath under /corpus (listed in /corpus/index.txt).
 * Every node loads the same files, so every node answers the same subtask the same way.
 * The first line of each document is its title; the rest is its text.
 */
public final class Corpus {

    public static final class Doc {
        private final String id;
        private final String title;
        private final List<String> sentences;
        private final Map<String, Integer> termCounts;
        private final int length;

        Doc(String id, String title, String text) {
            this.id = id;
            this.title = title;
            this.sentences = Collections.unmodifiableList(splitSentences(text));
            Map<String, Integer> counts = new HashMap<>();
            List<String> tokens = tokenize(title + " " + text);
            for (String t : tokens) {
                counts.merge(t, 1, Integer::sum);
            }
            this.termCounts = Collections.unmodifiableMap(counts);
            this.length = Math.max(1, tokens.size());
        }

        public String getId() { return id; }
        public String getTitle() { return title; }
        public List<String> getSentences() { return sentences; }
        public int count(String term) { return termCounts.getOrDefault(term, 0); }
        public boolean contains(String term) { return termCounts.containsKey(term); }
        public int length() { return length; }
    }

    private static final Set<String> STOPWORDS = Set.of(
            "a", "an", "and", "are", "as", "at", "be", "because", "been", "by", "can", "do", "does",
            "each", "for", "from", "has", "have", "how", "if", "in", "into", "is", "it", "its",
            "may", "more", "most", "must", "no", "not", "of", "on", "one", "or", "other", "so",
            "such", "than", "that", "the", "their", "them", "then", "there", "these", "they",
            "this", "those", "to", "two", "up", "use", "used", "uses", "was", "were", "what",
            "when", "where", "which", "while", "who", "why", "will", "with", "without", "would",
            "you", "your", "done", "about", "all", "any", "also", "only", "own", "same");

    private static volatile Corpus defaultCorpus;

    private final List<Doc> docs;
    private final Map<String, Doc> byId;

    Corpus(List<Doc> docs) {
        this.docs = Collections.unmodifiableList(new ArrayList<>(docs));
        Map<String, Doc> m = new LinkedHashMap<>();
        for (Doc d : docs) {
            m.put(d.getId(), d);
        }
        this.byId = Collections.unmodifiableMap(m);
    }

    /** The corpus bundled on the classpath, loaded once per JVM. */
    public static Corpus load() {
        Corpus c = defaultCorpus;
        if (c == null) {
            synchronized (Corpus.class) {
                c = defaultCorpus;
                if (c == null) {
                    c = loadFromClasspath();
                    defaultCorpus = c;
                }
            }
        }
        return c;
    }

    private static Corpus loadFromClasspath() {
        List<Doc> docs = new ArrayList<>();
        for (String name : readLines("/corpus/index.txt")) {
            if (name.isBlank()) {
                continue;
            }
            List<String> lines = readLines("/corpus/" + name.trim());
            String title = lines.isEmpty() ? name : lines.get(0).trim();
            StringBuilder text = new StringBuilder();
            for (int i = 1; i < lines.size(); i++) {
                text.append(lines.get(i).trim()).append(' ');
            }
            String id = name.trim().replaceFirst("\\.txt$", "");
            docs.add(new Doc(id, title, text.toString().trim()));
        }
        if (docs.isEmpty()) {
            throw new IllegalStateException("corpus is empty: /corpus/index.txt lists no documents");
        }
        return new Corpus(docs);
    }

    private static List<String> readLines(String resource) {
        InputStream in = Corpus.class.getResourceAsStream(resource);
        if (in == null) {
            throw new IllegalStateException("classpath resource not found: " + resource
                    + " (build with backend/build.* so src/main/resources is copied)");
        }
        List<String> lines = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return lines;
    }

    public List<Doc> docs() {
        return docs;
    }

    public Doc get(String id) {
        return byId.get(id);
    }

    public int size() {
        return docs.size();
    }

    /** Documents of chunk index (0-based) when the corpus is split into chunkCount contiguous chunks. */
    public List<Doc> chunk(int index, int chunkCount) {
        int n = docs.size();
        int count = Math.max(1, chunkCount);
        int from = (int) ((long) n * index / count);
        int to = (int) ((long) n * (index + 1) / count);
        return docs.subList(Math.max(0, from), Math.min(n, to));
    }

    /** Smoothed inverse document frequency: ln((N + 1) / (df + 1)) + 1. */
    public double idf(String term) {
        int df = 0;
        for (Doc d : docs) {
            if (d.contains(term)) {
                df++;
            }
        }
        return Math.log((docs.size() + 1.0) / (df + 1.0)) + 1.0;
    }

    /** Lower-cased word tokens without stopwords, with a plural "s" removed. */
    public static List<String> tokenize(String text) {
        List<String> out = new ArrayList<>();
        if (text == null) {
            return out;
        }
        for (String raw : text.toLowerCase().split("[^a-z0-9]+")) {
            if (raw.length() < 2 || STOPWORDS.contains(raw)) {
                continue;
            }
            out.add(stem(raw));
        }
        return out;
    }

    /** Distinct tokens, in first-seen order. */
    public static List<String> terms(String text) {
        return new ArrayList<>(new java.util.LinkedHashSet<>(tokenize(text)));
    }

    private static String stem(String w) {
        if (w.length() > 4 && w.endsWith("s") && !w.endsWith("ss") && !w.endsWith("us") && !w.endsWith("is")) {
            return w.substring(0, w.length() - 1);
        }
        return w;
    }

    static List<String> splitSentences(String text) {
        List<String> out = new ArrayList<>();
        for (String s : text.split("(?<=[.!?])\\s+")) {
            String t = s.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }

    /** Sum of idf over the distinct query terms that appear in the sentence. */
    public double sentenceScore(String sentence, List<String> queryTerms) {
        Set<String> tokens = new HashSet<>(tokenize(sentence));
        double score = 0;
        for (String q : queryTerms) {
            if (tokens.contains(q)) {
                score += idf(q);
            }
        }
        return score;
    }
}
