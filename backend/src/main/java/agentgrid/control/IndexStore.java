package agentgrid.control;

import agentgrid.orchestrator.Corpus;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The MapReduce-built inverted index (Exp 7, mapreduce/out/inverted_index.json) as the control
 * plane uses it for INDEX retrieval. Loaded at start if the file exists and again after every
 * successful MapReduce run.
 *
 * The file carries a corpus fingerprint: the document count and a SHA-256 over "docId:length\n"
 * for every document in index.txt order, length being the document's token count. The same is
 * computed here over the control plane's own corpus (the one the nodes load from the classpath).
 * If they differ the index is stale (built over other documents, or before a corpus change that
 * was then rebuilt) and INDEX jobs are refused with 409 until MapReduce is run again.
 */
public class IndexStore {

    /** The result of looking a query up in the index. */
    public static final class Lookup {
        private final List<String> terms;
        private final List<String> docIds;
        private final String builtAt;
        private final long micros;

        Lookup(List<String> terms, List<String> docIds, String builtAt, long micros) {
            this.terms = terms;
            this.docIds = docIds;
            this.builtAt = builtAt;
            this.micros = micros;
        }

        /** The query's terms (Corpus.terms, the backend's tokenizer). */
        public List<String> getTerms() { return terms; }
        /** Documents whose postings contain at least one query term, in corpus order. */
        public List<String> getDocIds() { return docIds; }
        public String getBuiltAt() { return builtAt; }
        public long getMicros() { return micros; }
    }

    /** One load of the file; replaced as a whole. */
    private static final class Snapshot {
        boolean available;
        String reason;
        Map<String, List<String>> docsByTerm = Collections.emptyMap();
        int terms;
        int postings;
        Integer n;
        String builtAt;
        Integer fingerprintDocs;
        String fingerprintSha;
        long loadedAtMs;
    }

    private final Path file;
    private final Corpus corpus;
    private final int corpusDocs;
    private final String corpusSha;
    private volatile Snapshot snapshot = new Snapshot();
    /** Test hook (-Dagentgrid.testHooks=true): the backend's expected fingerprint is replaced. */
    private volatile boolean corruptExpectation;

    public IndexStore(Path file, Corpus corpus) {
        this.file = file;
        this.corpus = corpus;
        this.corpusDocs = corpus.size();
        this.corpusSha = fingerprint(corpus);
        snapshot.reason = "not loaded yet";
    }

    /** SHA-256 hex over "docId:length\n" for every document, in corpus (index.txt) order. */
    static String fingerprint(Corpus corpus) {
        StringBuilder sb = new StringBuilder();
        for (Corpus.Doc d : corpus.docs()) {
            sb.append(d.getId()).append(':').append(d.length()).append('\n');
        }
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public Path getFile() {
        return file;
    }

    /** (Re)reads the index file; a missing or unreadable file makes the index unavailable. */
    public synchronized Map<String, Object> reload() {
        Snapshot s = new Snapshot();
        s.loadedAtMs = System.currentTimeMillis();
        if (!Files.isRegularFile(file)) {
            s.reason = "no index file (" + file + "): run MapReduce first";
            snapshot = s;
            return view();
        }
        try {
            Map<String, Object> root = JsonUtil.parseObject(Files.readString(file, StandardCharsets.UTF_8));
            Map<String, Object> index = asMap(root.get("index"));
            if (index == null) {
                throw new IllegalArgumentException("no \"index\" object");
            }
            Map<String, List<String>> docsByTerm = new HashMap<>();
            int postings = 0;
            for (Map.Entry<String, Object> e : index.entrySet()) {
                Map<String, Object> entry = asMap(e.getValue());
                List<String> docs = new ArrayList<>();
                for (Object posting : asList(entry == null ? null : entry.get("postings"))) {
                    List<Object> pair = asList(posting);
                    if (!pair.isEmpty()) {
                        docs.add(String.valueOf(pair.get(0)));
                    }
                }
                postings += docs.size();
                docsByTerm.put(e.getKey(), Collections.unmodifiableList(docs));
            }
            s.docsByTerm = Collections.unmodifiableMap(docsByTerm);
            s.terms = docsByTerm.size();
            s.postings = postings;
            s.n = root.get("N") instanceof Number num ? num.intValue() : null;
            s.builtAt = root.get("builtAt") == null ? null : String.valueOf(root.get("builtAt"));
            Map<String, Object> fp = asMap(root.get("fingerprint"));
            if (fp != null) {
                s.fingerprintDocs = fp.get("documents") instanceof Number num ? num.intValue() : null;
                s.fingerprintSha = fp.get("sha256") == null ? null : String.valueOf(fp.get("sha256"));
            }
            s.available = true;
        } catch (Exception e) {
            s.available = false;
            s.reason = "index file " + file + " could not be read: " + e.getMessage();
        }
        snapshot = s;
        return view();
    }

    /** Null if the snapshot's fingerprint matches the backend corpus, else why it is stale. */
    private String staleReason(Snapshot s) {
        if (!s.available) {
            return null;
        }
        if (s.fingerprintSha == null || s.fingerprintDocs == null) {
            return "the index file has no corpus fingerprint (written by an older exp7_mapreduce.py)";
        }
        String expected = corruptExpectation ? "corrupted-by-test-hook" : corpusSha;
        if (s.fingerprintDocs != corpusDocs || !s.fingerprintSha.equals(expected)) {
            return "corpus fingerprint mismatch: the index was built over " + s.fingerprintDocs + " documents (sha256 "
                    + abbrev(s.fingerprintSha) + "), the control plane's corpus has " + corpusDocs
                    + " documents (sha256 " + abbrev(expected) + ")";
        }
        return null;
    }

    /** GET /api/mapreduce/index: {available, stale, fingerprintOk, reason, terms, postings, documents, builtAt, ...}. */
    public Map<String, Object> view() {
        Snapshot s = snapshot;
        String stale = staleReason(s);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("available", s.available);
        m.put("fingerprintOk", s.available && stale == null);
        m.put("stale", s.available && stale != null);
        m.put("usable", s.available && stale == null);
        m.put("reason", s.available ? stale : s.reason);
        m.put("terms", s.terms);
        m.put("postings", s.postings);
        m.put("documents", s.n);
        m.put("builtAt", s.builtAt);
        m.put("corpusDocuments", corpusDocs);
        m.put("fingerprint", s.fingerprintSha);
        m.put("corpusFingerprint", corpusSha);
        m.put("testHookCorruptFingerprint", corruptExpectation);
        m.put("file", file.toString());
        m.put("loadedAtMs", s.loadedAtMs == 0 ? null : s.loadedAtMs);
        return m;
    }

    /**
     * The documents that contain at least one of the query's terms. 409 if there is no index
     * or it is stale. The query is tokenized with the backend's own Corpus.terms.
     */
    public Lookup lookup(String query) throws JobDirectory.ApiException {
        Snapshot s = snapshot;
        if (!s.available) {
            throw new JobDirectory.ApiException(409, "retrieval INDEX needs the MapReduce index: run MapReduce first "
                    + "(POST /api/mapreduce/run {\"job\":\"index\"}); " + s.reason);
        }
        String stale = staleReason(s);
        if (stale != null) {
            throw new JobDirectory.ApiException(409, "the MapReduce index is stale, re-run MapReduce: " + stale);
        }
        long t0 = System.nanoTime();
        List<String> terms = Corpus.terms(query);
        Set<String> hit = new HashSet<>();
        for (String t : terms) {
            hit.addAll(s.docsByTerm.getOrDefault(t, List.of()));
        }
        List<String> docIds = new ArrayList<>();
        for (Corpus.Doc d : corpus.docs()) {
            if (hit.contains(d.getId())) {
                docIds.add(d.getId());
            }
        }
        return new Lookup(terms, docIds, s.builtAt, (System.nanoTime() - t0) / 1000);
    }

    /** Test hook: true makes the backend expect a fingerprint no index has (the index reads as stale). */
    public void setCorruptExpectation(boolean corrupt) {
        corruptExpectation = corrupt;
    }

    private static String abbrev(String sha) {
        return sha == null ? "none" : sha.length() > 12 ? sha.substring(0, 12) + "..." : sha;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return o instanceof Map ? (Map<String, Object>) o : null;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object o) {
        return o instanceof List ? (List<Object>) o : List.of();
    }
}
