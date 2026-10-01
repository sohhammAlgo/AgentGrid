package agentgrid.orchestrator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * How a job's RETRIEVE stage picks the documents it reads.
 *
 * SCAN (the default): every corpus chunk is dispatched and its worker checks every document.
 * INDEX: the control plane looked the query's terms up in the MapReduce-built inverted index
 * (Exp 7) and passes the documents that contain at least one of them; only the chunks holding
 * such a document are dispatched, and each worker checks only those documents. RANK, SUMMARIZE
 * and SYNTHESIZE are the same in both modes.
 */
public final class RetrievalPlan implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SCAN = "SCAN";
    public static final String INDEX = "INDEX";

    private final String mode;
    private final ArrayList<String> docIds;
    private final String indexBuiltAt;
    private final ArrayList<String> queryTerms;

    private RetrievalPlan(String mode, List<String> docIds, String indexBuiltAt, List<String> queryTerms) {
        this.mode = mode;
        this.docIds = docIds == null ? null : new ArrayList<>(docIds);
        this.indexBuiltAt = indexBuiltAt;
        this.queryTerms = queryTerms == null ? new ArrayList<>() : new ArrayList<>(queryTerms);
    }

    public static RetrievalPlan scan() {
        return new RetrievalPlan(SCAN, null, null, null);
    }

    /**
     * @param docIds       the documents whose postings contain at least one query term
     * @param indexBuiltAt builtAt of the index the lookup used (shown with the job)
     * @param queryTerms   the query's terms, as Corpus.terms tokenized them
     */
    public static RetrievalPlan index(List<String> docIds, String indexBuiltAt, List<String> queryTerms) {
        return new RetrievalPlan(INDEX, docIds, indexBuiltAt, queryTerms);
    }

    public String getMode() { return mode; }
    public boolean isIndex() { return INDEX.equals(mode); }
    /** The documents RETRIEVE may read in INDEX mode; null in SCAN mode. */
    public List<String> getDocIds() { return docIds == null ? null : Collections.unmodifiableList(docIds); }
    public Set<String> docIdSet() { return docIds == null ? Set.of() : new LinkedHashSet<>(docIds); }
    public String getIndexBuiltAt() { return indexBuiltAt; }
    public List<String> getQueryTerms() { return Collections.unmodifiableList(queryTerms); }

    @Override
    public String toString() {
        return isIndex() ? INDEX + " (" + docIds.size() + " documents)" : SCAN;
    }
}
