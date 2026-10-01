#!/usr/bin/env python3
"""Exp 7: MapReduce with Apache Spark (PySpark, local mode) on the AgentGrid corpus.

Job 1  word count       flatMap(tokenize) -> map (term, 1) -> reduceByKey(add) -> top 15
Job 2  inverted index   flatMap (term, (docId, 1)) -> reduceByKey per (term, docId)
                        -> groupByKey into postings: term -> [(docId, tf), ...]
Demo   tf-idf retrieval over the Spark-built index (same formula as the backend's RANK stage)

The 40 documents are read by the Python driver and handed to Spark with sc.parallelize; no
Spark file reader or writer is used (on Windows those need winutils.exe). Results are written
with plain Python to mapreduce/out/*.json. Every Spark result is checked against the same
computation done in plain Python with collections.Counter.

Run from the repo root:  python mapreduce/exp7_mapreduce.py  [--partitions 4] [--repeats 3]

--json (used by the control plane, POST /api/mapreduce/run): one pass of one job (--job index or
wordcount) at local[4], plus one pass at local[1] with --compare. stdout is exactly one JSON line;
every human-readable line goes to stderr.
"""

import argparse
import hashlib
import json
import math
import os
import re
import statistics
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from operator import add
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
DEFAULT_CORPUS = REPO_ROOT / "backend" / "src" / "main" / "resources" / "corpus"
OUT_DIR = HERE / "out"
QUERY = "How do leader election and failure detectors handle a crashed node?"
TOP_WORDS = 15
TOP_DF_TERMS = 10
TOP_DOCS = 5
RULE = "-" * 69

# Same stopwords, split rule and plural stemming as agentgrid.orchestrator.Corpus.tokenize, so
# the terms here are the terms the cluster's RETRIEVE and RANK stages work with.
STOPWORDS = frozenset("""
    a an and are as at be because been by can do does each for from has have how if in into
    is it its may more most must no not of on one or other so such than that the their them
    then there these they this those to two up use used uses was were what when where which
    while who why will with without would you your done about all any also only own same
""".split())
SPLIT = re.compile(r"[^a-z0-9]+")


def stem(word):
    """Drops a plural "s" (not from -ss, -us, -is), as Corpus.stem does."""
    if len(word) > 4 and word.endswith("s") and not word.endswith(("ss", "us", "is")):
        return word[:-1]
    return word


def tokenize(text):
    """Lower-cased word tokens of 2+ characters, without stopwords, plural "s" removed."""
    return [stem(w) for w in SPLIT.split(text.lower()) if len(w) >= 2 and w not in STOPWORDS]


def idf(df, n_docs):
    """Smoothed inverse document frequency, as Corpus.idf: ln((N + 1) / (df + 1)) + 1."""
    return math.log((n_docs + 1.0) / (df + 1.0)) + 1.0


# ---------------------------------------------------------------------------------------------
# Driver-side input and the plain-Python reference results
# ---------------------------------------------------------------------------------------------

def load_corpus(corpus_dir):
    """Reads index.txt and each listed doc: line 1 is the title, the rest is the body.
    Returns ([(docId, title + " " + body)], {docId: title}); the backend tokenizes title + body."""
    index_file = corpus_dir / "index.txt"
    if not index_file.is_file():
        sys.exit(f"corpus index not found: {index_file} (pass --corpus <dir>)")
    docs, titles = [], {}
    for name in index_file.read_text(encoding="utf-8").splitlines():
        name = name.strip()
        if not name:
            continue
        lines = (corpus_dir / name).read_text(encoding="utf-8").splitlines()
        title = lines[0].strip() if lines else name
        body = " ".join(line.strip() for line in lines[1:]).strip()
        doc_id = re.sub(r"\.txt$", "", name)
        docs.append((doc_id, f"{title} {body}"))
        titles[doc_id] = title
    if not docs:
        sys.exit(f"{index_file} lists no documents")
    return docs, titles


def reference_results(docs):
    """Word count and inverted index computed in plain Python, without Spark."""
    word_count = Counter()
    postings = {}
    for doc_id, text in docs:
        tokens = tokenize(text)
        word_count.update(tokens)
        for term, tf in Counter(tokens).items():
            postings.setdefault(term, []).append((doc_id, tf))
    index = {term: sorted(plist) for term, plist in postings.items()}
    return dict(word_count), index


def reference_scores(docs, query_terms):
    """tf-idf of every document that contains a query term, as RankStrategy scores it:
    sum over query terms t of (count(t, d) / length(d)) * idf(t)."""
    n_docs = len(docs)
    counts = {doc_id: Counter(tokenize(text)) for doc_id, text in docs}
    lengths = {doc_id: max(1, sum(c.values())) for doc_id, c in counts.items()}
    df = {t: sum(1 for c in counts.values() if t in c) for t in query_terms}
    scores = {}
    for doc_id, c in counts.items():
        s = sum(c[t] / lengths[doc_id] * idf(df[t], n_docs) for t in query_terms if c[t])
        if s > 0:
            scores[doc_id] = s
    return scores


def top_k(pairs, k):
    """Highest value first, ties by key, so Spark and plain Python order ties the same way."""
    return sorted(pairs, key=lambda kv: (-kv[1], kv[0]))[:k]


def doc_lengths(docs):
    """Tokens per document, at least 1 (Corpus.Doc.length in the backend)."""
    return {doc_id: max(1, len(tokenize(text))) for doc_id, text in docs}


def corpus_fingerprint(docs):
    """Document count plus SHA-256 over "docId:length\n" for every document in index.txt order.
    The control plane computes the same over its own corpus (agentgrid.control.IndexStore) and
    refuses INDEX retrieval when the two differ."""
    lengths = doc_lengths(docs)
    text = "".join(f"{doc_id}:{lengths[doc_id]}\n" for doc_id, _ in docs)
    return {"documents": len(docs), "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "scheme": "sha256 of 'docId:length\\n' per document in index.txt order"}


def index_file(docs, index, built_at):
    """Contents of out/inverted_index.json. Besides the index (term -> {df, postings [[docId, tf], ...]})
    it carries what the control plane needs to use it without reading the documents: N, every
    document's length, the corpus fingerprint and when it was built."""
    n_postings = sum(len(p) for p in index.values())
    by_df = sorted(index.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:TOP_DF_TERMS]
    return {
        "documents": len(docs), "terms": len(index), "postings": n_postings,
        "top_by_document_frequency": [[t, len(p)] for t, p in by_df],
        "index": {t: {"df": len(p), "postings": [[d, tf] for d, tf in p]} for t, p in sorted(index.items())},
        "N": len(docs),
        "builtAt": built_at,
        "fingerprint": corpus_fingerprint(docs),
        "docLengths": doc_lengths(docs),
    }


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------------------------
# Environment checks and the Spark session
# ---------------------------------------------------------------------------------------------

def java_major_version():
    """Major version of the java that spark-submit will use (JAVA_HOME first, then PATH)."""
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin" / "java") if java_home else "java"
    try:
        proc = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        sys.exit(f"cannot run '{java} -version' ({e}); install JDK 17+ and set JAVA_HOME (see README)")
    m = re.search(r'version "(\d+)(?:\.(\d+))?', proc.stderr + proc.stdout)
    if not m:
        return None, java
    major = int(m.group(1))
    if major == 1 and m.group(2):  # "1.8.0_x" -> 8
        major = int(m.group(2))
    return major, java


def start_spark(master, driver_java_options, quiet=False):
    from pyspark.sql import SparkSession

    builder = (SparkSession.builder
               .master(master)
               .appName("AgentGrid-Exp7-MapReduce")
               .config("spark.ui.enabled", "false")
               # Loopback only: a changing Wi-Fi address must not break driver/executor traffic.
               .config("spark.driver.host", "127.0.0.1")
               .config("spark.driver.bindAddress", "127.0.0.1"))
    if driver_java_options:
        # Read by spark-submit when it launches the driver JVM (first session only).
        builder = builder.config("spark.driver.extraJavaOptions", driver_java_options)
    if quiet:
        # No "[Stage n: ...]" progress bar on stderr (the control plane keeps the stderr tail).
        builder = builder.config("spark.ui.showConsoleProgress", "false")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------------------------
# The two MapReduce jobs as RDD pipelines
# ---------------------------------------------------------------------------------------------

def word_count_rdds(docs_rdd, partitions):
    # MAP: one (term, 1) pair per token.
    mapped = docs_rdd.flatMap(lambda doc: tokenize(doc[1])).map(lambda term: (term, 1))
    # COMBINE + SHUFFLE + REDUCE: reduceByKey sums per key inside each map partition first,
    # hash-partitions the partial sums, then sums them again on the reduce side.
    counts = mapped.reduceByKey(add, numPartitions=partitions)
    return mapped, counts


def inverted_index_rdds(docs_rdd, partitions):
    # MAP: one (term, (docId, 1)) pair per token.
    mapped = docs_rdd.flatMap(lambda doc: [(term, (doc[0], 1)) for term in tokenize(doc[1])])
    # REDUCE 1: term frequency per (term, docId).
    tf = (mapped.map(lambda kv: ((kv[0], kv[1][0]), kv[1][1]))
                .reduceByKey(add, numPartitions=partitions))
    # REDUCE 2: re-key by term and group into a sorted postings list.
    index = (tf.map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
               .groupByKey(numPartitions=partitions)
               .mapValues(sorted))
    return mapped, tf, index


def run_pipeline(sc, docs, partitions):
    docs_rdd = sc.parallelize(docs, partitions)
    _, counts = word_count_rdds(docs_rdd, partitions)
    _, _, index = inverted_index_rdds(docs_rdd, partitions)
    return counts.collectAsMap(), index.collectAsMap()


def time_pipeline(sc, docs, partitions, repeats):
    """Median wall time of both jobs. A warm-up job first starts the Python worker processes."""
    sc.parallelize(range(sc.defaultParallelism * 4), sc.defaultParallelism).map(lambda x: x + 1).count()
    times, result = [], None
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = run_pipeline(sc, docs, partitions)
        times.append((time.perf_counter() - t0) * 1000.0)
    return times, result


def spark_retrieval(sc, docs_rdd, index, n_docs, query_terms):
    """Scores documents with the Spark-built index: only the query terms' postings are read."""
    doc_len = sc.broadcast(dict(docs_rdd.map(lambda d: (d[0], max(1, len(tokenize(d[1]))))).collect()))
    wanted = sc.broadcast(frozenset(query_terms))
    query_postings = index.filter(lambda kv: kv[0] in wanted.value)
    df = query_postings.map(lambda kv: (kv[0], len(kv[1]))).collectAsMap()
    scores = (query_postings
              .flatMap(lambda kv: [(doc_id, tf / doc_len.value[doc_id] * idf(len(kv[1]), n_docs))
                                   for doc_id, tf in kv[1]])
              .reduceByKey(add)
              .collectAsMap())
    return scores, df


# ---------------------------------------------------------------------------------------------
# Stage metrics
# ---------------------------------------------------------------------------------------------

def partition_sizes(rdd):
    """Number of records in each partition."""
    return rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()


def combined_sizes(rdd):
    """Records each map partition sends to the shuffle after map-side combining: one per distinct
    key in that partition. PySpark's reduceByKey (combineByKey) merges values per key inside each
    partition before partitionBy, so this is what crosses the shuffle."""
    return rdd.mapPartitions(lambda it: [len({k for k, _ in it})]).collect()


def hash_placement_ok(rdd, partitions):
    """True when every key sits in reduce partition portable_hash(key) % partitions."""
    try:
        from pyspark.rdd import portable_hash
    except ImportError:
        return None
    return all(rdd.mapPartitionsWithIndex(
        lambda i, it: [i == portable_hash(k) % partitions for k, _ in it]).collect())


def stage_lines(sc, group):
    """Spark's own stages for the jobs run under a job group (map stage and result stage)."""
    try:
        tracker = sc.statusTracker()
        lines = []
        for job_id in sorted(tracker.getJobIdsForGroup(group)):
            info = tracker.getJobInfo(job_id)
            for stage_id in sorted(info.stageIds) if info else []:
                st = tracker.getStageInfo(stage_id)
                if st:
                    lines.append(f"Spark job {job_id} stage {stage_id}: {st.numTasks} tasks, {st.name}")
        return lines or ["(no stage info recorded)"]
    except Exception as e:  # informational only
        return [f"(stage info unavailable: {e})"]


def pct_fewer(before, after):
    return 0.0 if before == 0 else 100.0 * (before - after) / before


def header(title):
    print()
    print(RULE)
    print(title)
    print(RULE)


def spark_environment():
    """Points Spark's Python workers at this interpreter and checks pyspark and Java.
    Returns (pyspark version, java major, java command, driver JVM options); exits on a problem."""
    # Spark's Python workers must run this same interpreter (and pyspark install).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("PYTHONHASHSEED", "0")

    try:
        import pyspark
    except ImportError:
        sys.exit("pyspark is not installed: python -m pip install -r mapreduce/requirements.txt")

    java_major, java_cmd = java_major_version()
    if java_major is not None and java_major < 17:
        sys.exit(f"{java_cmd} is Java {java_major}; pyspark {pyspark.__version__} needs Java 17+. "
                 f"Set JAVA_HOME to a JDK 17+ (for example C:\\Program Files\\Java\\jdk-23).")
    # On JDK 18-23, Subject.getSubject (called by Hadoop's UserGroupInformation at SparkContext
    # start) throws unless a security manager is allowed. JDK 24+ rejects this flag, so it is
    # added only for 18-23.
    driver_java_options = "-Djava.security.manager=allow" if java_major and 18 <= java_major <= 23 else ""
    return pyspark.__version__, java_major, java_cmd, driver_java_options


# ---------------------------------------------------------------------------------------------
# --json: one job, one timed pass, one JSON line on stdout
# ---------------------------------------------------------------------------------------------

def claim_stdout():
    """Keeps the real stdout for the single JSON line and points file descriptor 1 (and
    sys.stdout) at stderr, so nothing else (prints, the Spark JVM, Python workers) reaches stdout."""
    sys.stdout.flush()
    saved = os.dup(1)
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    return os.fdopen(saved, "w", encoding="utf-8")


def timed_collect(rdd):
    t0 = time.perf_counter()
    result = rdd.collectAsMap()
    return result, (time.perf_counter() - t0) * 1000.0


def json_pass(sc, docs, job, partitions):
    """One timed pass of one job: (result, ms, mapper output as (key, value) pairs).
    No warm-up job: on Windows PySpark starts a Python worker per task, so every extra job
    costs seconds; the timed pass therefore includes worker start-up."""
    docs_rdd = sc.parallelize(docs, partitions)
    if job == "wordcount":
        mapped, counts = word_count_rdds(docs_rdd, partitions)
        result, ms = timed_collect(counts)
        return result, ms, mapped
    mapped, _, index = inverted_index_rdds(docs_rdd, partitions)
    result, ms = timed_collect(index)
    return result, ms, mapped.map(lambda kv: ((kv[0], kv[1][0]), kv[1][1]))


def map_side_sizes(pairs):
    """One Spark job: per map partition, (pairs the mapper emitted, pairs left after the map-side
    combine = distinct keys), i.e. partition_sizes and combined_sizes together."""
    def sizes(it):
        n, keys = 0, set()
        for k, _ in it:
            n += 1
            keys.add(k)
        return [(n, len(keys))]
    per_part = pairs.mapPartitions(sizes).collect()
    return [n for n, _ in per_part], [c for _, c in per_part]


def index_scores(index, lengths, n_docs, query_terms):
    """tf-idf of the query over the Spark-built index (collected to the driver): only the query
    terms' postings are read. Same formula as spark_retrieval and the backend's RANK stage."""
    scores, df = {}, {}
    for t in query_terms:
        plist = index.get(t, [])
        df[t] = len(plist)
        for doc_id, tf in plist:
            scores[doc_id] = scores.get(doc_id, 0.0) + tf / lengths[doc_id] * idf(len(plist), n_docs)
    return scores, df


def main_json(args, partitions):
    try:
        sys.stderr.reconfigure(errors="backslashreplace")
    except AttributeError:
        pass
    out = claim_stdout()

    def log(msg):
        print(msg, file=sys.stderr, flush=True)

    pyspark_version, java_major, java_cmd, driver_java_options = spark_environment()
    docs, titles = load_corpus(args.corpus)
    n_docs = len(docs)
    query_terms = list(dict.fromkeys(tokenize(QUERY)))
    ref_counts, ref_index = reference_results(docs)
    built_at = now_iso()
    log(f"exp7 --json: job {args.job}, {partitions} partitions, {n_docs} documents, compare={args.compare}; "
        f"pyspark {pyspark_version}, Python {sys.version.split()[0]}, Java {java_major} ({java_cmd})")

    checks = {}
    top_docs = []
    docs_matching = None
    spark = start_spark("local[4]", driver_java_options, quiet=True)
    sc = spark.sparkContext
    try:
        sc.setJobGroup("exp7-run", args.job)
        result, local4_ms, pairs = json_pass(sc, docs, args.job, partitions)
        spark_stages = stage_lines(sc, "exp7-run")
        log(f"local[4] pass: {local4_ms:.0f} ms")
        sc.setJobGroup("exp7-metrics", "stage metrics")
        raw, comb = map_side_sizes(pairs)
    finally:
        spark.stop()

    if args.job == "wordcount":
        top = top_k(result.items(), TOP_WORDS)
        top_terms = [{"term": t, "count": n} for t, n in top]
        checks["word count: all term counts equal"] = result == ref_counts
        checks["word count: top 15 equal"] = top == top_k(ref_counts.items(), TOP_WORDS)
        reduce_keys = len(result)
        extra = {"tokens": sum(result.values())}
    else:
        totals = {t: sum(tf for _, tf in plist) for t, plist in result.items()}
        top = top_k(totals.items(), TOP_WORDS)
        top_terms = [{"term": t, "count": n, "df": len(result[t])} for t, n in top]
        scores, query_df = index_scores(result, doc_lengths(docs), n_docs, query_terms)
        ref_scores = reference_scores(docs, query_terms)
        top_pairs = top_k(scores.items(), TOP_DOCS)
        top_docs = [{"rank": i, "docId": d, "score": round(sc_, 6), "title": titles.get(d, "")}
                    for i, (d, sc_) in enumerate(top_pairs, 1)]
        docs_matching = len(scores)
        checks["inverted index: all postings equal"] = result == ref_index
        checks["inverted index: term totals equal word count"] = totals == ref_counts
        checks["retrieval: same documents scored"] = set(scores) == set(ref_scores)
        checks["retrieval: same top 5 documents"] = (
            [d for d, _ in top_pairs] == [d for d, _ in top_k(ref_scores.items(), TOP_DOCS)])
        checks["retrieval: scores equal (rel. tol. 1e-9)"] = all(
            math.isclose(scores[d], ref_scores[d], rel_tol=1e-9) for d in ref_scores if d in scores)
        reduce_keys = len(result)
        extra = {"postings": sum(len(p) for p in result.values()),
                 "queryTerms": {t: query_df.get(t, 0) for t in query_terms}}

    local1_ms = None
    if args.compare:
        spark = start_spark("local[1]", driver_java_options, quiet=True)
        try:
            result1, local1_ms, _ = json_pass(spark.sparkContext, docs, args.job, partitions)
        finally:
            spark.stop()
        log(f"local[1] pass: {local1_ms:.0f} ms")
        checks["local[1] pass: results equal"] = result1 == result

    verified = all(checks.values())
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if args.job == "wordcount":
        written = {"word_count.json": {
            "top15": [[t, n] for t, n in top], "distinct_terms": len(result),
            "total_tokens": sum(result.values()), "builtAt": built_at,
            "counts": dict(sorted(result.items(), key=lambda kv: (-kv[1], kv[0])))}}
    else:
        written = {
            "inverted_index.json": index_file(docs, result, built_at),
            "retrieval.json": {
                "query": QUERY, "terms": {t: {"df": query_df.get(t, 0), "idf": idf(query_df.get(t, 0), n_docs)}
                                          for t in query_terms},
                "top": top_docs}}
    if verified:   # a result that disagrees with plain Python never replaces the files
        for name, data in written.items():
            with open(OUT_DIR / name, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        log(f"wrote {', '.join(written)} to {OUT_DIR}")
    log("VERIFIED: identical" if verified else "VERIFICATION FAILED: Spark and plain Python disagree")

    report = {
        "job": args.job,
        "partitions": partitions,
        "stages": {"mapPairs": sum(raw), "pairsAfterCombine": sum(comb), "reduceKeys": reduce_keys,
                   "shuffleRecordsWithCombiner": sum(comb), "shuffleRecordsWithout": sum(raw), **extra},
        "timings": {"local4Ms": round(local4_ms, 1), "local1Ms": None if local1_ms is None else round(local1_ms, 1),
                    "speedup": None if local1_ms is None else round(local1_ms / local4_ms, 3)},
        "topTerms": top_terms,
        "query": QUERY,
        "topDocsForQuery": top_docs,
        "docsMatchingQuery": docs_matching,
        "verified": verified,
        "checks": checks,
        "docCount": n_docs,
        "builtAt": built_at,
        "fingerprint": corpus_fingerprint(docs),
        "sparkStages": spark_stages,
        "filesWritten": sorted(written) if verified else [],
        "versions": {"pyspark": pyspark_version, "python": sys.version.split()[0], "java": java_major},
    }
    out.write(json.dumps(report, ensure_ascii=False) + "\n")
    out.flush()
    return 0


# ---------------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Exp 7: MapReduce with PySpark on the AgentGrid corpus")
    ap.add_argument("--partitions", type=int, default=4, help="input and reduce partitions (default 4)")
    ap.add_argument("--repeats", type=int, default=3, help="timed runs per master (default 3)")
    ap.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS, help="corpus directory with index.txt")
    ap.add_argument("--json", action="store_true",
                    help="one JSON line on stdout, logs on stderr; one local[4] pass of --job")
    ap.add_argument("--job", choices=["index", "wordcount"], default="index", help="job for --json (default index)")
    ap.add_argument("--compare", action="store_true", help="with --json: also one pass at local[1]")
    args = ap.parse_args()
    partitions = max(1, args.partitions)
    repeats = max(1, args.repeats)
    if args.json:
        return main_json(args, partitions)

    try:
        sys.stdout.reconfigure(errors="backslashreplace")
    except AttributeError:
        pass

    pyspark_version, java_major, java_cmd, driver_java_options = spark_environment()

    docs, titles = load_corpus(args.corpus)
    n_docs = len(docs)
    query_terms = list(dict.fromkeys(tokenize(QUERY)))
    ref_counts, ref_index = reference_results(docs)
    ref_scores = reference_scores(docs, query_terms)

    print("=" * 69)
    print(" Exp 7: MapReduce with Apache Spark (PySpark, local mode) on the AgentGrid corpus")
    print("=" * 69)
    print(f"pyspark {pyspark_version}; Python {sys.version.split()[0]}; "
          f"Java {java_major if java_major else '?'} ({java_cmd})"
          + (f"; driver JVM option {driver_java_options}" if driver_java_options else ""))
    print(f"corpus: {args.corpus} ({n_docs} documents, read by the driver)")
    print(f"partitions: {partitions}; timed runs per master: {repeats}")

    stats = {"partitions": partitions, "documents": n_docs, "pyspark": pyspark_version,
             "python": sys.version.split()[0], "java": java_major}

    spark = start_spark("local[4]", driver_java_options)
    sc = spark.sparkContext
    try:
        docs_rdd = sc.parallelize(docs, partitions)

        # ----- Job 1: word count -------------------------------------------------------------
        header("JOB 1  word count: flatMap(tokenize) -> map (term, 1) -> reduceByKey(add)")
        sc.setJobGroup("exp7-job1", "word count")
        wc_mapped, wc_counts = word_count_rdds(docs_rdd, partitions)
        counts = wc_counts.collectAsMap()
        job1_stages = stage_lines(sc, "exp7-job1")
        sc.setJobGroup("exp7-metrics", "stage metrics")

        docs_per_part = partition_sizes(docs_rdd)
        wc_raw = partition_sizes(wc_mapped)
        wc_comb = combined_sizes(wc_mapped)
        wc_reduce = partition_sizes(wc_counts)
        wc_hash_ok = hash_placement_ok(wc_counts, partitions)

        print(f"MAP      {n_docs} documents in {docs_rdd.getNumPartitions()} input partitions {docs_per_part}")
        print(f"         mapper emitted {sum(wc_raw)} (term, 1) pairs, per partition {wc_raw}")
        print(f"         after map-side combine (reduceByKey's combiner): {sum(wc_comb)} pairs, "
              f"per partition {wc_comb} ({pct_fewer(sum(wc_raw), sum(wc_comb)):.1f}% fewer)")
        print(f"SHUFFLE  hash partitioning into {wc_counts.getNumPartitions()} reduce partitions: "
              f"key goes to portable_hash(term) % {partitions}; placement verified: {wc_hash_ok}")
        print(f"         records crossing the shuffle: {sum(wc_comb)} with the combiner "
              f"vs {sum(wc_raw)} without (e.g. groupByKey)")
        print(f"REDUCE   reduceByKey(add) -> {len(counts)} distinct terms, "
              f"keys per reduce partition {wc_reduce}; {sum(counts.values())} tokens in total")
        for line in job1_stages:
            print(f"         {line}")
        top_words = top_k(counts.items(), TOP_WORDS)
        print(f"top {TOP_WORDS} terms:")
        for rank, (term, n) in enumerate(top_words, 1):
            print(f"  {rank:2d}. {term:<16} {n}")

        # ----- Job 2: inverted index ---------------------------------------------------------
        header("JOB 2  inverted index: map (term, (docId, 1)) -> reduceByKey per (term, docId) -> postings")
        sc.setJobGroup("exp7-job2", "inverted index")
        ix_mapped, ix_tf, ix_index = inverted_index_rdds(docs_rdd, partitions)
        index = ix_index.collectAsMap()
        job2_stages = stage_lines(sc, "exp7-job2")
        sc.setJobGroup("exp7-metrics", "stage metrics")

        ix_raw = partition_sizes(ix_mapped)
        ix_keyed = ix_mapped.map(lambda kv: ((kv[0], kv[1][0]), kv[1][1]))
        ix_comb = combined_sizes(ix_keyed)
        ix_tf_parts = partition_sizes(ix_tf)
        ix_parts = partition_sizes(ix_index)
        n_postings = sum(len(p) for p in index.values())

        print(f"MAP      mapper emitted {sum(ix_raw)} (term, (docId, 1)) pairs, per partition {ix_raw}")
        print(f"         after map-side combine on (term, docId): {sum(ix_comb)} pairs, per partition "
              f"{ix_comb} ({pct_fewer(sum(ix_raw), sum(ix_comb)):.1f}% fewer); a document never spans "
              f"two partitions, so the combiner already produces final tf values")
        print(f"SHUFFLE  1) reduceByKey on (term, docId): {sum(ix_comb)} records hash-partitioned into "
              f"{ix_tf.getNumPartitions()} partitions {ix_tf_parts}")
        print(f"         2) groupByKey on term: {n_postings} (docId, tf) postings hash-partitioned into "
              f"{ix_index.getNumPartitions()} partitions (no combiner: every posting is kept)")
        print(f"REDUCE   {len(index)} terms, {n_postings} postings; terms per reduce partition {ix_parts}")
        for line in job2_stages:
            print(f"         {line}")
        by_df = sorted(index.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:TOP_DF_TERMS]
        print(f"top {TOP_DF_TERMS} terms by document frequency (of {n_docs} documents):")
        for rank, (term, plist) in enumerate(by_df, 1):
            shown = ", ".join(f"{d}:{tf}" for d, tf in plist[:6]) + (", ..." if len(plist) > 6 else "")
            print(f"  {rank:2d}. {term:<16} df={len(plist):2d}  [{shown}]")

        # ----- Retrieval demo ----------------------------------------------------------------
        header("RETRIEVAL  tf-idf over the Spark-built index (same formula as the backend's RANK stage)")
        scores, query_df = spark_retrieval(sc, docs_rdd, ix_index, n_docs, query_terms)
        top_docs = top_k(scores.items(), TOP_DOCS)
        print(f'query: "{QUERY}"')
        print("terms: " + ", ".join(f"{t} (df {query_df.get(t, 0)}, idf {idf(query_df.get(t, 0), n_docs):.4f})"
                                    for t in query_terms))
        print("score(d) = sum over query terms t of tf(t, d) / length(d) * idf(t); "
              "idf = ln((N + 1) / (df + 1)) + 1")
        print(f"{len(scores)} documents contain at least one query term; top {TOP_DOCS}:")
        for rank, (doc_id, score) in enumerate(top_docs, 1):
            print(f"  {rank}. {doc_id}  {score:.4f}  {titles.get(doc_id, '')}")

        # ----- Timing ------------------------------------------------------------------------
        header(f"TIMING  both jobs end to end, {partitions} partitions, median of {repeats} runs")
        t4, res4 = time_pipeline(sc, docs, partitions, repeats)
    finally:
        spark.stop()

    spark = start_spark("local[1]", driver_java_options)
    try:
        t1, res1 = time_pipeline(spark.sparkContext, docs, partitions, repeats)
    finally:
        spark.stop()

    med1, med4 = statistics.median(t1), statistics.median(t4)
    print(f"local[1]: median {med1:.0f} ms, runs {[round(t) for t in t1]}  (1 task at a time)")
    print(f"local[4]: median {med4:.0f} ms, runs {[round(t) for t in t4]}  (up to 4 tasks at a time)")
    print(f"speed-up local[4] vs local[1]: {med1 / med4:.2f}x  (40 short documents: task scheduling "
          f"and Python worker overhead dominate, so the gain is small)")

    # ----- Correctness -----------------------------------------------------------------------
    header("CORRECTNESS  Spark results vs plain Python (collections.Counter)")
    ref_top = top_k(ref_scores.items(), TOP_DOCS)
    checks = [
        ("word count: all term counts equal", counts == ref_counts),
        ("word count: top 15 equal", top_words == top_k(ref_counts.items(), TOP_WORDS)),
        ("inverted index: all postings equal", index == ref_index),
        ("retrieval: same documents scored", set(scores) == set(ref_scores)),
        ("retrieval: same top 5 documents", [d for d, _ in top_docs] == [d for d, _ in ref_top]),
        ("retrieval: scores equal (rel. tol. 1e-9)",
         all(math.isclose(scores[d], ref_scores[d], rel_tol=1e-9) for d in ref_scores if d in scores)),
        ("timed runs local[4]: results equal", res4 == (ref_counts, ref_index)),
        ("timed runs local[1]: results equal", res1 == (ref_counts, ref_index)),
    ]
    for name, ok in checks:
        print(f"{'ok  ' if ok else 'FAIL'}  {name}")
    verified = all(ok for _, ok in checks)

    # ----- Results to mapreduce/out/*.json ----------------------------------------------------
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stats.update({
        "job1_word_count": {
            "input_partitions": docs_per_part, "mapped_pairs": sum(wc_raw), "mapped_per_partition": wc_raw,
            "after_map_side_combine": sum(wc_comb), "combined_per_partition": wc_comb,
            "reduce_partitions": wc_reduce, "hash_placement_verified": wc_hash_ok,
            "distinct_terms": len(counts), "spark_stages": job1_stages},
        "job2_inverted_index": {
            "mapped_pairs": sum(ix_raw), "mapped_per_partition": ix_raw,
            "after_map_side_combine": sum(ix_comb), "combined_per_partition": ix_comb,
            "tf_partitions": ix_tf_parts, "index_partitions": ix_parts,
            "terms": len(index), "postings": n_postings, "spark_stages": job2_stages},
        "timing_ms": {"local[1]": {"median": med1, "runs": t1}, "local[4]": {"median": med4, "runs": t4},
                      "speedup": med1 / med4},
        "checks": {name: ok for name, ok in checks},
        "verified": verified,
    })
    outputs = {
        "word_count.json": {
            "top15": [[t, n] for t, n in top_words], "distinct_terms": len(counts),
            "total_tokens": sum(counts.values()),
            "counts": dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))},
        "inverted_index.json": index_file(docs, index, now_iso()),
        "retrieval.json": {
            "query": QUERY, "terms": {t: {"df": query_df.get(t, 0), "idf": idf(query_df.get(t, 0), n_docs)}
                                      for t in query_terms},
            "top": [{"rank": i, "docId": d, "score": s, "title": titles.get(d, "")}
                    for i, (d, s) in enumerate(top_docs, 1)]},
        "run_stats.json": stats,
    }
    for name, data in outputs.items():
        with open(OUT_DIR / name, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    print()
    print(f"wrote {', '.join(outputs)} to {OUT_DIR}")

    if not verified:
        print("VERIFICATION FAILED: Spark and plain Python disagree (see FAIL lines above)")
        return 1
    print("VERIFIED: identical")
    return 0


if __name__ == "__main__":
    sys.exit(main())
