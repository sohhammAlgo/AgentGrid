# Exp 7: MapReduce with Apache Spark (PySpark, local mode)

This experiment runs two classic MapReduce jobs over the same 40-document corpus that the AgentGrid cluster searches:

- **Job 1, word count.** `flatMap(tokenize)` produces tokens, `map` turns each into `(term, 1)`, `reduceByKey(add)` sums them, and the top 15 terms are printed.
- **Job 2, inverted index.** `map` emits `(term, (docId, 1))`, `reduceByKey` sums per `(term, docId)`, and `groupByKey` collects the postings `term -> [(docId, tf), ...]`.
- **Retrieval demo.** The Spark-built index scores the query *"How do leader election and failure detectors handle a crashed node?"* with tf-idf and prints the top 5 documents.

The script prints labelled **MAP / SHUFFLE / REDUCE** figures for each job:

- the number of partitions;
- the intermediate pairs before and after map-side combining;
- how the shuffle hash-partitions the keys;
- Spark's own stages;
- the wall time of both jobs under `local[1]` vs `local[4]`.

It then recomputes everything in plain Python with `collections.Counter`, checks that the results are equal, and prints `VERIFIED: identical`.

The code lives only in this folder. It reads the backend's corpus files but does not touch `backend/` or `frontend/`.

## Corpus

- **Path:** `backend/src/main/resources/corpus/`
- **Format:**
  - `index.txt` lists the 40 file names, one per line: `doc-01.txt` … `doc-40.txt`.
  - Each document is a plain UTF-8 text file. Line 1 is the title (for example `Bully Election`), and the rest is one paragraph of body text.
  - The document id is the file name without `.txt`, for example `doc-07`.

The Python driver reads the files and passes `(docId, title + " " + body)` to Spark with `sc.parallelize(docs, 4)`. No Spark file reader or writer is used, because on Windows those need `winutils.exe`. Results are written with plain Python to `mapreduce/out/`.

Tokenizing follows `agentgrid.orchestrator.Corpus.tokenize`:

- lowercase the text and split on `[^a-z0-9]+`;
- drop tokens shorter than 2 characters;
- drop the backend's stopword list;
- remove a plural "s".

Scoring follows the backend's RANK stage, so the terms and scores are the ones the cluster's RETRIEVE and RANK stages use:

- `score(d) = Σ_t tf(t,d) / length(d) · idf(t)`
- `idf = ln((N+1)/(df+1)) + 1`

## Requirements

| | Needed | On this machine |
|---|---|---|
| Python | 3.10+ (pyspark 4.2.0 lists 3.10–3.14) | 3.14.2 |
| Java | JDK 17+ for pyspark 4.x | `C:\Program Files\Java\jdk-23` (JDK 23) **and** `C:\Program Files\Java\jre-1.8`. Point JAVA_HOME at the JDK 23, not the JRE 1.8. |
| pyspark | `pyspark==4.2.0` (`requirements.txt`) | not installed yet |

pyspark 3.5.x supports only Java 8/11/17, so it is not used here.

**JDK 23 caveat:** Spark 4 is officially tested on Java 17 and 21; JDK 23 is not an LTS release.

- On JDK 18–23, Hadoop's login code calls `Subject.getSubject`, which throws unless a security manager is allowed. The script therefore passes `-Djava.security.manager=allow` to the driver JVM when it detects JDK 18–23. JDK 24+ rejects that flag, so it is never passed there.
- If the session still fails to start on JDK 23, install JDK 21 (LTS) and point `JAVA_HOME` at it.

## Run on Windows (PowerShell, from the repo root)

```powershell
cd C:\Users\Dhruvv\Desktop\dc\AgentGrid
python -m pip install -r mapreduce\requirements.txt
$env:JAVA_HOME = "C:\Program Files\Java\jdk-23"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
python mapreduce\exp7_mapreduce.py
```

**Options:**

- `--partitions N`: the number of input and reduce partitions; the default is 4.
- `--repeats N`: timed runs per master; the default is 3, and the median is reported.
- `--corpus DIR`: use a different corpus directory.
- `--json`: the control plane's mode (`POST /api/mapreduce/run`, dashboard tab "MapReduce"). Runs
  one job once at `local[4]` and prints exactly one JSON line on stdout. Every other line goes to
  stderr. Files are written only if the result is verified.
  - `--job index|wordcount` picks the job (default `index`).
  - `--compare` adds one `local[1]` pass.
  - It takes about 35 s here, against about 5 minutes for the console mode's 6 timed runs.

**Environment.** The script sets these itself:

- `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to the running interpreter (`sys.executable`);
- `PYTHONHASHSEED=0`;
- the Spark session to `local[4]` with the UI disabled, log level WARN, and the driver bound to 127.0.0.1.

**Harmless messages you may see on Windows:**

- At startup: `Did not find winutils.exe` / `HADOOP_HOME and hadoop.home.dir are unset` and `Unable to load native-hadoop library`. No Hadoop filesystem call is made.
- At exit: `ERROR ShutdownHookManager: Exception while deleting Spark temp dir`. Windows keeps a file lock on the temp dir.

### Expected output shape

The numbers are left out below because this file was written before the first run.

```
=====================================================================
 Exp 7: MapReduce with Apache Spark (PySpark, local mode) on the AgentGrid corpus
=====================================================================
pyspark 4.2.0; Python 3.14.2; Java 23 (...\bin\java); driver JVM option -Djava.security.manager=allow
corpus: ...\backend\src\main\resources\corpus (40 documents, read by the driver)
partitions: 4; timed runs per master: 3

---------------------------------------------------------------------
JOB 1  word count: flatMap(tokenize) -> map (term, 1) -> reduceByKey(add)
---------------------------------------------------------------------
MAP      40 documents in 4 input partitions [10, 10, 10, 10]
         mapper emitted <P> (term, 1) pairs, per partition [...]
         after map-side combine (reduceByKey's combiner): <C> pairs, per partition [...] (<x>% fewer)
SHUFFLE  hash partitioning into 4 reduce partitions: key goes to portable_hash(term) % 4; placement verified: True
         records crossing the shuffle: <C> with the combiner vs <P> without (e.g. groupByKey)
REDUCE   reduceByKey(add) -> <T> distinct terms, keys per reduce partition [...]; <P> tokens in total
         Spark job <j> stage <s>: 4 tasks, reduceByKey at ...exp7_mapreduce.py:<line>
         Spark job <j> stage <s>: 4 tasks, collectAsMap at ...exp7_mapreduce.py:<line>
top 15 terms:
   1. <term>           <count>
   ...

---------------------------------------------------------------------
JOB 2  inverted index: map (term, (docId, 1)) -> reduceByKey per (term, docId) -> postings
---------------------------------------------------------------------
MAP / SHUFFLE (1) reduceByKey, (2) groupByKey / REDUCE lines, Spark stages, then
top 10 terms by document frequency:  <term>  df=<n>  [doc-xx:tf, ...]

---------------------------------------------------------------------
RETRIEVAL  tf-idf over the Spark-built index (same formula as the backend's RANK stage)
---------------------------------------------------------------------
query: "How do leader election and failure detectors handle a crashed node?"
terms: leader (df .., idf ..), election, failure, detector, handle, crashed, node
<k> documents contain at least one query term; top 5:
  1. doc-09  <score>  Failure Detectors
  ...

---------------------------------------------------------------------
TIMING  both jobs end to end, 4 partitions, median of 3 runs
---------------------------------------------------------------------
local[1]: median <ms> ms, runs [...]  (1 task at a time)
local[4]: median <ms> ms, runs [...]  (up to 4 tasks at a time)
speed-up local[4] vs local[1]: <x>x  (...)

---------------------------------------------------------------------
CORRECTNESS  Spark results vs plain Python (collections.Counter)
---------------------------------------------------------------------
ok    word count: all term counts equal
...   (8 checks)

wrote word_count.json, inverted_index.json, retrieval.json, run_stats.json to ...\mapreduce\out
VERIFIED: identical
```

The script exits with status 0 when every check passes. If any check fails, it prints `VERIFICATION FAILED` and exits with status 1.

**Retrieval cross-check.** The cluster's RANK stage scored this query with the same formula. In a JobVerifier run its top 5 were:

| Rank | Document | Score |
|---|---|---|
| 1 | doc-09 | 0.9181 |
| 2 | doc-07 | 0.7111 |
| 3 | doc-26 | 0.3763 |
| 4 | doc-08 | 0.3165 |
| 5 | doc-30 | 0.2972 |

The Spark demo should print the same five documents. That has not been confirmed by a run yet.

**Timing.** With only 40 short documents, starting tasks and Python workers costs more than the computation itself. Expect `local[4]` to be only a little faster than `local[1]`, or about the same. The comparison shows the mechanism (parallel task slots), not a big-data speed-up.

### Output files (`mapreduce/out/`)

| File | Contents |
|---|---|
| `word_count.json` | top 15, distinct-term and token totals, every term count (sorted) |
| `inverted_index.json` | top terms by document frequency, the full index `term -> {df, postings [[docId, tf], ...]}`, `N`, `docLengths`, `builtAt`, and the corpus `fingerprint` (document count + SHA-256 of `docId:length\n` per document) that the control plane checks before INDEX retrieval |
| `retrieval.json` | the query, each term's df and idf, and the top 5 documents with score and title |
| `run_stats.json` | the MAP/SHUFFLE/REDUCE figures, Spark stages, timings, and the result of every check |

## Viva notes: Hadoop MapReduce terms → this Spark code

| Hadoop MapReduce | In `exp7_mapreduce.py` |
|---|---|
| **Input split** | One partition of `sc.parallelize(docs, 4)`. Each map task handles 10 of the 40 documents. |
| **Mapper** | `flatMap(lambda doc: tokenize(doc[1]))` + `map(lambda term: (term, 1))` (Job 1). `flatMap` emitting `(term, (docId, 1))` (Job 2). |
| **Combiner** | The map-side combine inside `reduceByKey`. PySpark's `combineByKey` sums values per key inside each map partition before the shuffle. The script prints the pair count before combining (the raw mapper output) and after it (one pair per distinct key per partition, which is what is shuffled). `groupByKey` has no combiner, so every record is shuffled. |
| **Partitioner** | Hash partitioning. A key goes to reduce partition `portable_hash(key) % numPartitions`, and the script checks that every key sits in that partition. `PYTHONHASHSEED=0` keeps the string hash the same in every worker. |
| **Shuffle and sort** | The `partitionBy` inside `reduceByKey`/`groupByKey`. Map tasks write their partitioned output, and reduce tasks fetch their partition from every map task. Spark cuts the job into stages at this boundary: a map stage and a result stage, printed as "Spark job … stage …". |
| **Reducer** | `reduceByKey(add)` sums counts (Job 1) and term frequencies per `(term, docId)` (Job 2). `groupByKey().mapValues(sorted)` builds each postings list. |
| **JobTracker / driver** | The Python driver plus the `SparkContext` in the JVM. It plans the DAG and schedules tasks. |
| **TaskTracker / slots** | `local[N]` gives one JVM with N task threads; `local[1]` runs one task at a time and `local[4]` up to four. Each Python task runs in a Python worker process. |
| **Output to HDFS** | Replaced by plain-Python JSON files. Spark chains the stages of a job in one DAG and keeps only shuffle files between them; Hadoop writes every job's output to HDFS. |

**Why Job 2's combiner already does all the work.** Every document sits in exactly one partition, so summing `((term, docId), 1)` inside a partition already gives the final tf. The reduce side of that step only moves data and does not merge any further. Word count is different: a common term appears in all 4 partitions, and the reducer adds the 4 partial sums.

## How this fits AgentGrid

- The cluster's RETRIEVE stage scans corpus chunks on several nodes, and its RANK stage scores the candidates with tf-idf.
- This experiment builds the same term statistics (document frequency, tf per document) as a distributed MapReduce inverted index.
- In a larger deployment, that index would be built offline across the cluster and loaded into RETRIEVE, so the stage looks up postings instead of scanning every document.
