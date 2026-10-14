# AgentGrid

A distributed agent grid built on Java RMI. A client dispatches `Subtask`s to a
remote agent node, which executes them and returns `Result`s.


## Commands to run the Control Plane
cd ./backend
.\build.ps1
java -cp build/classes agentgrid.control.ControlPlaneMain


+
## Layout

```
backend/src/main/java/agentgrid/
  common/   Subtask, Result          — serializable types marshalled over RMI
  rmi/      AgentService             — the remote interface
            AgentServiceImpl         — the agent node (thread-pool backed)
            AgentServer              — binds an agent into the RMI registry
            AgentClient              — Experiment 1 client
  agent/    ConcurrencyBenchmarkClient — Experiment 2 client
  clock/    LamportClock             — logical clock (causal event ordering)
            TimeService              — remote interface for a node's physical clock
            TimeServiceImpl          — drift-simulating clock, corrected by offset
            TimeServer               — bootstraps one time node
            BerkeleySyncCoordinator  — Experiment 3 physical sync driver
  election/ ElectionNodeService      — remote interface for leader election RPCs
            RingMessage              — serializable ring election message
            ElectionMetrics          — empirical message and latency tracker
            BullyElectionNode        — Garcia-Molina bully algorithm node
            RingElectionNode         — Chang-Roberts ring algorithm node
            ElectionServer           — bootstraps one election node
            ElectionDemo             — Experiment 4 leader election driver
  replication/ BlackboardEntry        — serializable research finding data model
               ConsistencyLevel       — STRONG vs EVENTUAL consistency enum
               ReplicationService     — remote interface for replicated blackboard
               ReplicationMetrics     — empirical latency and staleness tracker
               ReplicatedBlackboardNode — node implementing strong sync & eventual LWW
               ReplicationServer      — bootstraps one replicated blackboard node
               ReplicationDemo        — Experiment 5 data consistency driver
  balancer/ LoadBalancer             — pluggable routing policy interface
            RoundRobinBalancer       — cycles nodes in fixed order
            WeightedBalancer         — smooth weighted round-robin by capacity
            LeastLoadedBalancer      — real-time queue depth routing over RMI
            LoadBalancerDemo         — Experiment 6 load balancing driver
```

## Requirements

JDK 17 or newer (`javac`, `java` on your `PATH`). No external dependencies.

## Build

On Linux/macOS:
```bash
./backend/build.sh
```

On Windows (CMD or PowerShell):
```cmd
.\backend\build.bat
```
or
```powershell
.\backend\build.ps1
```

Output lands in `backend/build/classes` (git-ignored).


## Run

Start an agent node — it creates the RMI registry and stays in the foreground:

```bash
cd backend
java -cp build/classes agentgrid.rmi.AgentServer [agentId] [port]
```

Defaults: `agentId=agent-1`, `port=1099`. Stop it with Ctrl-C; a shutdown hook
releases the worker thread pool.

### Experiment 1 — single remote call

In a second terminal:

```bash
cd backend
java -cp build/classes agentgrid.rmi.AgentClient [agentId] [port]
```

Pings the agent and executes one `SUMMARIZE` subtask over RMI.

### Experiment 2 — serial vs. thread-pooled execution

```bash
cd backend
java -cp build/classes agentgrid.agent.ConcurrencyBenchmarkClient [agentId] [port] [numSubtasks]
```

Defaults: `agentId=agent-1`, `port=1099`, `numSubtasks=8`. It runs the same
batch twice — once via `execute()` one subtask at a time, then via
`executeBatch()` on the agent's 4-thread pool — and reports the speedup.

Each subtask simulates 150 ms of I/O-bound work, so 8 subtasks on a 4-thread
pool should land near a 4x speedup:

```
Subtasks:             8
Serial execution:     1223 ms
Thread-pooled batch:  309 ms
Speedup:              3.96x
```

### Experiment 3 — clock synchronization

**Part A, Lamport logical clocks.** `AgentServiceImpl` holds a `LamportClock`;
every `execute()` applies the receive rule `max(local, received) + 1` and stamps
the outgoing `Result` with the result. No extra process is needed — start an
agent and run either client, then read the server log:

```
[agent-1] executing Subtask{subtask-0, ...} on pool-1-thread-1 | lamport=9
[agent-1] executing Subtask{subtask-1, ...} on pool-1-thread-2 | lamport=10
```

Sorting a merged multi-node trace by `lamport=` reconstructs a causally legal
execution order.

**Part B, Berkeley physical sync.** Start three nodes with simulated drift, each
in its own terminal:

```bash
cd backend
java -cp build/classes agentgrid.clock.TimeServer node-a 1100 3000
java -cp build/classes agentgrid.clock.TimeServer node-b 1101 -2000
java -cp build/classes agentgrid.clock.TimeServer node-c 1102 500
```

Then run the coordinator:

```bash
java -cp build/classes agentgrid.clock.BerkeleySyncCoordinator \
    node-a:1100 node-b:1101 node-c:1102
```

It polls every node, averages the round-trip-compensated offsets, sends each node
its correction, and re-polls:

```
--- BEFORE SYNC ---
node        reported time    vs average    rtt
node-a      1788841479670    +2500 ms      1 ms
node-b      1788841474671    -2500 ms      1 ms
node-c      1788841477172    +0 ms         0 ms

--- AFTER SYNC ---
node-a      1788841477224    +1 ms         2 ms
node-b      1788841477225    +1 ms         1 ms
node-c      1788841477225    +0 ms         0 ms

--- summary ---
Max clock spread before: 5000 ms
Max clock spread after:  1 ms
```

All three converge on offset `+500 ms`, the mean of `+3000`, `-2000` and `+500`.
Berkeley makes nodes agree with *each other*, not with true time — which is
exactly what comparing cross-node timestamps requires.

### Experiment 4 — leader election

Demonstrates Bully and Ring leader election algorithms on a cluster of 5 nodes under crash-stop failover conditions:

```bash
cd backend
java -cp build/classes agentgrid.election.ElectionDemo
```

The client bootstraps 5 election nodes, elects an initial leader (Node 5), simulates a crash-stop failure of Node 5, and triggers failover election under both algorithms:

```
=================================================
 EXPERIMENT 4 SUMMARY: BULLY VS RING ELECTION
=================================================
Algorithm       Elected Leader  Message Count   Convergence Time
Bully           Node 4          15 msgs         5 ms           
Ring            Node 4          11 msgs         30 ms          
=================================================
```

The **Bully algorithm** achieves lower convergence latency (5 ms) via parallel broadcast to higher-ID nodes, whereas the **Ring algorithm** generates fewer total messages (11 msgs) by circulating candidate tokens sequentially along a logical ring topology.

### Experiment 5 — data consistency & replication

Demonstrates a shared Replicated Agent Blackboard across 3 nodes under runtime-selectable **Strong** and **Eventual** consistency protocols:

```bash
cd backend
java -cp build/classes agentgrid.replication.ReplicationDemo
```

The demonstration client publishes subtask findings under both consistency modes, measures write response latency vs. read staleness, and verifies Last-Write-Wins (LWW) timestamp conflict resolution:

```
=================================================
 EXPERIMENT 5 SUMMARY: CONSISTENCY MODEL COMPARISON
=================================================
Consistency Mode   Avg Write Latency    Stale Reads %      Convergence    
STRONG             11.60 ms             0.0%               0 ms (Instant) 
EVENTUAL           0.20 ms              66.7%              52 ms          
=================================================
```

**Strong Consistency** guarantees zero stale reads across all agent nodes at the cost of higher synchronous write latency (11.60 ms), whereas **Eventual Consistency** provides near-instant write response times (0.20 ms) with background propagation converging across all nodes within ~50 ms.

### Experiment 6 — load balancing

Start three agent nodes with different pool sizes simulating heterogeneous hardware capacities (weak, medium, strong), each in its own terminal:

```bash
cd backend
java -cp build/classes agentgrid.rmi.AgentServer agent-weak 1201 2
java -cp build/classes agentgrid.rmi.AgentServer agent-medium 1202 4
java -cp build/classes agentgrid.rmi.AgentServer agent-strong 1203 6
```

Then run the load balancer demonstration client:

```bash
cd backend
java -cp build/classes agentgrid.balancer.LoadBalancerDemo
```

It dispatches a batch of 18 subtasks under three routing policies (`RoundRobin`, `Weighted`, and `LeastLoaded`) and reports the distribution and makespan:

```
Connected nodes: 3 (pool sizes 2 / 4 / 6, simulating weak / medium / strong hardware)

--- RoundRobin ---
 Distribution: {agent-strong=6, agent-weak=6, agent-medium=6}
 Makespan: 224 ms

--- Weighted ---
 Distribution: {agent-weak=3, agent-strong=9, agent-medium=6}
 Makespan: 181 ms

--- LeastLoaded ---
 Distribution: {agent-strong=6, agent-weak=6, agent-medium=6}
 Makespan: 217 ms
```

Routing subtasks proportional to node processing capacity via `Weighted` balancing reduces overall makespan compared to uniform `RoundRobin` distribution.

## Integrated Cluster

The experiments also run together in one live cluster: 5 node processes, a control plane
that starts and watches them, and a web dashboard. Each node process (`agentgrid.node.NodeMain`)
binds these services in its own RMI registry:

| Binding | Service |
|---|---|
| `agent` | Worker: runs subtasks on a fixed thread pool (Exp 1, 2), dispatching by type to the stage strategies in `agentgrid.orchestrator` |
| `time` | Drifting physical clock (Exp 3) |
| `election-node-<id>` | Bully / Ring leader election with a heartbeat failure detector (Exp 4) |
| `blackboard-node-node-<id>` | Replicated blackboard (Exp 5): `ClusterBlackboardService`, which also serves the submitted `ReplicationService` methods |
| `telemetry` | The node's event buffer, pulled by the control plane |
| `orchestrator` | Job orchestrator; active only on the elected leader |
| `clock` | Berkeley clock-sync coordinator; runs rounds only on the elected leader |

### Configuration
[`backend/cluster.properties`](backend/cluster.properties):
- The initial membership (epoch 1): 5 nodes (IDs 1–5) on ports 1601–1605 (avoiding the standalone demo ports 1099, 1100–1102, 1201–1203, 1301–1305, 1401–1403). Nodes can be added (up to 9) and removed (down to 3) at runtime; an added node gets the next id and port 1600 + id (see Elastic membership).
- Worker pool sizes 2, 4, 4, 6, 6 (heterogeneous hardware). The WEIGHTED routing weight of a node is `node.<id>.weight`, by default its pool size.
- Clock drift offsets +3000, -2000, +500, 0, +1500 ms.
- `simulatedWorkMs=150`: delay each node adds to every subtask. The real stage work is
  sub-millisecond, so without it the thread-pool and load-balancing effects would not show.
  Override with `-Dagentgrid.simulatedWorkMs=<ms>`.
- `eventualLagMs=400`: a **simulated** delay before an EVENTUAL blackboard write is pushed to
  peers. On one machine replication takes about a millisecond, so without it the staleness
  window would be invisible.
- `blackboardAntiEntropyMs=2000`: how often each replica pulls newer records from a random peer.
- `berkeleyIntervalMs=30000`: how often the leader runs a Berkeley round (0 disables the
  periodic round).
- `collectivePortOffset=100`, `collectiveTimeoutMs=3000`: each node's Exp 9 collective port is its RMI port
  + 100 (1701–1705), and a collective fails, naming the rank, when a rank does not answer within 3 s.

- `pythonCommand=python`: the Python interpreter the control plane runs Exp 7 (MapReduce) with.
  It needs pyspark (`mapreduce/requirements.txt`) and Java 17+. On this machine `python` is a
  Python 3.14 without pyspark, so start the control plane with
  `-Dagentgrid.pythonCommand=D:\anaconda3\python.exe` (or install pyspark for `python`).

Each of these can also be overridden with `-Dagentgrid.<name>=<value>`.

The document corpus that jobs search is `backend/src/main/resources/corpus/` (40 short
documents listed in `index.txt`); the build copies it onto the classpath.

### Build
From the repo root:
```bash
# Linux/macOS
./backend/build.sh

# Windows
.\backend\build.bat
# or
.\backend\build.ps1
```
Output goes to `backend/build/classes` (compiled classes plus the corpus).

### Start the control plane
```bash
cd backend
java -cp build/classes agentgrid.control.ControlPlaneMain
```
It starts nodes 1–5 as child JVM processes (each with `-Xms16m -Xmx256m`), polls them over
RMI, pulls their events, and serves the API and the dashboard. `Ctrl-C` stops the nodes too
(a shutdown hook). A control plane that is force-killed (Task Manager "End task",
`taskkill /F`, `Stop-Process -Force`) cannot run that hook, so its nodes keep running and hold
their ports (1601–1605, and 1600 + id for nodes added at runtime); stop them with
`backend\stop-cluster.ps1`, which stops only `NodeMain` and `ControlPlaneMain` java processes
(added nodes included) and checks ports 8080 and 1601–1699. The shutdown hook stops every node
process the control plane started, added ones included. Node output is appended to
`backend/build/logs/node-<id>.log` (one header per start).

Dashboard: **http://127.0.0.1:8080/** (the server binds to 127.0.0.1 only).

The older `agentgrid.node.ClusterLauncher` still starts the nodes of `cluster.properties`
without a control plane and prints an RMI health table; it cannot add or remove nodes.

### HTTP API

POST endpoints require `Content-Type: application/json` (otherwise 415); an `Origin` header,
if present, must be `http://127.0.0.1:8080` or `http://localhost:8080` (otherwise 403);
malformed or invalid input returns 400. Errors are `{"error": "...", "status": N}`.

| Method | Path | Body | Returns |
|---|---|---|---|
| GET | `/api/modules` | | Dashboard modules: exp1–exp9 |
| GET | `/api/cluster` | | Per current member: `id, port, up, poolSize, weight, epoch` (the membership epoch it reports), `queueDepth, lamport, clockOffsetMs, bindings, leaderView, isLeader` |
| GET | `/api/cluster?view=membership` | | `{epoch, size, quorum, members: [{id, port, poolSize, weight}], nextId, minMembers, maxMembers, changeInProgress, processes, nodes}` (`nodes` = the array above) |
| POST | `/api/cluster/nodes` | `{"poolSize": 4, "weight": 4}` (both optional) | Adds a node (see Elastic membership): `{node, port, poolSize, weight, readyMs, snapshotSource, membership, pushed, clockRound, totalMs}`; 409 at 9 members or during another change; 500 with the reason if the node could not be started, came up or pulled its snapshot (membership unchanged) |
| POST | `/api/cluster/nodes/{id}/remove` | `{}` | Removes a member: `{node, wasLive, wasLeader, membership, pushed}`; 404 for a non-member; 409 below 3 members, while a job runs, if a live node's removal would leave fewer live members than the new quorum, or during another change |
| GET | `/api/events?since=<seq>` | | Merged event log (control-plane and node events) after `seq`: `seq, type, node, details, lamport, nodeWallMs, trueMs`, plus `nodeSeq` and `fields` for node events (and `fields` for `MEMBERSHIP_CHANGED`, `NODE_ADDED`, `NODE_REMOVED`, `NODE_ADD_FAILED`) |
| GET | `/api/stream` | | Server-sent events: `cluster`, `membership` and `election` snapshots every 1 s (0.2 s during elections and jobs), and every new `event` |
| POST | `/api/nodes/{id}/kill` | `{}` | Kills the node process; 404 for a non-member (e.g. a removed id) |
| POST | `/api/nodes/{id}/restart` | `{}` | Restarts the node process with the current membership; 404 for a non-member |
| POST | `/api/rmi/invoke` | `{"node": 1, "type": "SUMMARIZE", "count": 12, "doc": "doc-07"}` | Runs `count` concurrent `execute()` calls on one node (Exp 1, 2). SUMMARIZE summarises corpus document `doc` (default: the first one; 400 for an unknown id) |
| GET | `/api/corpus` | | `[{id, title}]` of the bundled documents |
| POST | `/api/clock/drift` | `{"node": 1, "deltaMs": 5000}` | Shifts a node's clock (Exp 3) |
| GET | `/api/clock/drift` | | Per node: `node, up, configuredDriftMs, offsetMs` (current offset against the control plane's clock, RTT-compensated) |
| POST | `/api/clock/sync` | `{}` | Asks the elected leader to run one Berkeley round now: `spreadBefore, spreadAfter, averageOffset, nodeCount, corrections, coordinator, skipped`; **409** while no leader is agreed |
| GET | `/api/clock/auto` | | `{enabled, berkeleyIntervalMs}` |
| POST | `/api/clock/auto` | `{"enabled": false}` | Turns the leader's periodic round and the rejoin sync off (or on) on every UP node and for restarted nodes; the manual round still works |
| POST | `/api/blackboard/write` | `{"node": 2, "key": "k", "value": "v", "mode": "STRONG"}` (or `EVENTUAL`) | Writes through that node: `status` (`STORED, STORED_DEGRADED, REFUSED, FAILED_PARTIAL` for STRONG, `ACCEPTED` for EVENTUAL), `stored, timestamp, writer, liveAtCheck, acked, failed, latencyMs, message`. Key 1–128 characters, value at most 1024, else 400. Lengths are Java `String.length()` (UTF-16 code units, so a character outside the BMP counts as 2). The control plane checks them and the node checks them again; BlackboardVerifier B12 tests 128/129/200-char keys and 1024/1025-char values |
| GET | `/api/blackboard/read?node=2&key=k` | | That replica's record: `ready, clockSynced, found, value, timestamp, writer, version, verdict` (`FRESH, STALE, NOT_READY, UNCHECKED`) |
| GET | `/api/blackboard?prefix=` | | Every replica's records under the prefix (each marked `stale` if another replica holds a newer one), with `ready, clockSynced, snapshotSource, pendingPropagation, missingKeys` per node |
| GET | `/api/blackboard/metrics` | | STRONG stored / degraded / refused / failed-partial counts and median write latency; EVENTUAL accepted, pending propagation, retries, anti-entropy and median latency; stale / fresh / not-ready reads |
| GET | `/api/election` | | `algorithm, views, agreed, leaderId, backupId, lastEpisode {startTrueMs, endTrueMs, convergenceMs, messages, byKind, leaderId, initiators, table}` |
| POST | `/api/election/algorithm` | `{"name": "BULLY"}` or `"RING"` | Switches the algorithm on all UP nodes |
| POST | `/api/election/start` | `{"node": 1}` | Starts an election on one node |
| POST | `/api/jobs` | `{"query": "...", "policy": "ROUND_ROBIN", "consistency": "EVENTUAL", "retrieval": "SCAN"}` (policy `LEAST_LOADED` / `WEIGHTED`; consistency `STRONG`, default `EVENTUAL`; retrieval `INDEX`, default `SCAN`) | `{jobId, leader, consistency, retrieval, status}`; 503 if no leader is agreed; for `INDEX`, 409 if there is no MapReduce index ("run MapReduce first") or it is stale ("re-run MapReduce") |
| GET | `/api/jobs` | | Recent jobs: `jobId, query, policy, status, leaderNode, makespanMs, subtasks, completedSubtasks, retrieval {mode, docsTotal, docsTouched, retrieveSubtasks, retrievalMs}, ...` |
| GET | `/api/jobs/{id}` | | The task graph: status (`QUEUED, RUNNING, COMPLETE, FAILED, ORPHANED, RECOVERED`), `leaderNode` (the first leader) and `currentLeader` (the node running it now), `recovered`, `replicated`, answer, per-node subtask counts and peak queue depth, `retrieval`, and every stage's subtasks with node, status and dispatch / result / completion Lamport times |
| POST | `/api/mapreduce/run` | `{"job": "index", "partitions": 4, "compare": false}` (job `wordcount`; partitions 1–16) | Runs `mapreduce/exp7_mapreduce.py --json` (see MapReduce below) and returns its JSON: `job, partitions, stages {mapPairs, pairsAfterCombine, reduceKeys, shuffleRecordsWithCombiner, shuffleRecordsWithout, ...}, timings {local4Ms, local1Ms, speedup}, topTerms, topDocsForQuery, verified, checks, docCount, builtAt, fingerprint`, plus `wallMs` and the reloaded `index`. 409 while a run is in progress; 503 with the reason (missing interpreter, missing pyspark, 300 s timeout, non-zero exit; with the last 20 stderr lines) |
| GET | `/api/mapreduce/last` | | The last successful run's result; 404 if none since the control plane started |
| GET | `/api/mapreduce/status` | | `{running, current {job, partitions, compare, elapsedMs}, hasLast, lastFailure, pythonCommand, script, timeoutSeconds, index}` |
| GET | `/api/mapreduce/index` | | `{available, fingerprintOk, stale, usable, reason, terms, postings, documents, builtAt, corpusDocuments, fingerprint, corpusFingerprint, file}` |
| POST | `/api/mapreduce/index/test-hook` | `{"corruptFingerprint": true}` | Test only (404 unless the control plane runs with `-Dagentgrid.testHooks=true`): replaces the expected corpus fingerprint so the index reads as stale; `false` restores it |
| GET | `/api/replication` | | `{enabled, primary, backup, term, lastSeq, backupLastSeq, lag, noBackup, ready, warning, nodes: [{node, up, role, term, lastSeq, logEvents, staleRejected, enabled}], staleRejected, recoveredJobs}`; `warning` is `no backup: ...`, `replication is OFF: ...`, `no agreed leader`, or null |
| POST | `/api/replication/toggle` | `{"enabled": false}` | Switches primary-backup replication on or off for the whole cluster (pushed to every node, passed to nodes started later); returns the new view |
| GET | `/api/replication/log?node=N&from=S` | | One node's replicated log as summaries `{seq, term, jobId, type, subtaskId, digest}` with seq above `from`, plus its `term` and `lastSeq`; the digest covers the whole event, so two replicas hold the same log iff their lists are equal |
| POST | `/api/replication/test/stale` | `{"node": 4, "term": 2, "event": true}` | Test only (404 unless `-Dagentgrid.testHooks=true`): plays a stale primary against one node, sending its orchestrator a replicated event and its worker a subtask carrying that (lower) term; returns what each answered. `"event": false` tests the dispatch only |
| GET | `/api/collective` | | The communicator (Exp 9): `{epoch, size, root, rootNode, leader, timeoutMs, portOffset, maxMessageChars, maxInts, ranks: [{rank, node, port, rmiPort, up, role}], running}` |
| POST | `/api/collective/broadcast`, `/scatter`, `/gather`, `/demo` | see Collective communication (Exp 9) | Per-rank results, `trace`, `messages, bytesSent, rounds, timeMs, totalMs, lamport`; 400 for invalid input, 409 while another collective runs, after a membership change or for chunks a rank does not hold, 503 naming the rank that did not respond |

### Jobs
A job runs on the orchestrator of the elected leader: RETRIEVE (keyword match, fanned out
over 20 corpus chunks) -> RANK (TF-IDF) -> SUMMARIZE (extractive, fanned out over the 20
best documents) -> SYNTHESIZE (one merged answer, each sentence citing its document).
Each stage starts after the previous one completes. Subtasks are routed with the submitted
`agentgrid.balancer` policies. If the leader is killed mid-job with primary-backup replication ON
(the default, see Exp 8) the backup resumes the job and it ends RECOVERED. With replication OFF, or if
the job had no backup, the job becomes ORPHANED (its completed subtasks are kept) and the new leader
accepts new jobs; it is not resumed.

Each worker that runs a SUMMARIZE subtask posts its finding to its own blackboard replica as
`job/<jobId>/finding/<subtaskId>`, and the SYNTHESIZE worker posts the answer as
`job/<jobId>/answer`, under the job's `consistency`. The job view shows every finding as stored
or not stored; a refused or failed write never fails the job. The leader records one
`BLACKBOARD_JOB_FINDINGS` event per job (no event per write).

### MapReduce (Exp 7) and INDEX retrieval
The dashboard tab "MapReduce" (or `POST /api/mapreduce/run`) runs `mapreduce/exp7_mapreduce.py --json`
on the control-plane host with `pythonCommand`, in the repository root, with a 300 s limit. **Spark runs
in local mode on the control-plane machine, not on the cluster nodes.**
- `--json` runs one job (`index` or `wordcount`) once at `local[4]` and, with `compare`, once more at
  `local[1]`. The script prints exactly one JSON line on stdout; everything else goes to stderr.
  Every Spark result is checked against plain Python (`verified`). Files in `mapreduce/out/` are
  written only when the result is verified.
- One run at a time. The events are `MAPREDUCE_STARTED`, `MAPREDUCE_COMPLETE` (job, ms, verified) and
  `MAPREDUCE_FAILED` (reason).
- An index run took 35 s from the command line and 54 s through the control plane while the 5 nodes
  were running (on Windows, PySpark starts a Python worker per task; most of the time is Spark start-up).

`mapreduce/out/inverted_index.json` holds the postings (`index`: term -> `{df, postings [[docId, tf], ...]}`),
`N`, every document's length (`docLengths`), `builtAt`, and a corpus `fingerprint`: the document count
and a SHA-256 over `docId:length\n` for every document in `index.txt` order. The control plane's
`IndexStore` loads the file at start and after every successful run, and computes the same fingerprint
over its own corpus (the one on the classpath, which the nodes use).

**Retrieval option.** `POST /api/jobs` takes `"retrieval": "SCAN"` (default, unchanged) or `"INDEX"`:
- SCAN dispatches all 20 RETRIEVE chunks and each worker reads every document in its chunk.
- INDEX tokenizes the query with the backend's own `Corpus.terms`, looks the terms up in the index,
  and restricts RETRIEVE to the documents that contain at least one query term. Only the chunks that
  hold such a document are dispatched, and the payload names the documents the worker reads.
- RANK, SUMMARIZE, SYNTHESIZE, the findings on the blackboard, the JobDirectory and failover are the
  same in both modes. RANK scores the same candidate set with the same idf, so the ranking and the
  answer are identical. For the sample question both modes ranked
  doc-09, doc-07, doc-26, doc-08, doc-30 first and cited the same documents (MapReduceVerifier MR4).
- Every job records `retrieval {mode, docsTotal, docsTouched, retrieveSubtasks, retrievalMs}`. These
  appear in the Job Console summary ("docs touched X/Y") and in the `JOB_SUBMITTED` / `JOB_COMPLETED`
  event text. For the sample question: SCAN 40/40 documents and 20 RETRIEVE subtasks; INDEX 22/40 and 16.

**Stale-index guard.** INDEX is refused with 409 when there is no index ("run MapReduce first") or
when the index's fingerprint differs from the control plane's corpus ("the MapReduce index is stale,
re-run MapReduce"). The Job Console's INDEX option is disabled, with that reason as a hint, until
`/api/mapreduce/index` reports `usable`. It updates on every `MAPREDUCE_*` event and every 10 s.

**Limitation.** With 40 short documents the saving shows in documents touched (22 instead of 40) and
RETRIEVE subtasks (16 instead of 20), not in time: the job's makespan is dominated by the simulated
150 ms per subtask and the stage barriers. The index is built on one host and is not partitioned
across the nodes.

### Replicated blackboard (Exp 5)
Every node keeps a replica of `{key, value, timestamp, writerNodeId, version}` records. The
dashboard tab "Blackboard" shows every replica side by side.

- **STRONG** is synchronous replication to every *live* replica, gated by a live quorum of
  floor(n/2)+1 of the n members in the writing node's current membership epoch (3 of 5 by
  default, 4 of 6, 5 of 9). The node that takes the write probes which members are live
  (in parallel, 500 ms cap); with fewer than the quorum it refuses and writes nothing anywhere.
  Otherwise it writes to every live member and returns after they acknowledge. Dead nodes catch
  up when they restart. A node can die between the live check and the write, so the result
  can also be `STORED_DEGRADED` (at least a quorum acknowledged) or `FAILED_PARTIAL` (fewer).
  **This is not consensus and not linearizable**: two concurrent writers of the same key are
  ordered only by last-writer-wins.
- **EVENTUAL** acknowledges after the local write and pushes to peers after the simulated
  `eventualLagMs`, retrying peers that are down; periodic anti-entropy pulls newer records
  from peers, so replicas converge without operator action.
- **Last-writer-wins**: the higher timestamp wins; equal timestamps go to the higher writer
  node id. Timestamps come from the writer node's Berkeley-corrected clock (physical time +
  drift + corrections), made monotonic per node.
- **Clocks**: the elected leader runs Berkeley rounds (every `berkeleyIntervalMs`, when asked by
  `/api/clock/sync`, and when a restarted node rejoins). Berkeley converges the nodes to the
  **mean of their offsets, not to true time**: that is enough for LWW, which needs the nodes to
  agree with each other, not with real time. With auto-sync off, skewed clocks make LWW keep
  the write with the larger stamp even if it happened earlier (BlackboardVerifier B7 shows this).
- **Restart**: a restarted replica pulls the records its live peers hold and asks the leader
  for a clock round before it reports `ready`; until then its reads are answered `NOT_READY`.
  It waits at most 3 s for the clock (then `clockSynced=false`), and not at all when
  auto-sync is off. If it goes ready unsynced, it keeps asking the leader in the background
  (short backoff) until a round includes it or auto-sync is turned off. A restart used to
  miss its rejoin sync sometimes (8 of 36 restarts in `RejoinRepro`); the cause and fix are in
  FIXES.md, Fix V. BlackboardVerifier B4/B5 now fail unless the restarted node's clock is within
  100 ms of the others within 5 s of ready.

#### Cost of STRONG vs EVENTUAL (BlackboardVerifier B10)
These numbers come from one BlackboardVerifier run and one JobFlakeCheck run, each against a
fresh control plane, on the build with elastic membership (5 members, loopback RMI stubs). B10
runs 3 STRONG and 3 EVENTUAL jobs, interleaved (WEIGHTED policy, 42 subtasks, 20 findings and
1 answer each); JobFlakeCheck adds a second sample of 15 jobs per mode.

| | STRONG min / median / max | EVENTUAL min / median / max |
|---|---|---|
| Direct write via the API (B1/B2, 20 writes each) | 2 / 4 / 151 ms | 0 / 0 / 8 ms |
| Finding write inside a job (per-job median, 3 jobs each) | 13 / 31 / 80 ms | 0 / 0 / 0 ms |
| Job makespan, B10 (3 jobs each) | 840 / 1151 / 1179 ms | 702 / 787 / 1019 ms |
| Job makespan, JobFlakeCheck (15 jobs each) | 648 / 685 / 2232 ms | 643 / 658 / 798 ms |

- **What the numbers show:**
  - A STRONG write waits for every live replica, while an EVENTUAL write returns after the local write.
  - Inside a job, a STRONG finding write competes with the job's own RMI traffic, so it costs more than a direct write.
  - EVENTUAL's price is staleness instead: in B2, 80 of 100 immediate reads were stale, and replicas agreed after 433 / 457 / 556 ms (min / median / max).
  - The makespan ranges overlap: in the larger JobFlakeCheck sample STRONG costs about 27 ms per job at the median, far less than the run-to-run spread.
- **Caveats:**
  - Measured on one Windows machine running all 5 nodes and the control plane, with simulated work (150 ms per subtask) and a simulated 400 ms EVENTUAL lag (`eventualLagMs`).
  - Machine load causes visible run-to-run variance: the first job of a series is often the slowest (1151 ms in B10, 2232 ms in JobFlakeCheck), and one direct STRONG write took 151 ms against a median of 4 ms.
  - Treat these as orders of magnitude, not benchmarks.

### Elastic membership (add / remove nodes)
The cluster's membership is an immutable `{epoch, members}` object (`node/Membership`,
`node/MemberSpec {id, port = 1600 + id, poolSize, weight}`). The control plane
(`control/MembershipManager`) is the only authority: it creates every new epoch and pushes it
to the nodes, which apply a membership only if its epoch is higher (`NodeAgent.applyMembership`).
Everything that used to assume nodes 1–5 reads the current membership: the monitor and
`/api/cluster`, Bully's higher/lower ids and the ring order (ascending ids), the failure
detector's leader, the blackboard's replica set and STRONG quorum, the Berkeley round, the
orchestrator's workers and the WEIGHTED weights, the election tracker, and the dashboard.

- **Add** (`POST /api/cluster/nodes {"poolSize": 4, "weight": 4}`, both optional, defaults 4 / 4):
  1. allocate the next id (ids are never reused, not even after a failed add); 409 if the cluster already has 9 members;
  2. start the process with a *joining* view (current members plus itself, current epoch) and wait until it is UP;
  3. wait until its blackboard replica is ready (snapshot pulled from the live peers);
  4. only then bump the epoch and push it to every live member; the new node's first election
     waits for this (its boot gate is held), and as the highest id it becomes the Bully leader;
  5. ask the agreed leader for a Berkeley round over the new membership.
  Any failure before step 4 (port busy, no UP within 20 s, snapshot failed) stops the new
  process, leaves the membership unchanged, records `NODE_ADD_FAILED` and returns 500 with the reason.
- **Remove** (`POST /api/cluster/nodes/{id}/remove`): 404 for a non-member; 409 if fewer than 3
  members would remain, while a job is QUEUED or RUNNING, or if removing a *live* node would
  leave fewer live members than the quorum of the new membership (removing a dead node is not
  limited by this rule). The new epoch is pushed to the remaining members first, then the process
  is stopped and dropped from the monitor. If it was the leader, the members' failure detectors
  elect a new one as for any dead leader.
- Membership changes are serialised: one at a time, a concurrent request gets 409.
- Events: `NODE_ADDED`, `NODE_REMOVED`, `NODE_ADD_FAILED`, and `MEMBERSHIP_CHANGED` with fields
  `{epoch, members, quorum}`.
- `GET /api/cluster?view=membership` and the `membership` SSE event give the epoch, quorum,
  member specs, next id, limits and the tracked node processes; `/api/cluster` entries carry each
  node's `weight` and the `epoch` it reports. Kill and Restart are unchanged; on a removed id they return 404.
- A restarted node is launched with the current membership, and the monitor re-pushes the
  membership to any node that reports an older epoch.

**Limitations:**
- Membership is not persisted: a control-plane restart starts again from `cluster.properties` (5 nodes).
- No epoch fencing on replication: a replica applies a record whatever epoch the writer held.
  A STRONG write checks the quorum of the writer's current epoch only; anti-entropy covers gaps.
- Nodes added at runtime have no configured clock drift (`configuredDriftMs` 0).
- `ClusterLauncher` (the standalone Phase 1 launcher) has no control plane and cannot add or remove nodes.
- Node RMI stubs advertise `127.0.0.1` (`java.rmi.server.hostname`): all nodes run on this machine.
  With the default (the LAN IP), a Wi-Fi address change during a run broke every node-to-node call (FIXES.md, Fix W).

### Primary-backup replication of job state (Exp 8)
**Problem.** A job's live state (its task graph, the subtasks in flight, the queue) existed only in the
leader's orchestrator, so a leader that died took its running jobs with it: they were marked ORPHANED.

**What it does.** The elected leader is the **primary**. The **backup** is the live member with the next
highest id below it (the node the dashboard already shows as BACKUP); it is recomputed whenever the
membership or the live set changes. Every job state change is an event `{seq, term, jobId, type, payload}`
with type `JOB_SUBMITTED`, `SUBTASK_DISPATCHED`, `SUBTASK_DONE`, `JOB_DONE` or `JOB_FAILED`; `seq` grows
by 1 and `term` is the term of the primary that wrote it. The primary appends the event to its log, sends it
to the backup over RMI and **waits for the ack before the job moves on** (a dispatch is replicated before
the subtask leaves, a result before it is applied). With no live backup the primary carries on
unreplicated and the API and dashboard show `no backup`; it never blocks the cluster.

**Failover.** When the failure detector declares the primary dead and a new leader is elected, that node
(normally the backup): (1) asks every live member for its log position and, if a peer's log is longer (last
term, then last seq), copies it; (2) starts a new term, the highest term any peer reported plus one;
(3) replays its log to rebuild the jobs; (4) queues the unfinished ones, marked recovered, ahead of new
work. The runner resumes each job: stages whose subtasks all completed are skipped, subtasks that were
DISPATCHED but not DONE (or never dispatched) are sent again. A job that finished after a failover ends
**RECOVERED** instead of COMPLETE (a distinct colour in the dashboard); `JOB_RECOVERING` is recorded by the
new primary. The control plane follows the job to the new primary and does not orphan it; if no new primary
reports the job within 20 s it is orphaned after all.

**Fencing.** Every replicated event, snapshot and dispatch carries the term. A node that sees a term below
the highest it has seen refuses it (`STALE_TERM`; a worker refuses a dispatch with
`StaleTermException`), and a primary that is refused, or that receives a higher term, steps down and leaves
its running jobs to the new primary. A node that is elected after a restart has an empty log: it copies the
current one before it serves, so nothing is lost and its term is higher than any before.

**Idempotence.** A subtask re-dispatched after a failover is keyed by `jobId + subtaskId`: the replay and the
runner ignore a second `SUBTASK_DONE` for a subtask that is already COMPLETE, so it is counted once
(its blackboard finding is written under the same key, last-writer-wins).

**Toggle.** `primaryBackup` (default ON), from the dashboard's Replication tab, `POST /api/replication/toggle`
or `-Dagentgrid.primaryBackup=false` on the control plane. OFF is the old behaviour exactly: no log, no
fencing (term 0 is never refused), a job whose leader dies is ORPHANED. Switching OFF clears the logs. A job
that started while replication was off is not replicated and cannot be recovered. JobVerifier J4,
BlackboardVerifier B9 and `JobFlakeCheck orphan` test the ORPHANED path, so each switches replication off
for its kill and restores it afterwards.

**Demo.** Open the dashboard's **Replication** tab: primary and backup cards, term, primary and backup last
seq, lag, the switch and a live log of replication events. Submit a job in the Job Console and click Kill
on the leader's card while it runs: the job ends RECOVERED, run by the new primary, and the tab shows the new
term and a new backup. Switch replication off and repeat: the job ends ORPHANED. From a shell:
```bash
curl http://127.0.0.1:8080/api/replication
curl -X POST -H "Content-Type: application/json" -d '{"enabled": false}' http://127.0.0.1:8080/api/replication/toggle
java -Dagentgrid.testHooks=true -cp build/classes agentgrid.control.ControlPlaneMain   # then, in another shell:
java -cp build/classes agentgrid.control.ReplicationVerifier                            # R1-R6
```

**Cost (measured: 10 interleaved runs per mode, 5 nodes on one machine, replication toggled between runs).** A
42-subtask job logs 86 events, each one RMI round trip to the backup. Median makespan with replication OFF vs ON:
WEIGHTED 688 ms vs 720 ms (+31 ms, +5%), ROUND_ROBIN 1007 ms vs 1034 ms (+27 ms, +3%). That is on localhost; events are
sent one at a time, so over a real network the cost grows with 86 times the round-trip time.

**Limits:**
- **One backup, not a quorum.** The log is on the primary and one backup. If both die the jobs are lost; there
  is no majority agreement on the log, and a backup that is lagging when the primary dies resumes from what it has
  (a subtask may be re-run).
- **The log is in memory.** It survives the death of the primary or the backup, not of both, and not a control-plane
  restart (jobs the control plane has not seen finish are lost with it). It is never trimmed, so a long-running
  cluster grows (about 90 events per job).
- **Synchronous replication adds latency:** one RMI round trip per event, about 2 events per subtask (see Cost).
- **Bully re-elects the highest id.** The old primary, once restarted, is elected again; it first copies the log from
  the interim primary and takes a newer term, and the interim primary becomes its backup. It does not stay a plain
  member. The stale-write checks are therefore made with a test hook (a lower-term event and dispatch), not with
  a live zombie primary; the only failures injected are process kills, never a network partition.
- **Workers learn terms from dispatches.** A freshly restarted worker has not seen a term yet and accepts a lower one
  until the first dispatch of the current primary reaches it.
- **Only the orchestrator's job state is replicated.** The blackboard keeps its own replication (Exp 5); a job's
  workers, the control plane's directory and the election are not replicated by this.
- A job needs a backup that has acknowledged its events to be recoverable; a job submitted while there is no
  backup runs, but is ORPHANED if its primary dies.
- A control-plane monitor cycle is up to 1 s: a new primary appears in `/api/replication` once the election has
  agreed (about 2 s after a kill), and the job finishes as soon as it has been re-run.

**Tested (ReplicationVerifier R1-R6, plus by hand):** identical logs on primary and backup after a job; a leader
killed mid-job with a SCAN job, with an INDEX + STRONG job (22/40 documents, 21/21 findings stored) and with a second
job still QUEUED behind it (all RECOVERED, same answer as a run without a failure, no subtask counted twice); the
OFF path (ORPHANED); the old primary restarting; killing the backup, then every node but the primary (the "no backup"
warning, the job still completes), then bringing them back; adding a node and removing the backup and the primary;
the dashboard tab (switch, a kill mid-job with replication ON and OFF, RECOVERED and ORPHANED side by side).

**Not tested:**
- A network partition or a primary that is paused but alive (only process kills were injected); a stale primary is
  exercised through the test hook only.
- Both the primary and the backup dying, and a second failure while a recovered job is still running.
- The RING election algorithm with replication (the backup rule is the same, but only BULLY was run).
- More than 9 members, or logs of many thousands of events (the snapshot sent to a new backup grows with the log).
- Several concurrent control-plane clients toggling the switch at once.

### Collective communication (Exp 9)
**What.** MPI-style Broadcast, Scatter and Gather between the node processes over plain TCP (`java.net`
sockets, no MPI library), code in `backend/src/main/java/agentgrid/collective/`. The communicator is the
current membership sorted by node id: rank = position (0..n-1), size = n, plus the membership epoch. A member
that is down stays in it, as in MPI, and fails the operation naming its rank. The root defaults to the leader's
rank; any rank can be chosen. Each node listens on its RMI port + `collectivePortOffset` (1701-1705, loopback
only). The control plane plays `mpirun`: it connects to every rank, sends each its START frame (its rank, the
root, the communicator), and the ranks then move the data directly between themselves. Frames are
length-prefixed: `opId, op, epoch, srcRank, dstRank, seq (the round), payloadLength, payload`; each connection
has one reader thread that blocks on the socket.

| MPI | Endpoint | What the ranks do |
|---|---|---|
| `MPI_COMM_WORLD`, rank, size | `GET /api/collective` | `{epoch, size, root, rootNode, leader, timeoutMs, portOffset, ranks: [{rank, node, port, rmiPort, up, role}], running}` |
| `MPI_Bcast` | `POST /api/collective/broadcast {"root": 4, "message": "hi", "algorithm": "TREE"}` | LINEAR: the root sends to each rank in turn, n-1 messages in n-1 rounds. TREE (default): binomial tree, in round k every rank that has the data sends it to the rank 2^(k-1) further on, n-1 messages in ceil(log2 n) rounds. Each rank reports the SHA-256 of the bytes it holds |
| `MPI_Scatter` | `POST /api/collective/scatter {"root": 4, "data": [1, 2, 3, ...]}` | The root sends chunk i (equal chunks, in rank order) to rank i. A length that does not divide by n is refused with 400 naming the remainder; nothing is padded |
| `MPI_Gather` | `POST /api/collective/gather {"root": 4, "randomDelay": true}` | Each rank sends the chunk it holds to the root, which puts chunk i at position i whatever the arrival order. `randomDelay` makes each rank wait a shuffled 0, 60, 120, ... ms; the response has the arrival order next to the rank order |
| scatter, compute, gather | `POST /api/collective/demo {"root": 4}` | Scatter 1..N (N = 4 x size), every rank squares its chunk, gather; the sum of squares is checked against N(N+1)(2N+1)/6 |

`root` is optional everywhere. Each operation returns per-rank results, a message trace (from and to rank and
node, bytes, round, sent at, received at: ms after the START frames went out, from `System.nanoTime()`, which
all JVMs on one machine share) and totals (messages, bytes, rounds, time from the first send to the last
receipt). It is recorded as a `COLLECTIVE_BCAST`, `COLLECTIVE_SCATTER` or `COLLECTIVE_GATHER` event with the
control plane's Lamport time, failures included. Input limits: message 1-1024 characters, at most 1000 ints,
root 0..n-1, else 400. One operation runs at a time (409 while another runs).

**Failure.** A rank that is down fails the operation before any rank starts it: 503 `rank r (node X) did not
respond` within about 5 ms here (connect pre-check, 300 ms at most). A rank whose process dies mid-operation fails it as
soon as its connection drops; a rank that stays silent fails it after `collectiveTimeoutMs` (3 s) plus at most
0.5 s for the other ranks to report. A membership change during the operation fails it with 409. Chunks from an
older epoch, or a restarted rank holding none, are not gathered (409, "run Scatter first").

**Run.** From `backend/`, with the control plane running:
```bash
java -cp build/classes agentgrid.collective.CollectiveDemo     # terminal demo (about 1 s), ends with VERIFIED lines
java -cp build/classes agentgrid.control.CollectiveVerifier    # C1-C9, against a fresh control plane
```
Dashboard: the **Collectives** tab (communicator table, root selector, Broadcast with LINEAR / TREE and their
comparison, Scatter, Gather with the delay switch, the scatter-compute-gather demo, every message trace).

**Limits.**
- Sockets between processes on one machine, not real MPI: no MPI runtime, no network between machines, and the
  trace times rely on the one clock those processes share.
- The root is the bottleneck of Scatter and Gather: it sends or receives all n-1 messages itself (linear
  algorithms only; the tree is for Broadcast).
- No Scatterv / Gatherv (unequal chunks), no Reduce, Allreduce, Allgather or Alltoall, no non-blocking
  collectives, one communicator (all members), one operation at a time.
- A message costs well under a millisecond here, so TREE's fewer rounds save little time (about 1.2 ms vs 1.4 ms
  for 5 ranks); the first operation through a fresh JVM also pays for class loading.
- What a rank holds lives in that process's memory: a restarted node holds nothing until the next Scatter.

### Verifiers
With the control plane running, from `backend/`:
```bash
java -cp build/classes agentgrid.control.ElectionVerifier   # Exp 4: election (V1-V6)
java -cp build/classes agentgrid.control.JobVerifier        # Phase 4: jobs and routing (J1-J5)
java -cp build/classes agentgrid.control.BlackboardVerifier # Phase 5: replicated blackboard (B1-B12)
java -cp build/classes agentgrid.control.MembershipVerifier # elastic membership: add / remove nodes (M1-M13)
java -cp build/classes agentgrid.control.MapReduceVerifier  # Exp 7: MapReduce runs and INDEX retrieval (MR1-MR6)
java -cp build/classes agentgrid.control.ReplicationVerifier # Exp 8: primary-backup replication (R1-R6; needs -Dagentgrid.testHooks=true)
java -cp build/classes agentgrid.control.CollectiveVerifier # Exp 9: Broadcast, Scatter, Gather (C1-C9)
java -cp build/classes agentgrid.control.RestartStress      # restart all 5 nodes at once, 10 cycles
java -cp build/classes agentgrid.control.JobFlakeCheck      # 30 jobs, no false ORPHANED
java -cp build/classes agentgrid.control.JobFlakeCheck orphan 5  # kill the leader mid-job: time to JOB_ORPHANED
java -cp build/classes agentgrid.control.RejoinRepro 18     # restart node 2 18 times: clock rejoins every time
```
They drive the HTTP API (JobVerifier and BlackboardVerifier also read the bundled corpus to
check answer sentences, and BlackboardVerifier checks the merge function in-process), print
every measured number, end with a PASS/FAIL table, and exit non-zero if a check fails.
ElectionVerifier and BlackboardVerifier kill and restart nodes and take several minutes.
ElectionVerifier leaves the election algorithm set to RING when it finishes.
The verifiers other than MembershipVerifier expect the default 5 members (ids 1-5); run them
against a fresh control plane. MembershipVerifier also needs a fresh one, and leaves a 5-member
cluster whose ids are not 1-5 (it removes nodes 4 and 5 to test the minimum-size rule, and ids
are never reused).
ReplicationVerifier needs `-Dagentgrid.testHooks=true` on the control plane (R4 plays a stale primary); R6 adds and removes nodes and leaves a 4-member cluster.
CollectiveVerifier kills and restarts a node (C7) and adds and removes nodes (C8, run last), so it leaves a
5-member cluster whose ids are not 1-5.
MapReduceVerifier needs a control plane whose `pythonCommand` has pyspark. MR3 runs only when the control
plane was started without `mapreduce/out/inverted_index.json`, and MR5 only with `-Dagentgrid.testHooks=true`;
otherwise they are reported as SKIP with the reason:
```bash
java "-Dagentgrid.pythonCommand=D:\anaconda3\python.exe" -Dagentgrid.testHooks=true -cp build/classes agentgrid.control.ControlPlaneMain
```

**Not yet tested:**
- The shutdown hook when the control plane's console window is closed. Ctrl+C stops all 5 nodes; a forced kill of the control plane leaves them running (stop them with `stop-cluster.ps1`).
- Linux, and `build.sh` / `build.bat`. Everything here was built with `build.ps1` and run on Windows.
- The dashboard has not been opened in a browser (including the Add node / Remove controls, the membership-driven dropdowns, the MapReduce tab and the Job Console's retrieval select).
- The 300 s MapReduce timeout path (only the missing-interpreter and missing-pyspark 503s were run).

`agentgrid.node.ElectionBaselineProbe`, `agentgrid.orchestrator.BalancerBaselineProbe` and
`agentgrid.node.BlackboardBaselineProbe` produce the "before" numbers in [FIXES.md](FIXES.md).


