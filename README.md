# AgentGrid

A distributed agent grid built on Java RMI. A client dispatches `Subtask`s to a
remote agent node, which executes them and returns `Result`s.

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
| `blackboard-node-node-<id>` | Replicated blackboard (Exp 5; not yet used by the cluster) |
| `telemetry` | The node's event buffer, pulled by the control plane |
| `orchestrator` | Job orchestrator; active only on the elected leader |

### Configuration
[`backend/cluster.properties`](backend/cluster.properties):
- 5 nodes (IDs 1–5) on ports 1601–1605 (avoiding the standalone demo ports 1099, 1100–1102, 1201–1203, 1301–1305, 1401–1403).
- Worker pool sizes 2, 4, 4, 6, 6 (heterogeneous hardware).
- Clock drift offsets +3000, -2000, +500, 0, +1500 ms.
- `simulatedWorkMs=150`: delay each node adds to every subtask. The real stage work is
  sub-millisecond, so without it the thread-pool and load-balancing effects would not show.
  Override with `-Dagentgrid.simulatedWorkMs=<ms>`.

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
RMI, pulls their events, and serves the API and the dashboard. `Ctrl-C` stops the nodes too.
Node output goes to `backend/build/logs/node-<id>.log`.

Dashboard: **http://127.0.0.1:8080/** (the server binds to 127.0.0.1 only).

The older `agentgrid.node.ClusterLauncher` still starts the 5 nodes without a control plane
and prints an RMI health table.

### HTTP API

POST endpoints require `Content-Type: application/json` (otherwise 415); an `Origin` header,
if present, must be `http://127.0.0.1:8080` or `http://localhost:8080` (otherwise 403);
malformed or invalid input returns 400. Errors are `{"error": "...", "status": N}`.

| Method | Path | Body | Returns |
|---|---|---|---|
| GET | `/api/modules` | | Dashboard modules: exp1–exp4, exp6 |
| GET | `/api/cluster` | | Per node: `id, port, up, poolSize, queueDepth, lamport, clockOffsetMs, bindings, leaderView, isLeader` |
| GET | `/api/events?since=<seq>` | | Merged event log (control-plane and node events) after `seq`: `seq, type, node, details, lamport, nodeWallMs, trueMs`, plus `nodeSeq` and `fields` for node events |
| GET | `/api/stream` | | Server-sent events: `cluster` and `election` snapshots every 1 s (0.2 s during elections and jobs), and every new `event` |
| POST | `/api/nodes/{id}/kill` | `{}` | Kills the node process |
| POST | `/api/nodes/{id}/restart` | `{}` | Restarts the node process |
| POST | `/api/rmi/invoke` | `{"node": 1, "type": "SUMMARIZE", "count": 12}` | Runs `count` concurrent `execute()` calls on one node (Exp 1, 2) |
| POST | `/api/clock/drift` | `{"node": 1, "deltaMs": 5000}` | Shifts a node's clock (Exp 3) |
| POST | `/api/clock/sync` | `{}` | One Berkeley round over the UP nodes (Exp 3) |
| GET | `/api/election` | | `algorithm, views, agreed, leaderId, backupId, lastEpisode {startTrueMs, endTrueMs, convergenceMs, messages, byKind, leaderId, initiators, table}` |
| POST | `/api/election/algorithm` | `{"name": "BULLY"}` or `"RING"` | Switches the algorithm on all UP nodes |
| POST | `/api/election/start` | `{"node": 1}` | Starts an election on one node |
| POST | `/api/jobs` | `{"query": "...", "policy": "ROUND_ROBIN"}` (or `LEAST_LOADED`, `WEIGHTED`) | `{jobId, leader, status}`; 503 if no leader is agreed |
| GET | `/api/jobs` | | Recent jobs: `jobId, query, policy, status, leaderNode, makespanMs, subtasks, completedSubtasks, ...` |
| GET | `/api/jobs/{id}` | | The task graph: status (`QUEUED, RUNNING, COMPLETE, FAILED, ORPHANED`), answer, per-node subtask counts and peak queue depth, and every stage's subtasks with node, status and dispatch / result / completion Lamport times |

### Jobs
A job runs on the orchestrator of the elected leader: RETRIEVE (keyword match, fanned out
over 20 corpus chunks) -> RANK (TF-IDF) -> SUMMARIZE (extractive, fanned out over the 20
best documents) -> SYNTHESIZE (one merged answer, each sentence citing its document).
Each stage starts after the previous one completes. Subtasks are routed with the submitted
`agentgrid.balancer` policies. If the leader is killed mid-job, the job becomes ORPHANED
(its completed subtasks are kept) and the new leader accepts new jobs; it is not resumed.

### Verifiers
With the control plane running, from `backend/`:
```bash
java -cp build/classes agentgrid.control.ElectionVerifier   # Exp 4: election (V1-V6)
java -cp build/classes agentgrid.control.JobVerifier        # Phase 4: jobs and routing (J1-J5)
```
Both use only the HTTP API (JobVerifier also reads the bundled corpus to check that the
answer's sentences come from the cited documents), print every measured number, end with a
PASS/FAIL table, and exit non-zero if a check fails. ElectionVerifier kills and restarts nodes
and takes several minutes.

`agentgrid.node.ElectionBaselineProbe` and `agentgrid.orchestrator.BalancerBaselineProbe`
produce the "before" numbers in [FIXES.md](FIXES.md).


