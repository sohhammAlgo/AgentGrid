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
| GET | `/api/modules` | | Dashboard modules: exp1–exp6 |
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
| POST | `/api/jobs` | `{"query": "...", "policy": "ROUND_ROBIN", "consistency": "EVENTUAL"}` (policy `LEAST_LOADED` / `WEIGHTED`; consistency `STRONG`, default `EVENTUAL`) | `{jobId, leader, consistency, status}`; 503 if no leader is agreed |
| GET | `/api/jobs` | | Recent jobs: `jobId, query, policy, status, leaderNode, makespanMs, subtasks, completedSubtasks, ...` |
| GET | `/api/jobs/{id}` | | The task graph: status (`QUEUED, RUNNING, COMPLETE, FAILED, ORPHANED`), answer, per-node subtask counts and peak queue depth, and every stage's subtasks with node, status and dispatch / result / completion Lamport times |

### Jobs
A job runs on the orchestrator of the elected leader: RETRIEVE (keyword match, fanned out
over 20 corpus chunks) -> RANK (TF-IDF) -> SUMMARIZE (extractive, fanned out over the 20
best documents) -> SYNTHESIZE (one merged answer, each sentence citing its document).
Each stage starts after the previous one completes. Subtasks are routed with the submitted
`agentgrid.balancer` policies. If the leader is killed mid-job, the job becomes ORPHANED
(its completed subtasks are kept) and the new leader accepts new jobs; it is not resumed.

Each worker that runs a SUMMARIZE subtask posts its finding to its own blackboard replica as
`job/<jobId>/finding/<subtaskId>`, and the SYNTHESIZE worker posts the answer as
`job/<jobId>/answer`, under the job's `consistency`. The job view shows every finding as stored
or not stored; a refused or failed write never fails the job. The leader records one
`BLACKBOARD_JOB_FINDINGS` event per job (no event per write).

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

### Verifiers
With the control plane running, from `backend/`:
```bash
java -cp build/classes agentgrid.control.ElectionVerifier   # Exp 4: election (V1-V6)
java -cp build/classes agentgrid.control.JobVerifier        # Phase 4: jobs and routing (J1-J5)
java -cp build/classes agentgrid.control.BlackboardVerifier # Phase 5: replicated blackboard (B1-B12)
java -cp build/classes agentgrid.control.MembershipVerifier # elastic membership: add / remove nodes (M1-M13)
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

**Not yet tested:**
- The shutdown hook when the control plane's console window is closed. Ctrl+C stops all 5 nodes; a forced kill of the control plane leaves them running (stop them with `stop-cluster.ps1`).
- Linux, and `build.sh` / `build.bat`. Everything here was built with `build.ps1` and run on Windows.
- The dashboard has not been opened in a browser (including the Add node / Remove controls and the membership-driven dropdowns).

`agentgrid.node.ElectionBaselineProbe`, `agentgrid.orchestrator.BalancerBaselineProbe` and
`agentgrid.node.BlackboardBaselineProbe` produce the "before" numbers in [FIXES.md](FIXES.md).


