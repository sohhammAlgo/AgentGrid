# AgentGrid-Lite Fixes and Enhancements

This document tracks bug fixes, architectural improvements, and empirical validations across the integration phases of AgentGrid-Lite.

---

## Phase 1: Node Process Integration & Service Hosting

### Bug A: `AgentServiceImpl.execute()` Bypasses Thread Pool

- **Bug Description**:
  In the baseline `AgentServiceImpl`, the single-subtask `execute(Subtask subtask)` method ran synchronously directly on RMI's connection dispatch thread (`Thread.currentThread().getName()` like `RMI TCP Connection(X)-127.0.0.1`). It never submitted work to `threadPool`, meaning that the configured `poolSize` had zero effect on limiting concurrency when multiple remote clients or threads invoked `execute()` simultaneously.

- **Location in Submitted Code**:
  [`backend/src/main/java/agentgrid/rmi/AgentServiceImpl.java`](file:///c:/Users/Dhruvv/Desktop/dc/AgentGrid/backend/src/main/java/agentgrid/rmi/AgentServiceImpl.java#L53-L83)

- **What Changed in Integrated Version**:
  In [`backend/src/main/java/agentgrid/node/NodeAgentService.java`](file:///c:/Users/Dhruvv/Desktop/dc/AgentGrid/backend/src/main/java/agentgrid/node/NodeAgentService.java#L59-L86), `execute(Subtask subtask)` increments `queueDepth`, submits the task to the managed `threadPool`, and blocks on `Future.get()`. When a worker thread executes the task, it runs the subtask logic and decrements `queueDepth` in a `finally` block upon completion. This strictly enforces the node's `poolSize` limit on concurrency.

- **Before/After Measurement**:
  - *Before (`AgentServiceImpl`, poolSize=2)*:
    12 concurrent `execute()` invocations dispatched simultaneously across RMI threads completed in **190 ms** (all 12 ran concurrently in parallel on `RMI TCP Connection` threads, bypassing the pool limit).
  - *After (`NodeAgentService`, poolSize=2)*:
    12 concurrent `execute()` invocations dispatched to the pool-2 node completed in **951 ms** (6 sequential rounds of 2 worker threads on `pool-2-thread-X` × ~150 ms simulated work per subtask).

---

### Bug B: `executeBatch()` Double-Counts Subtasks in `queueDepth`

- **Bug Description**:
  In `AgentServiceImpl.executeBatch()`, `queueDepth` was incremented during task submission (`queueDepth.incrementAndGet()`). However, the submitted `Callable` job subsequently delegated execution to `execute(subtask)`, which itself invoked `queueDepth.incrementAndGet()`. As a result, each actively running subtask was counted twice in `queueDepth` (once while queued, and twice while running). Additionally, had `execute()` submitted to the pool, this pattern would have caused thread pool self-deadlock.

- **Location in Submitted Code**:
  [`backend/src/main/java/agentgrid/rmi/AgentServiceImpl.java`](file:///c:/Users/Dhruvv/Desktop/dc/AgentGrid/backend/src/main/java/agentgrid/rmi/AgentServiceImpl.java#L86-L103)

- **What Changed in Integrated Version**:
  In [`backend/src/main/java/agentgrid/node/NodeAgentService.java`](file:///c:/Users/Dhruvv/Desktop/dc/AgentGrid/backend/src/main/java/agentgrid/node/NodeAgentService.java#L95-L132), `executeBatch(List<Subtask> subtasks)` submits each subtask directly as a worker pool job executing `executeSubtaskInternal(subtask)` without delegating through `execute()`. Each subtask increments `queueDepth` exactly once upon submission and decrements it exactly once upon worker completion.

- **Before/After Measurement**:
  - *Before (`AgentServiceImpl`, poolSize=2)*:
    When submitting a 12-subtask batch, sampling `getQueueDepth()` every 20 ms observed a peak queue depth of **14** (10 queued + 2 actively running counted twice, exceeding the 12-subtask total).
  - *After (`NodeAgentService`, poolSize=2)*:
    When submitting a 12-subtask batch, sampling `getQueueDepth()` every 20 ms observed a maximum queue depth of **12** (monotonically draining: 12 → 10 → 8 → 6 → 4 → 2 → 0; never exceeds 12).

- **Viva Note — Thread-Pool Self-Deadlock Prevention**:
  Why couldn't `executeBatch()` simply call the fixed `execute()` method?
  In the fixed `NodeAgentService`, `execute()` submits a task to `threadPool` and blocks the caller thread awaiting `Future.get()`. If `executeBatch()` submitted tasks to `threadPool` whose task bodies called `execute()`, the worker threads in `threadPool` would each be blocked waiting for a nested subtask to be executed by `threadPool`. With all `poolSize` worker threads blocked waiting for free workers, no worker would ever become available to execute the nested subtasks, resulting in complete **thread-pool starvation deadlock**. By decoupling subtask execution logic (`executeSubtaskInternal()`) from the RMI submission boundary (`execute()`), batch execution runs tasks directly on worker threads with zero nested submission.

---

## Phase 2: Control Plane & Event Ordering

### Fix C: Batch Path Lamport Timestamp Monotonicity Under Concurrent Workers

- **Bug Description**:
  When `executeBatch()` runs subtasks concurrently across worker threads in `threadPool`, each thread executed `long currentLamport = lamportClock.update(subtask.getLamportTimestamp())` and then subsequently printed `[agent-X] executing ... | lamport=...`. Because the update and stdout print were uncoordinated, worker threads completing out of order or experiencing thread scheduling preemption could interleave stdout output such that `| lamport=` stamps appeared non-monotonic in the console/log output even though the logical clock itself advanced monotonically.

- **Location in Code**:
  [`backend/src/main/java/agentgrid/node/NodeAgentService.java`](file:///c:/Users/Dhruvv/Desktop/dc/AgentGrid/backend/src/main/java/agentgrid/node/NodeAgentService.java#L143-L157)

- **What Changed in Integrated Version**:
  In `NodeAgentService.executeSubtaskInternal()`, the `lamportClock.update(...)` advance and the corresponding console print statement are enclosed within a synchronized block on the monitor of `lamportClock`:
  ```java
  long currentLamport;
  synchronized (lamportClock) {
      currentLamport = lamportClock.update(subtask.getLamportTimestamp());
      System.out.println(String.format(
          "[%s] executing %s on %s | lamport=%d",
          agentId, subtask, Thread.currentThread().getName(), currentLamport
      ));
  }
  ```
  This guarantees that every subtask execution log output reflects strictly increasing Lamport timestamps on the batch path without interleaving artifacts.

- **Validation**:
  Verified via Step 0 smoke test: 4 subtasks dispatched to Node 1 on the batch path produced strictly increasing logged Lamport timestamps:
  `lamport=1` -> `lamport=2` -> `lamport=3` -> `lamport=4`.

---

## Phase 3B: Experiment 4 (Leader Election) in the Live Cluster

Baseline: `java -cp build/classes agentgrid.election.ElectionDemo` (submitted code, 5 in-JVM nodes, node 5 crash simulated with `simulateCrash()`):

| Run | Bully (leader / messages / reported time) | Ring (leader / messages / reported time) |
|---|---|---|
| Manual run | Node 4 / 15 / 5 ms | Node 4 / 11 / 11 ms |
| Phase 3B Step 0 run | Node 4 / 15 / 1 ms | Node 4 / 11 / 24 ms |

The "before" numbers for D, E and F below also come from `agentgrid.node.ElectionBaselineProbe`. It drives the submitted `BullyElectionNode`, unmodified, on ports 1711–1725. The "after" numbers come from `agentgrid.control.ElectionVerifier` against the live 5-process cluster (real process kills).

### Fix D: Convergence Time Overwritten by Each Participant's `recordStart()`

- **Bug Description**:
  `BullyElectionNode.runBullyElection()` calls `metrics.recordStart()` at the start of every node's sub-election. In `ElectionDemo` one `ElectionMetrics` object is shared by all nodes, so node 2, then 3, then 4 each overwrite `startTime`. `recordCompletion()` runs when node 4 claims, so the reported "convergence" covers only node 4's final sub-election. It excludes node 1's election, the cascade through nodes 2 and 3, and the COORDINATOR broadcast.

- **Location in Submitted Code**:
  [`backend/src/main/java/agentgrid/election/BullyElectionNode.java`](backend/src/main/java/agentgrid/election/BullyElectionNode.java) (`runBullyElection`, `recordStart()` call) and [`ElectionMetrics.java`](backend/src/main/java/agentgrid/election/ElectionMetrics.java) (`recordStart` overwrites `startTime`).

- **What Changed in Integrated Version**:
  The election classes in `agentgrid.node` (`BullyNode`, `RingNode`) do no timing. Each node records `ELECTION_STARTED`, `ELECTION_MSG` (one per message sent) and `LEADER_ACCEPTED` events in its telemetry buffer ([`NodeEventBuffer`](backend/src/main/java/agentgrid/node/NodeEventBuffer.java)). The control plane ([`ElectionTracker`](backend/src/main/java/agentgrid/control/ElectionTracker.java)) derives each episode:
  - The episode runs from the first `ELECTION_STARTED` after the last stable state to the last `LEADER_ACCEPTED`.
  - Times use the machine clock (`trueMs`), not the drifted node clocks.
  - The message count is the number of `ELECTION_MSG` events inside the episode.

- **Before/After Measurement**:
  - *Before (ElectionDemo)*: Bully failover reported **5 ms** (manual run) / **1 ms** (Step 0 run) for 15 messages.
    - The probe measured the true time from `startElection()` on node 1 until nodes 1–4 all named node 4: **40, 28, 33, 25, 14 ms**. For the same 5 runs `ElectionMetrics` reported **2, 2, 2, 3, 1 ms**, so the reported value was 8–20× too small.
    - Ring in the demo has a single initiator, so `recordStart()` runs once there. The demo's Ring number (11 / 24 ms) is not distorted by this bug.
  - *After (live cluster, leader killed, 5 runs per algorithm, same failure detector)*:
    - **Bully**: convergence **502–513 ms, median 506 ms**, 19 messages (ANSWER 6, COORDINATOR 3, ELECTION 10). The 500 ms is node 4's ANSWER timeout: its ELECTION to the dead node 5 can never be answered, and that wait is now counted.
    - **Ring**: convergence **22–58 ms, median 24 ms**, median 12 messages.
    - These are not comparable to the demo's numbers: the demo's crashed node refused messages instantly, while a killed process has to time out.

### Fix E: Lower Node Claimed Leadership on Timeout While a Higher Node Was Still Electing

- **Bug Description**:
  After sending ELECTION to the higher nodes, the submitted initiator sleeps 500 ms. If no COORDINATOR has arrived by then, it calls `claimLeadership()`, even though a higher node answered and is still alive and electing. Any higher node that needs more than 500 ms therefore lets lower nodes declare themselves leader and broadcast COORDINATOR. Causes include a slow network, a loaded JVM, or a connect to a dead peer that stalls about 2 s on Windows (see F).

- **Location in Submitted Code**:
  [`backend/src/main/java/agentgrid/election/BullyElectionNode.java`](backend/src/main/java/agentgrid/election/BullyElectionNode.java), `runBullyElection()`: `Thread.sleep(ELECTION_TIMEOUT_MS)` followed by `claimLeadership()`.

- **What Changed in Integrated Version**:
  In [`BullyNode`](backend/src/main/java/agentgrid/node/BullyNode.java):
  - The initiator waits up to 500 ms for an ANSWER. A node claims leadership only if no higher node answered.
  - If an ANSWER arrives, it waits up to 2000 ms for COORDINATOR. If none arrives, it **restarts** the election (a new `ELECTION_STARTED`) and never claims leadership after that timeout.
  - A COORDINATOR from a lower id is ignored, and the receiver starts its own election.
  - The check-and-claim and the COORDINATOR acceptance happen under one lock, so a COORDINATOR cannot slip in between.

- **Before/After Measurement**:
  - *Before (probe, submitted code; node 4 handles ELECTION 800 ms late; node 5 crashed)*:
    - Node 1 claimed leadership at **+522 ms**.
    - During the run, nodes named **1, 2, 3 and 4** as leader.
    - Node 4 finally claimed at **+827 ms**, so the cluster had a wrong leader for about **305 ms**.
    - The ElectionDemo run itself did not show this, because its in-JVM calls return in microseconds.
  - *After (live cluster, V5)*: node 5 was killed, then node 4 was killed during its own election, 205 ms after its `ELECTION_STARTED` and inside its 500 ms ANSWER window.
    - Nodes 1–3 had ANSWERs from node 4. No COORDINATOR came, so each **restarted** after 2000 ms (3 restarts); none claimed.
    - Node 3 then claimed, and all survivors agreed on 3 (convergence 2521 ms). No node ever accepted node 4.
    - In V4 (20 runs of simultaneous elections from nodes 1 and 2), the only node that ever claimed leadership was node 5.

### Fix F: No Failure Detector; Elections Were Triggered by Hand

- **Bug Description**:
  Nothing in the submitted election code notices a dead leader. `ElectionDemo` crashes node 5 and then calls `startElection()` on node 1 itself ("Triggering Bully failover election from Node 1..."). A real cluster would keep pointing at a dead leader forever.

- **Location in Submitted Code**:
  [`backend/src/main/java/agentgrid/election/ElectionDemo.java`](backend/src/main/java/agentgrid/election/ElectionDemo.java) (manual `startElection()` after `simulateCrash()`); no detector exists in `agentgrid.election`.

- **What Changed in Integrated Version**:
  [`FailureDetector`](backend/src/main/java/agentgrid/node/FailureDetector.java):
  - Every node pings its leader every 500 ms. Three consecutive misses emit `HEARTBEAT_MISS` ×3, then `LEADER_LOST` (through [`LeaderLifecycleRegistry`](backend/src/main/java/agentgrid/node/LeaderLifecycleRegistry.java)), then start an election.
  - A freshly booted node starts its first election after its first `sync()` from the control plane, or after 3 s.
  - The election service is exported with [`TimeoutSocketFactory`](backend/src/main/java/agentgrid/node/TimeoutSocketFactory.java) (300 ms connect timeout). RMI stubs dial the host's LAN address (10.251.80.78 here). On this Windows machine, a connect to a closed port on that address took **2031 ms** without a timeout and **311 ms** with it; loopback refused in 0–16 ms either way.

- **Before/After Measurement**:
  - *Before (probe, submitted code)*: 1, 2, 3, 4 and 5 s after node 5 crashed, with no manual trigger, nodes 1–4 still named node 5 as leader. It was never detected.
  - *After (live cluster, measured from `NODE_KILLED` to the first `LEADER_LOST`, 10 kill-leader runs)*:
    - Bully: **1189–1415 ms, median 1281 ms**. Ring: **1077–1350 ms, median 1315 ms** (same detector).
    - All 10 failovers then elected node 4 without manual action.
    - The first miss appears about 700 ms after the kill, which includes up to one 500 ms ping interval plus the failed call itself. The second miss follows about 200 ms later, because the fixed-rate ping schedule runs a late tick immediately.

---

## Phase 4: Orchestrator on the Leader

"Before" and "after" numbers for G and H come from `agentgrid.orchestrator.BalancerBaselineProbe`. It runs the same pipeline as the leader's orchestrator (same `JobRunner`, same query, 42 subtasks per job, 150 ms simulated work per subtask) against the live 5-node cluster, with pools 2/4/4/6/6.

### Fix G: LeastLoaded Ignores Node Capacity

- **Bug Description**:
  The submitted `LeastLoadedBalancer.select()` routes to the node with the smallest raw `getQueueDepth()`. Three queued subtasks mean something different on a 2-thread node than on a 6-thread node, so the policy cannot favour stronger nodes. With pools 2/4/4/6/6 it behaved like a noisy round robin.
  - In the integrated cluster a second effect shows up. A dispatch reaches the node's queue a moment after `select()` returns, so a burst of selections reads stale depths and piles onto one node.
  - The submitted `LoadBalancerDemo` has the same race: it selects in a loop while the submissions run asynchronously.

- **Location in Submitted Code**:
  [`backend/src/main/java/agentgrid/balancer/LeastLoadedBalancer.java`](backend/src/main/java/agentgrid/balancer/LeastLoadedBalancer.java) (`select`, raw `getQueueDepth()` comparison).

- **What Changed in Integrated Version**:
  The submitted class is used unchanged; the fix is in the wrapper. [`WorkerNode`](backend/src/main/java/agentgrid/orchestrator/WorkerNode.java) presents each live node to the submitted balancers as an `AgentService`.
  - For LEAST_LOADED its `getQueueDepth()` returns `max(live queue depth, subtasks in flight to that node) * 1000 / poolSize`.
  - The ×1000 is there because the submitted class compares ints.
  - [`BalancingPolicy`](backend/src/main/java/agentgrid/orchestrator/BalancingPolicy.java) builds the three submitted balancers over these views.

- **Before/After Measurement** (3 runs each, makespan of the whole job):

  | LEAST_LOADED load view | Makespans (ms) | Median | Subtasks per node, worst run |
  |---|---|---|---|
  | ROUND_ROBIN (reference) | 1051, 1006, 957 | 1006 | {1=10, 2=8, 3=8, 4=8, 5=8} |
  | Raw depth (submitted) | 961, 1101, 1294 | **1101** | {1=6, 2=7, 3=20, 4=8, 5=1}; node 3 peak queue 10 |
  | Depth / pool only | 2205, 861, 989 | 989 | {1=24, 2=5, 3=7, 4=4, 5=2}; node 1 (pool 2) peak queue 20 |
  | Depth / pool + in flight (fix G) | 697, 675, 685 | **685** | {1=6, 2=8, 3=8, 4=10, 5=10} every run; peak queue = pool size |

  - With the submitted raw view, LeastLoaded was slower than round robin (median 1101 vs 1006 ms).
  - Normalising alone did not fix it: stale reads still piled up to 24 subtasks on the 2-thread node.
  - With both parts of the fix, the median dropped to 685 ms and the distribution was identical in all 3 runs.

### Fix H: Loop Index Used as the Subtask Timestamp

- **Bug Description**:
  The submitted `LoadBalancerDemo.buildBatch()` creates each subtask with `lamportTimestamp = i`, the loop index, and ignores the timestamps on the returned Results. (Our Phase 1 `BugVerification` driver copied the pattern.) The timestamp a node receives is then unrelated to anything that happened before it, so there is no causal chain across nodes. A subtask built from earlier results can carry a smaller timestamp than those results.

- **Location in Submitted Code**:
  [`backend/src/main/java/agentgrid/balancer/LoadBalancerDemo.java`](backend/src/main/java/agentgrid/balancer/LoadBalancerDemo.java) (`buildBatch`: `new Subtask(..., i)`).

- **What Changed in Integrated Version**:
  - **Dispatch:** [`JobRunner`](backend/src/main/java/agentgrid/orchestrator/JobRunner.java) records every dispatch as a `SUBTASK_DISPATCHED` event, which ticks the leader's Lamport clock, and the subtask carries that event's time.
  - **Worker:** [`NodeAgentService`](backend/src/main/java/agentgrid/node/NodeAgentService.java) applies the receive rule, does the work, then ticks again for the send, so the Result's timestamp exceeds the dispatch's.
  - **Leader:** it applies the receive rule to that timestamp before recording `SUBTASK_COMPLETED`.
  - A stage's subtasks are dispatched only after all results of the previous stage have been received, so they carry larger timestamps than those results.

- **Before/After Measurement** (one WEIGHTED job each; 22 subtasks depend on an earlier stage: RANK, 20 SUMMARIZE, SYNTHESIZE):
  - *Before (loop index, Result timestamps ignored)*: **22 of 22** dependent subtasks carried a timestamp no greater than a result they depend on. For example, RANK carried ts=0 while a RETRIEVE result it depends on had ts=2158.
  - *After (Lamport chain)*: **0 of 22**.
  - In the live cluster, JobVerifier J5 checks every subtask for dispatch < result < completion. It also checks that each stage's first dispatch follows the previous stage's last completion, and that each node's own events strictly increase.

---

## Phase 5: Replicated Blackboard (Exp 5) in the Live Cluster

The submitted `replication/*` classes are unchanged. The integrated blackboard is [`ClusterBlackboard`](backend/src/main/java/agentgrid/node/ClusterBlackboard.java) behind [`ClusterBlackboardService`](backend/src/main/java/agentgrid/node/ClusterBlackboardService.java), which extends the submitted `ReplicationService`. It is bound under the same name, `blackboard-node-node-<id>`.

- **"Before" numbers** for I–S come from [`BlackboardBaselineProbe`](backend/src/main/java/agentgrid/node/BlackboardBaselineProbe.java), which drives the submitted `ReplicatedBlackboardNode` in-process on ports 1731–1733. Where no number is given, the "before" comes from reading the submitted code.
- **"After" numbers** come from the two final `BlackboardVerifier` runs (run 1 / run 2) against the live 5-node cluster.
- **Conditions:** one Windows machine, simulated work (150 ms per subtask), and a simulated 400 ms EVENTUAL lag.

### Fix I: LWW Ties Depend on Arrival Order; the Caller Supplies the Timestamp

- **Bug Description**: `updateLocal` replaces an entry only if `incoming.timestamp > existing.timestamp`.
  - On a tie, whichever entry arrived first stays, so two replicas that see a tie in different orders diverge.
  - The timestamp is whatever the caller passes. No node clock is involved.
- **Location in Submitted Code**: [`replication/ReplicatedBlackboardNode.java`](backend/src/main/java/agentgrid/replication/ReplicatedBlackboardNode.java) (`updateLocal`).
- **What Changed in Integrated Version**:
  - [`BlackboardRecord.merge`](backend/src/main/java/agentgrid/node/BlackboardRecord.java) is a pure function. The higher timestamp wins, and a tie goes to the higher writer node id.
  - The writing node stamps each record from its own Berkeley-corrected clock. Stamps are monotonic per node: `max(corrected now, last + 1)`.
- **Before/After Measurement**:
  - *Before (probe 1)*: equal timestamps delivered in opposite orders left node-2 holding 'one' and node-3 holding 'two'. The replicas diverged.
  - *After (B6)*: all 24 arrival orders of 4 records, including a tie, end at the same record (both runs). B7 shows the node clock deciding LWW in the live cluster (see Fix R).

### Fix J: STRONG Applied Locally First, With No Quorum, and Failed on Any Dead Peer

- **Bug Description**:
  - A STRONG write applied locally before asking anyone. It then waited, with no timeout, for every configured peer.
  - It returned false if any single peer failed, but the partial write stayed on the origin and on every peer that succeeded.
  - It had no reachability or quorum check and never refused a write.
- **Location in Submitted Code**: [`replication/ReplicatedBlackboardNode.java`](backend/src/main/java/agentgrid/replication/ReplicatedBlackboardNode.java) (`publishFinding`, STRONG branch).
- **What Changed in Integrated Version**: `ClusterBlackboard.writeStrong`:
  - It probes liveness first, capped at 500 ms. If fewer than a majority (3 of 5) are live, it returns REFUSED and writes nothing.
  - Otherwise it applies locally and replicates in parallel, with each peer call bounded at 1500 ms.
  - The outcome is STORED, STORED_DEGRADED or FAILED_PARTIAL, with the acked and failed nodes listed. This is not consensus and not linearizable (see README).
- **Before/After Measurement**:
  - *Before (probe 2)*: with node-3 down, 5/5 STRONG writes returned false, yet all 5 of those "failed" writes were held by 2 of the 3 nodes.
  - *After (B3, both runs)*:
    - With 3 of 5 live, the write was STORED on [3, 4, 5] in 354 / 313 ms, and all live reads were FRESH.
    - With 2 of 5 live, it was REFUSED ("only 2 of 5 nodes live … nothing was written") and held by no live node.
    - After restarting 1, 2 and 3, the 3-live key was FRESH on all 5 and the refused key was absent everywhere.

### Fix K: STRONG Reported Failure When a Replica Already Held Newer Data

- **Bug Description**: `receiveReplicate` returns false when the replica already holds a newer timestamp. A STRONG write therefore reported failure even though every node held that key (in a newer version).
- **Location in Submitted Code**: [`replication/ReplicatedBlackboardNode.java`](backend/src/main/java/agentgrid/replication/ReplicatedBlackboardNode.java) (`receiveReplicate`, `publishFinding`).
- **What Changed in Integrated Version**: `ClusterBlackboard.replicaApply` acks when the replica holds this record *or a newer one*. The ack means "this key is at least this fresh here".
- **Before/After Measurement**:
  - *Before (probe 3)*: a STRONG write with ts 4000, sent while node-2 already held ts 5000, made `publishFinding` return false although every node held that key.
  - *After*: this exact scenario was not re-run in the live cluster; the rule is in the code. In the final runs, the STRONG counters after B7's full restart show 95 STORED, 0 FAILED_PARTIAL and 0 STORED_DEGRADED (both runs).

### Fix L: EVENTUAL Was One-Shot, With No Retry or Anti-Entropy

- **Bug Description**: an EVENTUAL write fired one `receiveReplicate` per peer. A peer that was down never received the write. There was no lag, so staleness depended only on thread scheduling.
- **Location in Submitted Code**: [`replication/ReplicatedBlackboardNode.java`](backend/src/main/java/agentgrid/replication/ReplicatedBlackboardNode.java) (`publishFinding`, EVENTUAL branch).
- **What Changed in Integrated Version**: `ClusterBlackboard.writeEventual`:
  - Delivery starts after the simulated `eventualLagMs` (400 ms). A failed delivery is retried every 1000 ms for up to 60 s.
  - Separately, each node pulls newer entries from one random peer every 2000 ms (anti-entropy, `pullNewer` with a `ts:writer` digest).
- **Before/After Measurement**:
  - *Before (probe 4)*: an EVENTUAL write made while node-3 was down was still missing on node-3 after it had been back for 3 s.
  - *After (B5)*: 5 EVENTUAL writes made while node 2 was down were all held by node 2 at its first ready snapshot. All 5 keys were FRESH on all 5 nodes 4008 / 4213 ms after the restart request, with no operator action.
  - *After (B2)*: 79–80 of 100 immediate reads were stale, and 20/20 writes converged on all 5 nodes, with medians of 460 / 453 ms.

### Fix M: Restarted Node Served Empty Reads as Current

- **Bug Description**: `getAllFindings()` exists but nothing calls it. A restarted node started empty and answered reads immediately as if it were up to date.
- **Location in Submitted Code**: [`replication/ReplicatedBlackboardNode.java`](backend/src/main/java/agentgrid/replication/ReplicatedBlackboardNode.java) (constructor; `getAllFindings` unused).
- **What Changed in Integrated Version**: `ClusterBlackboard.catchUp()`:
  - On start the node is not ready. It pulls from every reachable peer (retrying for up to 2 s), then asks the leader for a rejoin clock sync, and only then sets `ready`.
  - While it is not ready, reads report NOT_READY rather than an empty value.
- **Before/After Measurement**:
  - *Before (probe 5)*: restarted node-3 held 0 of 7 entries, and `getFinding("strong-0")` returned null.
  - *After (B4)*: restarted node 2 answered 12 / 14 reads NOT_READY, then became ready after 1989 / 1879 ms holding 5/5 of the keys written while it was down.

### Fix N: Findings From Different Jobs Collided on `subtaskId`

- **Bug Description**: entries are keyed by `subtaskId` only, and `taskId` is ignored. `summarize-1` from two different jobs overwrote each other.
- **Location in Submitted Code**: [`replication/ReplicatedBlackboardNode.java`](backend/src/main/java/agentgrid/replication/ReplicatedBlackboardNode.java) (map keyed by `entry.getSubtaskId()`).
- **What Changed in Integrated Version**:
  - Keys are strings chosen by the writer. Jobs use `job/<jobId>/finding/<subtaskId>` and `job/<jobId>/answer` (see [`JobRunner.blackboardKey`](backend/src/main/java/agentgrid/orchestrator/JobRunner.java)).
  - The worker that runs a SUMMARIZE subtask posts its own finding.
- **Before/After Measurement**:
  - *Before (probe 6)*: two jobs each wrote `summarize-1`. Node-1 kept 1 entry, with taskId=job-B, so job-A's finding was lost.
  - *After (B8, both runs)*: each job had 21 distinct keys (20 findings plus the answer) on all 5 replicas. The writer of each finding was the worker that ran it, and the blackboard answer equals the job answer.

### Fix O: Blackboard RMI Without Connect Timeouts

- **Bug Description**: the submitted blackboard is exported without a socket factory and looks up peers on every call. A peer that dies between lookup and call hits the Windows connect stall to the LAN address.
- **Location in Submitted Code**: [`replication/ReplicatedBlackboardNode.java`](backend/src/main/java/agentgrid/replication/ReplicatedBlackboardNode.java) (`UnicastRemoteObject` export, peer lookups).
- **What Changed in Integrated Version**:
  - `ClusterBlackboard` is exported with `TimeoutSocketFactory` (300 ms connect timeout).
  - Each peer call is bounded at 1500 ms, and the liveness probe is capped at 500 ms.
- **Before/After Measurement**:
  - *Before*: a connect to a closed port on the stubs' LAN address (10.251.80.78) took 2031 ms without the timeout (measured in Phase 3B).
  - *After*: 311 ms with it. In B3, a STRONG write with nodes 1 and 2 already dead completed in 354 / 313 ms.

### Fix P: The Submitted Interface Could Not Express the Design

- **Bug Description**: `ReplicationService` has no quorum outcome, ready flag, snapshot with metadata, writer id, or digest pull.
- **Location in Submitted Code**: [`replication/ReplicationService.java`](backend/src/main/java/agentgrid/replication/ReplicationService.java).
- **What Changed in Integrated Version**: `ClusterBlackboardService` extends `ReplicationService` and adds `write`, `read`, `snapshot`, `metrics`, `isReady`, `alive`, `replicaApply` and `pullNewer`. The 5 submitted methods are still served, adapted onto the new store.
- **Before/After Measurement** (probe 8): 5 remote methods before, 5 + 8 after.

### Fix Q: Berkeley Corrections Lost When a Node Restarts

- **Bug Description**: a restarted node rebuilds its `TimeServiceImpl` with the configured drift, so its earlier Berkeley corrections are lost until the next round.
- **Location in Submitted Code**: [`clock/TimeServiceImpl.java`](backend/src/main/java/agentgrid/clock/TimeServiceImpl.java) (offset initialised from the configured drift); nothing requested a sync on restart.
- **What Changed in Integrated Version**: during `catchUp()`, a restarting node asks the leader's [`ClockCoordinator`](backend/src/main/java/agentgrid/node/ClockCoordinator.java) for a rejoin round.
  - It waits at most 3 s, then becomes ready anyway, reporting `clockSynced=false`.
  - The leader debounces rejoin rounds to one per 2 s.
  - `POST /api/clock/auto` turns this off together with the periodic round.
- **Before/After Measurement**:
  - *Before (B7, auto-sync off, both runs)*: restarted nodes came back at their configured drift: 3001 / −1999 / 500 / 0 / 1500 ms and 3000 / −2000 / 499 / 0 / 1501 ms.
  - *After, B4 (both runs)*: restarted node 2 reported `clockSynced=true`, and offsets afterwards were N1–N5 = 32–34 ms and 90–91 ms.
  - At first this was **not fixed in every case**: in B5 of both Phase 5 final runs, restarted node 2 became ready with `clockSynced=false` and stayed at −2000 ms. The cause and fix are in Fix V. B4 and B5 now fail unless the restarted node's clock is within 100 ms of every other node within 5 s of ready.

### Fix R: Berkeley Unified on the Leader (BerkeleyRound Removed)

- **Bug Description**: the manual sync (`control/BerkeleyRound`, Phase 2 code) ran with the control plane as coordinator.
  - The control plane was left out of the average.
  - The round had no timeouts, and one failing node aborted it.
  - A second implementation would have been needed for the node-side rejoin sync.
- **Location**: `backend/src/main/java/agentgrid/control/BerkeleyRound.java` (deleted).
- **What Changed in Integrated Version**: there is one implementation, [`ClockCoordinator`](backend/src/main/java/agentgrid/node/ClockCoordinator.java).
  - It is run by the elected leader, which is included in the average. The nodes converge to their mean offset, not to true time.
  - Reads are RTT-compensated, in parallel, with a 700 ms timeout per node. Dead nodes are skipped.
  - It runs periodically every 30 s, on rejoin, and on manual `POST /api/clock/sync`. The manual sync asks the leader and returns 409 when no leader is agreed.
- **Before/After Measurement**:
  - *Before*: from code reading (no live measurement). A round with a dead node threw, and the rest of that round was skipped.
  - *After (B3, both runs)*: with nodes 1 and 2 down, a round completed with nodeCount=3, skipped=[1, 2], coordinator=5.
  - *After (B7, both runs)*: spread 5000 ms → 1 ms over 5 nodes. LWW kept the earlier write A before the sync and the later write B after it.

### Fix S: Blackboard Metrics Gave Averages Only, With No Refused Count

- **Bug Description**: `ReplicationMetrics` exposes an average write latency and a stale-read count, with no median and no refused-write count.
- **Location in Submitted Code**: [`replication/ReplicationMetrics.java`](backend/src/main/java/agentgrid/replication/ReplicationMetrics.java).
- **What Changed in Integrated Version**:
  - [`BlackboardMetrics`](backend/src/main/java/agentgrid/node/BlackboardMetrics.java) counts STORED / STORED_DEGRADED / REFUSED / FAILED_PARTIAL, EVENTUAL accepted / retries / delivered, anti-entropy applies and pending propagation.
  - Medians come from a bounded window of 200 samples.
  - The control plane classifies reads as FRESH / STALE / NOT_READY / UNCHECKED (`/api/blackboard/metrics`).
- **Before/After Measurement**:
  - *Before (probe 12)*: average only; no median, no refused count.
  - *After (B10, both runs)*:
    - STRONG: 95 stored, 0 degraded, 0 refused, 0 failedPartial, median 43 / 34 ms.
    - Reads: 320 / 630 stale, 341 / 672 fresh, 57 / 114 not ready.
  - Counters are per node process and reset on restart. B7 restarts all 5 nodes, so B3's REFUSED write shows as a `BLACKBOARD_REFUSED` event, not in these counters.

### Fix T: Restart Started the New JVM Before the Old One Released Its Port

- **Bug Description**: `NodeProcessManager.kill()` waited up to 1 s after `destroyForcibly()`, and `start()` launched the new JVM without checking the port. When several nodes were restarted at once, the old process could still hold its registry port, so the new JVM failed to bind and exited.
- **Location**: [`node/NodeProcessManager.java`](backend/src/main/java/agentgrid/node/NodeProcessManager.java) (Phase 1 code), `kill`, `start` and `restart`.
- **What Changed in Integrated Version**:
  - After `destroyForcibly()` it waits up to 5 s for the process to exit. It then bind-tests the node's port every 50 ms for up to 5 s, and only then starts the JVM.
  - If the port is still held, it starts nothing and throws. The restart API then records `NODE_RESTART_FAILED`, re-polls the node so it shows as down, and returns its usual 500 error shape.
  - The response shapes of `/api/nodes/{id}/restart` and `/kill` are unchanged.
- **Before/After Measurement**:
  - *Before*: in BlackboardVerifier run bb2, B7's parallel restart of all 5 left node 5 failing with "Port already in use: 1605", and the verifier aborted on the resulting 503.
  - *After*:
    - `RestartStress` (restart all 5 at once, 10 cycles) reached 10/10 ready in both runs. Time to ready was min / median / max 1399 / 3311 / 4167 ms, then 1360 / 3063 / 3399 ms. 0 of 50 restart requests failed.
    - In the first stress session, the port was still held after the old JVM had exited in 43 of 50 restarts, for up to 144 ms. Over the whole final session (stress, verifiers, orphan timing), this happened in 55 of 70 logged starts, for up to 215 ms. No start was refused.

### Fix U: A Single Missed Monitor Poll Orphaned a Running Job

- **Bug Description**: `JobDirectory.onCycle` marked a running job ORPHANED the first time the monitor saw its leader DOWN, and never let the leader's later report overwrite it.
  - The monitor's RMI calls time out at 700 ms. Under load, one slow poll produces a false NODE_DOWN_DETECTED / NODE_UP_DETECTED pair about 300 ms apart.
  - That false pair was enough to orphan a job that completed normally.
- **Location**: [`control/JobDirectory.java`](backend/src/main/java/agentgrid/control/JobDirectory.java) (Phase 4 code), `onCycle`.
- **What Changed in Integrated Version**:
  - A running job is orphaned only when its leader has been continuously unreachable for at least 1500 ms of wall-clock time, or when a different leader has been agreed.
  - An ORPHANED set while the original leader is still the agreed one is provisional. If that leader reports the job COMPLETE or FAILED, that outcome replaces ORPHANED. It becomes final once a different leader is agreed.
  - The monitor's 700 ms timeout and its NODE_DOWN_DETECTED behaviour are unchanged. The false DOWN/UP pair under load remains a known flake.
- **Before/After Measurement**:
  - *Before (BlackboardVerifier run bb3, B8 EVENTUAL)*: nodes 4 and 5 were seen DOWN for about 300 ms. The job was reported ORPHANED with makespan 5160 ms, although leader 5 recorded JOB_COMPLETED 21/21 at 5441 ms.
  - *Leader killed mid-job* (`JobFlakeCheck orphan 5`, times seen by the client after the kill request):

    | | Job ORPHANED in the API | JOB_ORPHANED event | New agreed leader |
    |---|---|---|---|
    | Before | 71–208 ms | 1871–2297 ms | 1745–2048 ms |
    | After | 1661–1834 ms | 1935–2154 ms | 1748–2002 ms |

    The event, which needs the new leader, is not delayed by the rule.
  - *No failure injected* (`JobFlakeCheck`, 30 jobs alternating STRONG / EVENTUAL):
    - Before: 30/30 COMPLETE.
    - After: 30/30 COMPLETE, 0 falsely ORPHANED.
    - Neither run saw a NODE_DOWN_DETECTED, so on an idle machine this check does not reproduce the flake. Its "before" evidence is the bb3 run above.

### Fix V: A Restarted Node Sometimes Never Asked for Its Rejoin Clock Sync

- **Bug Description**: a restarted node sometimes became ready with `clockSynced=false` and kept its configured drift until the next periodic round, up to 30 s later. The logs of the failing restarts all show `0 attempts, 15 polls without a leader`: the node never sent a rejoin request, because it did not know the leader during its 3 s clock wait. Its first election waits for a boot gate that opens on the control plane's first `sync()`, or after 3000 ms without one. That `sync()` was lost in two ways:
  - **(a) Dropped on arrival.** `NodeMain` binds `agent` before it installs the sync listener. The monitor counts a node as UP once `agent` and `time` answer, so its one-time first `sync()` could arrive in that window, and `NodeAgentService.sync()` dropped it. Logged 3 times in the second repro run: `sync() from the control plane arrived before the boot-gate listener was installed`, 0.5–0.8 s after start.
  - **(b) Never sent.** The monitor sends the first `sync()` only when it sees a node go from DOWN to UP. A kill and restart that fall between two monitor polls leave no DOWN observation (seen once: NODE_RESTARTED at 09:11:46.991 with no DOWN/UP pair).

  In both cases the gate opened on the 3000 ms fallback, just as the 3 s clock wait expired, and nothing retried afterwards. The node's `clockSynced` flag could also lag a successful rejoin: the leader answers `true`, then calls `noteSynced()` asynchronously.
  - **Not the cause:** debounce, leader reachability or timeouts. All 28 requests sent during the repro restarts returned `true`, within 22–907 ms; the longer ones had waited out the leader's 2 s debounce.
- **Location**:
  - [`node/NodeAgentService.java`](backend/src/main/java/agentgrid/node/NodeAgentService.java) (`sync`, `setSyncListener`) and [`node/NodeMain.java`](backend/src/main/java/agentgrid/node/NodeMain.java) (binding order);
  - [`control/ClusterMonitor.java`](backend/src/main/java/agentgrid/control/ClusterMonitor.java) (`pollNode` first-sighting rule);
  - [`node/ClusterBlackboard.java`](backend/src/main/java/agentgrid/node/ClusterBlackboard.java) (`catchUp`).
- **What Changed in Integrated Version**:
  - **Fix for (a):** `NodeAgentService` remembers a `sync()` that arrives before the listener exists and runs the listener when it is installed. Opening the gate is idempotent.
  - **Fix for (b):** the restart handler calls `ClusterMonitor.markRestarted(id)`, so the next UP poll of the new process is treated as a first sighting (algorithm and auto-sync aligned, first `sync()` sent) even if no poll saw it down.
  - **Node side:**
    - A `true` from the leader marks the node synced at once.
    - READY is still capped at 3 s. If the node is still unsynced then, it keeps asking the current leader in the background (backoff 250 ms doubling to 2 s, 5 s per request) until a round includes it or auto-sync is turned off.
  - **Diagnostics kept:**
    - Each rejoin attempt and its outcome are logged on the node, and the leader logs each request with its lock and debounce waits and whether the round included the node.
    - `NodeProcessManager` now appends to `build/logs/node-<id>.log`, with a header per start, so a restart no longer overwrites the previous run's log.
  - **Verifier:** B4 and B5 now FAIL unless, with auto-sync on, the restarted node's offset is within 100 ms of every other node's within 5 s of ready.
- **Before/After Measurement** (`RejoinRepro`, restart node 2 in three modes: 5 s apart, back to back within 2 s of the previous rejoin round, right after a manual round):

  | | Restarts | `clockSynced=false` at ready | Not within 100 ms of all nodes 5 s after ready |
  |---|---|---|---|
  | Before, run 1 | 18 | 4 (all `0 attempts, 15 polls without a leader`) | 2 |
  | Before, run 2 (drop logged) | 18 | 4 (3 × cause a, 1 × cause b) | 3 |
  | After | 18 | 0 | 0 (aligned 9–51 ms after ready) |

  - In the "after" run the early `sync()` happened twice; both times it was remembered and the gate opened on it.
  - Final runs (BlackboardVerifier ×2, ElectionVerifier, JobVerifier, RestartStress): 91 of 91 node starts with auto-sync on were synced at ready, and none needed the 3000 ms gate fallback. The 10 unsynced starts were B7's deliberate auto-sync-off restarts.
  - B4 / B5 clocks aligned 28 / 11 ms and 8 / 8 ms after ready.
  - **Not exercised live:** the background retry after READY never ran, because every rejoin completed within the 3 s wait. Four attempts to force it (kill the leader, then restart node 2 at once) also synced within 0.5 s.
  - **Seen once:** a booting node 2 briefly made itself Bully leader because its ELECTION got no ANSWER in time (0.4 s), then accepted node 5. Its own rejoin round ran in that window and left all 5 clocks within 37 ms. This is existing Exp 4 boot behaviour and was not changed.

---

## Elastic Cluster Membership

### Fix W: Node RMI Stubs Advertised a LAN Address That Could Disappear Mid-Run

- **Bug Description**: RMI puts `java.rmi.server.hostname` (by default the machine's LAN IP) into every stub a node exports. While a cluster was running, the Wi-Fi address changed from 10.10.127.176 to 10.10.177.24. Every stub still pointed at the old address, so all node-to-node and control-plane-to-node calls on exported objects failed:
  - `/api/rmi/invoke` returned `Exception creating connection to: 10.10.127.176; ... Connect timed out` for RETRIEVE and SUMMARIZE alike;
  - `/api/election` showed every leader view `null`;
  - node 2 ran Berkeley rounds "over 1 nodes ... skipped [1, 3, 4, 5]".

  Registry lookups still worked, because they use `localhost`. Earlier phases had seen the same address in stubs (10.251.80.78; the 2 s connect stall in Phase 3B).
- **Location**: [`node/NodeMain.java`](backend/src/main/java/agentgrid/node/NodeMain.java) (startup, before RMI initialises).
- **What Changed**: a node sets `java.rmi.server.hostname=127.0.0.1` unless it is already set. Every node runs on this machine and every lookup already used `localhost`. The control plane exports no remote objects and is unaffected.
- **Before/After**:
  - *Before*: on the cluster started at 10.10.127.176, 2 of 2 RMI-invoke calls failed with the connect timeout after the address change, and no leader was agreed.
  - *After*: on the same machine (now 10.10.177.24), SUMMARIZE calls returned their summaries, and all six final verifier runs (MembershipVerifier, BlackboardVerifier, ElectionVerifier, JobVerifier, RestartStress, JobFlakeCheck) passed on fresh control planes.
  - Not tested: an address change *during* a verifier run with the fix in place.

### Fix X: The Cluster Was Hard-Coded to the Five Nodes of cluster.properties

- **Bug Description**: the cluster could not change size. Every component took its node set from `cluster.properties` once at startup (`ClusterConfig` was immutable):
  - the monitor, Bully/Ring peers, blackboard replicas and the STRONG majority ("3 of 5"), the Berkeley round, the orchestrator's workers, and the process manager (`ClusterConfig.load().getNode(id)` for every start);
  - the dashboard: "Cluster Node Fleet (5 Nodes)", a 5-column grid, a `[1, 2, 3, 4, 5]` fallback in exp5, and "need a live majority (3 of 5)".
- **Location**:
  - [`node/ClusterConfig.java`](backend/src/main/java/agentgrid/node/ClusterConfig.java), [`node/ElectionNode.java`](backend/src/main/java/agentgrid/node/ElectionNode.java), [`node/ClusterBlackboard.java`](backend/src/main/java/agentgrid/node/ClusterBlackboard.java), [`node/NodeProcessManager.java`](backend/src/main/java/agentgrid/node/NodeProcessManager.java);
  - [`orchestrator/ClusterView.java`](backend/src/main/java/agentgrid/orchestrator/ClusterView.java), [`orchestrator/BalancingPolicy.java`](backend/src/main/java/agentgrid/orchestrator/BalancingPolicy.java);
  - [`control/ClusterMonitor.java`](backend/src/main/java/agentgrid/control/ClusterMonitor.java), [`control/ControlPlaneMain.java`](backend/src/main/java/agentgrid/control/ControlPlaneMain.java);
  - `frontend/` (index.html, app.js, styles.css, exp1–exp6), `backend/stop-cluster.ps1`.
- **What Changed**: an immutable `Membership {epoch, members}` with one authority, the control plane's `MembershipManager`.
  - Nodes apply only a higher epoch. `ClusterConfig` reads the current membership for ids, ports, pool sizes, weights and the quorum `floor(n/2)+1`, so every caller above follows it.
  - `ElectionNode.updatePeers` replaces Bully's and the ring's peer set.
  - The blackboard uses one membership snapshot per STRONG decision, and drops pending EVENTUAL deliveries to removed peers.
  - A joining node holds its boot gate (no election) until it is a member, and skips the clock wait (it is synced once it has joined).
  - Add/remove follow the decided order and guards (README, Elastic membership).
  - The dashboard fleet, every node dropdown, the exp5 quorum text and the exp6 columns follow the membership.
- **Before/After** (`MembershipVerifier`, fresh control plane):
  - *Before*: 5 members, no add or remove; quorum fixed at 3.
  - *After*:
    - Adding node 6 took 691 ms, with all 6 on epoch 2 and quorum 4. All 6 accepted node 6 as leader 21 ms after MEMBERSHIP_CHANGED, and it ran 6 of 42 WEIGHTED subtasks.
    - Node 6 held a key written before it joined. A STRONG write was acknowledged by 6. The join's Berkeley round covered 6 with a spread of 1 ms.
    - With 3 of 6 live, STRONG was refused ("needs 4"). The cluster grew to 9 members and the 10th add got 409.
    - The add-failure path (port held) gave 500, NODE_ADD_FAILED, the membership unchanged and no process left.
    - The ids of removed or failed adds were not reused. Removing the leader gave a new agreed leader in 1352 ms.
    - The guards returned 409 for a running job, a live removal below quorum, and fewer than 3 members, and 404 for an unknown id. Removing a dead node was allowed.
    - BlackboardVerifier, ElectionVerifier, JobVerifier, RestartStress and JobFlakeCheck all passed on the 5-member default.

### Fix Y: Exp 1 / Exp 2 SUMMARIZE Calls Named No Document

- **Bug Description**: `/api/rmi/invoke` sent every subtask the input `query-chunk-N`. SUMMARIZE expects a payload naming a corpus document, so every SUMMARIZE call from the Exp 1 and Exp 2 panels returned `no document (none) in the corpus`.
- **Location**: [`control/ControlPlaneMain.java`](backend/src/main/java/agentgrid/control/ControlPlaneMain.java) (`handleRmiInvoke`); `frontend/panels/exp1.js`, `exp2.js`.
- **What Changed**:
  - For SUMMARIZE, the control plane sends an encoded payload with a query and `doc`: the requested document, or the first corpus document by default. An unknown id is a 400.
  - `GET /api/corpus` lists the documents, and the panels have a document dropdown.
  - The other subtask types are unchanged.
- **Before/After**:
  - *Before*: the SUMMARIZE output was "no document (none) in the corpus".
  - *After*:
    - The default call summarised `doc-01` "Remote Method Invocation" in 2 sentences.
    - `doc-07` gave "Bully Election" (3 calls in 172 ms).
    - `"doc": "nope"` returned 400 "Unknown corpus document: nope".
