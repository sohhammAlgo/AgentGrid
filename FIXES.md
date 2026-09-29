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
