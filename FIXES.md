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


