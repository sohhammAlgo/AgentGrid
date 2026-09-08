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
```

## Requirements

JDK 17 or newer (`javac`, `java` on your `PATH`). No external dependencies.

## Build

```bash
./backend/build.sh
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
