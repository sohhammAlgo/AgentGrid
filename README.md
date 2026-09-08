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
