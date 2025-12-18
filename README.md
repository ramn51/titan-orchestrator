
---

# ⚡ Titan: Distributed Task Orchestrator

**Titan** is a high-performance, fault-tolerant distributed task orchestrator built from scratch in Java. Unlike traditional job schedulers, Titan implements a **"Push-Based" Resource-Aware Architecture**, making it capable of managing complex dependency graphs (DAGs), temporal workflows, and real-time load balancing without external dependencies like Zookeeper or Redis.

> **Status:** Phase 6 Complete (DAGs, Cascading Failures, CLI Dashboard)

## 🏗 System Architecture

Titan operates on a **Multi-Stage Queue Architecture** (often described as "Waterfall Scheduling"). A job must pass through three distinct gates before execution:

### 1. The Orchestrator (The Brain)

* **Gate 1: Logic (DAG Engine):** Jobs start in the `DAG Waiting Room`. They are "Blocked" until all parent dependencies (defined in the DAG) successfully complete.
* **Gate 2: Time (Delay Queue):** Once unblocked, jobs move to the `Waiting Room` where they sit with zero CPU overhead until their scheduled start time.
* **Gate 3: Resources (Active Queue):** Finally, jobs enter the `PriorityQueue`. The **Smart Dispatcher** uses a **Least-Connections Algorithm** to push tasks to the worker with the lowest real-time load.

### 2. The Worker (The Muscle)

* **Resource Guarding:** Enforces a strict `FixedThreadPool` limit. If saturated, it rejects new work to prevent cascading cluster failure.
* **Atomic Load Tracking:** Reports real-time CPU/Task load back to the Master via `PONG` heartbeats.

### 3. The Protocol (The Language)

* **TitanProtocol:** A custom, length-prefixed binary protocol over TCP (no HTTP overhead).
* **Commands:** `REGISTER`, `SUBMIT_DAG`, `EXECUTE`, `PING/PONG`, `STATS`.

---

## ✅ Core Capabilities

| Feature | Description |
| --- | --- |
| **🔗 Dependency Graphs** | Supports complex DAG patterns (Diamond, Fan-out/Fan-in). |
| **💥 Cascading Failure** | Implements a **"Kill Switch."** If a parent job fails (and exhausts retries), the entire downstream branch is automatically frozen and cancelled to prevent data corruption. |
| **⏳ Temporal Scheduling** | Jobs can be scheduled to run *immediately* after dependencies or after a fixed *time delay*. |
| **⚡ Resource-Aware Push** | unlike standard BPMN engines (Activiti/Camunda) that use a **DB Polling** model, Titan **Pushes** tasks to the least-loaded worker for lower latency. |
| **🛡️ Fault Tolerance** | Automatic 3-strike retry policy and Dead Letter Queue (DLQ) for "poison pill" tasks. |

---

## 🐳 Quick Start (Docker)

The easiest way to run a full cluster (1 Scheduler + 3 Workers) is using Docker Compose.

**1. Create `docker-compose.yml**`

```yaml
version: '3.8'
services:
  titan-scheduler:
    build: .
    command: ["java", "-cp", "out/production/DistributedOrchestrator", "scheduler.TitanMaster"]
    ports:
      - "9090:9090"
  worker-1:
    build: .
    command: ["java", "-cp", "out/production/DistributedOrchestrator", "network.TitanWorker"]
    depends_on:
      - titan-scheduler

```

**2. Run the Cluster**

```bash
docker-compose up --build

```

**3. Connect via CLI**

```bash
java -cp out/production/DistributedOrchestrator client.TitanCli

```

---

## 🖥️ Manual Setup (Localhost)

If you prefer running without Docker, open **three separate terminals**:

1. **Terminal 1 (Scheduler):**
```bash
java scheduler.TitanMaster
# Output: ✅ SchedulerServer Listening on port 9090

```


2. **Terminal 2 (Worker):**
```bash
java network.TitanWorker
# Output: ✅ Successfully registered with Scheduler!

```


3. **Terminal 3 (CLI):**
```bash
java client.TitanCli
# Output: Connected to localhost:9090

```



---

## 🎮 CLI Usage & Sample Output

The Titan CLI provides a real-time dashboard into the "Brain" of the orchestrator.

| Command | Usage | Description |
| --- | --- | --- |
| **Stats** | `stats` | View active workers, queue sizes, and blocked jobs. |
| **Submit Job** | `submit <SKILL> <DATA>` | Submit a single, independent task. |
| **Submit DAG** | `dag <RAW_STRING>` | Submit a complex dependency graph. |

### Sample: The "Twin Engine" Stress Test

(Running a fast "Diamond" Graph and a slow "Timer" Graph simultaneously)

```text
titan> stats
📡 Server Response:

--- 🛰️ TITAN SYSTEM MONITOR ---
Active Workers:    3
Execution Queue:   2 jobs      <-- (Parallel execution of Diamond branches)
Delayed (Time):    1 jobs      <-- (Timer job waiting for 15s)
Blocked (DAG):     1 jobs      <-- (Final merge step waiting for branches)
Dead Letter (DLQ): 0 jobs
-------------------------------
Worker Status:
 • [8081] Load: 1 | Skill: [TEST]
 • [8082] Load: 1 | Skill: [TEST]

```

---

## 📂 Project Structure

```text
src/
├── client/
│   └── TitanCli.java            # Remote CLI dashboard
├── network/
│   ├── TitanProtocol.java       # Binary framing logic
│   ├── RpcClient.java           # Communication with Timeouts
│   ├── RpcWorkerServer.java     # Worker implementation
│   └── TitanWorker.java         # Worker Entry Point
├── scheduler/
│   ├── Job.java                 # Task object (DAG & Status Logic)
│   ├── Scheduler.java           # The core "Brain" (DAG + Delay + Dispatch)
│   ├── TitanMaster.java         # Scheduler Entry Point
│   └── WorkerRegistry.java      # Dynamic cluster inventory

```

---

## 🔮 Roadmap

### Phase 6: Workflow Orchestration (DAGs)

* [x] **Dependency Management:** Support for Directed Acyclic Graphs (DAGs).
* [x] **Cascading Failures:** Automatic cancellation of children if parent fails.
* [x] **CLI:** Real-time system monitoring.

### Phase 7: Persistence & Reliability (Next Up)

* [ ] **State Journaling:** Log job state changes (SQLite) to recover execution after a crash.
* [ ] **Reconciliation Loop:** Recover "orphaned" tasks on startup.

### Phase 8: High Availability

* [ ] **Leader Election:** Allow multiple Masters to run with automatic failover.