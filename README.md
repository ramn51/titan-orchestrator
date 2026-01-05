# 🛰️ Titan: The Hybrid Distributed Runtime

**Titan** is a lightweight distributed orchestrator built to bridge the gap between **Static Job Schedulers** (like Airflow) and **Dynamic Agent Runtimes**.

Designed for small-to-medium scale environments, it acts as a **Self-Hosting, Self-Healing Micro-PaaS**. It synthesizes the core primitives of orchestration—resolving dependencies, managing worker lifecycles, and handling resource governance—into a single, zero-dependency binary.

> *Built from scratch in Java (Core Engine) and Python (SDK). No external databases. No frameworks. Just raw TCP sockets and systems engineering.*

<p align="center">
  <img src="/screenshots/Titan_L1_diagram.png" alt="Titan High Level Architecture" width="800"/>
</p>

---
## ⚡ Capability Spectrum

Titan is designed to grow with your system's complexity:

1. **Level 1: Distributed Cron (The "Scheduler")**

    - _Simple:_ Run a Python script on a remote machine every hour.

    - **Mental Model:** Distributed `crontab` or lightweight Airflow.

2. **Level 2: Service Orchestrator (The "Platform")**

    - _Intermediate:_ Deploy long-running API servers and keep them alive (restart on crash).

    - **Mental Model:** A self-hosted PM2 or HashiCorp Nomad.

3. **Level 3: Agentic AI Runtime (The "Brain")**

    - _Advanced:_ Use the SDK to build self-modifying execution graphs where AI Agents spawn their own infrastructure to solve problems.

    - **Mental Model:** Infrastructure-aware LangChain.
---

## ☯️ Philosophy: One Runtime, Two Patterns

Titan recognizes that Modern Infrastructure requires two distinct ways of working. You choose the interface that matches your role.
### 1. Static Workflows (The "DevOps" Path)

* **Requirement:** Just the Titan Binary (Java) + A YAML definition.
* **Definition:** Deterministic DAGs defined *before* execution.
* **Use Case:** Nightly ETL, Database Backups, Periodic Reports.
* **Workflow:** Define your infrastructure in a declarative `pipeline.yaml` and submit it.

### 2. Dynamic Agentic Workflows (The "AI" Path)

* **Requirement:** The **Titan Python SDK**.
* **Definition:** The execution graph is constructed *at runtime* based on logic or LLM decisions.
* **Use Case:** AI Agents, Self-Healing Loops, Recursive Web Scraping.
* **Workflow:** Programmatically construct graphs where the "Next Step" depends on the "Previous Output" (e.g., *Run  Error  Generate Fix  Retry*).

---

## ✨ Key Features

### 1. Universal Workload Support

Titan orchestrates a diverse mix of primitives within a single dependency graph:

* **Ephemeral Scripts:** Fire-and-forget Python or Shell scripts.
* **Long-Running Services:** Titan acts as a hosting solution (like PM2), keeping Web Servers and APIs alive.
* **Hybrid DAGs:** A single workflow can chain a Python script  into a Java Service  into a Shell cleanup task.

> **Note on Isolation:** Titan currently uses **Process-Level Isolation** (managing PIDs and workspaces directly on the Host OS). Full Container Isolation (Docker integration) is planned for v2.0.

### 2. High-Performance Engineering

* **Custom Binary Protocol:** Uses `TITAN_PROTO`, a fragmentation-safe TCP wire format (Header + Body) designed for less than 50ms latency without JSON overhead.
* **Smart Task Affinity:** Implements "Parent-Child Locality." If a Worker spawns a sub-task, the scheduler attempts to place it on the same node to leverage local caching. Useful for cases where training and other tasks of a model needs to happen on a specific node.
* **Reactive Auto-Scaling:** The scheduler monitors worker load. If the cluster is saturated, it triggers **"Inception"** events where workers spawn *new* child workers to handle burst traffic.
* **Least-Connection Routing:** Jobs are intelligently routed to the worker with the lowest active load.
  
> **Note on Concurrency:** To ensure stability on smaller nodes, the default Worker concurrency is currently **capped at 4 slots** per instance. To utilize more cores on a large server, simply spawn multiple Worker instances on different ports (e.g., 8081, 8082)

### 3. Enterprise Resilience (Self-Healing)

* **Automated Resource Governance:** Workers maintain a persistent PID registry. On startup, they automatically detect and terminate **"Zombie" processes** left over from previous crashes.
* **Graceful Termination:** Supports controlled shutdown signals, ensuring nodes finish critical housekeeping before going offline.
* **Workspace Hydration:** Automatically packages, zips, transfers, and executes code in isolated sandboxes.

---

## 📂 The Data Plane (File System)

Titan strictly separates "Source Artifacts" from "Runtime State" to ensure reproducibility.

| Directory | Role | Description |
| :--- | :--- | :--- |
| **`perm_files/`** | **Artifact Registry** | The "Source of Truth." Place your scripts (`.py`, `.sh`) and binaries (`.jar`) here.<br><br>*Note: SDK/YAML submissions automatically stage files here, but you can also manually drop files in.* |
| **`titan_workspace/`** | **Execution Sandbox** | The runtime staging area.<br><br>• **`jobs/{id}/`**: Contains execution logs (`.log`) and isolated script copies for specific jobs.<br>• **`shared/`**: A "Data Bus" directory allowing dependent DAG tasks to share intermediate files. |
---

## 🚀 Getting Started

### Prerequisites

* **Java 17+** (Required for Core Engine)
* **Python 3.10+** (Required for SDK/CLI)
* **Maven** (For building the project)

### Option 0: Run via IntelliJ (Recommended for Dev)

If you are developing Titan, simply open the project in IntelliJ IDEA and run the Main classes directly:

1. **Master:** Run `titan.TitanScheduler`
2. **Worker:** Run `titan.TitanWorker`
3. **CLI:** Run `client.TitanCli`

### Option 1: Build & Run (Production Simulation)

#### 1. Build the Engine

Titan is built as a single "Uber-JAR".

```bash
# Build the project
mvn clean package

# Setup the artifacts (perm_files comes with the repo)
cp target/titan-orchestrator-1.0-SNAPSHOT.jar perm_files/Worker.jar

```

#### 2. Start the Cluster

**Terminal 1: The Master (Scheduler)**

```bash
# Starts the Brain on Port 9090
java -cp perm_files/Worker.jar titan.TitanScheduler

```

**Terminal 2: The Worker (Infrastructure)**

```bash
# Starts a Worker Node. 
# DEFAULT: Connects to localhost:9090 on Port 8080
java -jar perm_files/Worker.jar

# CUSTOM: To run on a different port or connect to a remote master:
# Usage: java -jar Worker.jar <MyPort> <MasterIP> <MasterPort>
java -jar perm_files/Worker.jar 8081 192.168.1.50 9090

```

#### 3. Install the Client

```bash
cd titan_sdk && pip install -e .

```

---

## 💻 Operator Manual (CLI)

The Titan CLI connects to the cluster for real-time management.

**Connect:**

```bash
java -cp perm_files/Worker.jar client.TitanCli

```

**Supported Commands:**

| Command | Description |
| --- | --- |
| `stats` | View cluster health, active nodes, and job queues. |
| `run <file>` | Immediately execute a script from `perm_files`. |
| `deploy <file>` | Deploy a long-running service (e.g., a dashboard). |
| `deploy Worker.jar <port>` | Manually spawn a new Worker node on a specific port. |
| `stop <service_id>` | Gracefully stop a running service or job. |
| `shutdown <port>` | Remotely decommission a specific worker node. |
| `dag <dag_string>` | Submit a raw DAG string (Advanced users). |

CLI looks like this:

```
==========================================
    [INFO] TITAN DISTRIBUTED ORCHESTRATOR    
==========================================
Connected to: localhost:9090
Commands: stats, json, submit <skill> <data>, dag <raw_dag>, exit

titan> stats
--- TITAN SYSTEM MONITOR ---
Active Workers:    3
Execution Queue:   0 jobs
Blocked (DAG):     1 jobs
-------------------------------
Worker Status:
 • [8080] Load: 2/4 (50%)    | Skills: [GENERAL]
    └── ⚙️ Service ID: DAG-JOB_SPAWN
    └── ⚙️ Service ID: DAG-step-4-server
 • [8081] Load: 0/4 (0%)     | Skills: [GENERAL]
```

---



## 🐍 Developer Manual (Two Modes)

### Mode 1: Static Pipelines (YAML)

*Best for: Scheduled tasks and known dependencies.*

**`daily_etl.yaml`**

```yaml
name: "Nightly-Data-Pipeline"
jobs:
  - id: "EXTRACT"
    script: "scripts/pull_data.py"
  
  - id: "TRANSFORM"
    script: "scripts/clean_data.py"
    dependencies: ["EXTRACT"]
    
  - id: "DEPLOY_API"
    type: "SERVICE"  # Long running task
    script: "scripts/server.py"
    port: 8080
    dependencies: ["TRANSFORM"]

```

### Mode 2: Agentic Workflows (Python SDK)

*Best for: AI Agents, Self-Healing loops, and Dynamic logic.*

**The "Self-Healing" Loop**

```python
from titan_sdk import TitanClient, TitanJob

client = TitanClient()
job = TitanJob(id="TASK", filename="risky_script.py")

# 1. Submit initial task
client.submit("agent_run", [job])

# 2. "Think" Loop
logs = client.fetch_logs(job.id)
if "ERROR" in logs:
    print("[ERR] Failure detected. Deploying Fix...")
    
    # Dynamic: The Agent creates a NEW job based on the error
    fix_job = TitanJob(id="FIX", filename="healer.py")
    client.submit("agent_fix", [fix_job])

```

---

## 🖥️ Dashboard

Titan includes a lightweight Python Flask dashboard to visualize cluster health.

[//]: # (<p align="center">)

[//]: # (<img src="screenshots/UI_Screenshot.png" alt="Titan Dashboard" width="70%"/>)

[//]: # (</p>)

![Titan High Level Architecture](screenshots/UI_Screenshot.png)

* **Real-time Stats:** CPU/Memory usage of every worker.
* **Log Streaming:** Watch stdout/stderr from distributed jobs in real-time via UDP aggregation.
* **Job History:** Visual timeline of all executions.

```bash
# Start the Dashboard
python3 dashboard.py
# Running on http://localhost:5000

```

---

## 🛠️ Architecture

The system follows a **Leader-Follower** topology with a decoupled control plane.

**This is the L2 architectural Diagram:**

<p align="center">
  <img src="/screenshots/Titan_L2_Final.png" alt="Titan High Level Architecture" width="800"/>
</p>

### The Protocol (`TITAN_PROTO`)

Communication happens over raw TCP sockets using a fixed-header framing strategy to ensure integrity:

```text
[ HEADER (8 Bytes) ]
| Version (1B) | OpCode (1B) | Flags (1B) | Spare (1B) | Payload Length (4B) |

[ BODY ]
| Binary Payload (Variable) ... |

```

### Deep Dive: Internal Mechanics

#### 1. The Scheduler (Master)

* **Inverted Worker Registration:** Unlike traditional systems that scan for nodes, Titan uses a **Push-Based Discovery** model. Workers initiate the connection to the Master, allowing dynamic scaling behind NATs or firewalls without static IP configuration.
* **The "ClockWatcher":** Instead of inefficient polling, Titan uses a dedicated thread monitoring a `DelayQueue` to handle future tasks. This ensures `O(log n)` scheduling efficiency thereby consuming zero CPU cycles until the precise millisecond a job is ready.
* **Reconciliation Loop:** A background `ScalerExecutor` runs every 15 seconds to compare the `ActiveJobQueue` against `WorkerCapacity`. If the delta is too high, it triggers the Auto-Scaler.

#### 2. The Failure Detector (Heartbeats)

* **Active Keep-Alive:** The Master maintains a dedicated `HeartBeatExecutor`. It tracks the "Last Seen" timestamp of every worker. If a worker goes silent for >30s, it is marked **DEAD**, and its active jobs are immediately re-queued to healthy nodes (Resilience).

---

## 🔮 Roadmap

* **Distributed Consensus:** Implement Raft/Paxos for Leader Election (Removing Master SPOF).
* **Crash Recovery:** Implement Write-Ahead-Logging (WAL) for persistent state recovery after master failure.
* **Containerization:** Support for Docker/Firecracker execution drivers for true filesystem isolation.

---

**Author:** Ram Narayanan A S
*Built to explore the "Hard Parts" of Distributed Systems.*