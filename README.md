<p align="center">
  <img src="screenshots/Titan_logo.png" alt="Titan Orchestrator Logo" width="300">
</p>

# Titan Orchestrator

**Self-hosted distributed runtime for DAGs, Agents, and Services.**

[![Documentation](https://img.shields.io/badge/docs-live-1de9b6.svg)](https://ramn51.github.io/titan-orchestrator/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![Status: Experimental](https://img.shields.io/badge/Status-Experimental_Research-blue)
![Built by: 1 Developer](https://img.shields.io/badge/Solo_Project-Ram_Narayanan-brightgreen)

Titan is a zero-dependency distributed runtime built from first principles to solve the **Physical Execution** problem. It bridges the gap between static orchestrators (Airflow), dynamic AI agents, and long-running micro-services.

**What Titan can run:**

- **Static Pipelines** — deterministic ETL, DevOps, and reporting workflows
- **Long-Running Services** — persistent APIs and servers with auto-restart
- **Runtime-Defined DAGs** — execution graphs built on-the-fly via logic
- **Agentic Workflows** — complex graphs generated and mutated dynamically by LLMs
- **Human-in-the-Loop Pipelines** — DAGs that pause at checkpoints and wait for a human Approve/Reject before continuing

---

## Built-In Dashboard

Titan ships with a lightweight Python Flask dashboard with three views:

### Orchestrator — Cluster health and worker load

![Titan Orchestrator Dashboard](screenshots/dashboard_orchestrator.png)

Real-time view of every connected worker node — capability tag (GENERAL / GPU / HIGH_MEM), current load, active job, running services, and recent activity. Includes a **+ Launch Worker** button to spin up new worker nodes from the browser.

### DAG Pipelines — Visual pipeline monitor

![DAG Visualizer](screenshots/visualizer_overview.png)

Every pipeline submitted to the cluster — via CLI, SDK, YAML, or the visual Constructor — is automatically rendered as a live dependency graph. Node colours update in real-time as jobs move through `PENDING → RUNNING → COMPLETED / FAILED`. Click any node to stream its stdout/stderr live.

### DAG Constructor — Visual pipeline builder

![DAG Constructor](screenshots/constructor_overview.png)

Browser-based drag-and-drop DAG editor. Add task and service nodes, draw dependency edges, configure script, capability, args, priority, and HITL gates per node — then deploy directly to the cluster with one click. Auto-generates equivalent Python SDK and YAML as you build. Includes undo/redo, multi-select, autosave, and cycle detection.

### Agent Runs — Multi-stage agent timeline

![Agent Runs](screenshots/visualizer_agent_runs.png)

Groups all DAG stages that share an `agent_run_id` into a single timeline row. When an agent runs iteratively (PLAN → ITER → EVAL → SYNTH), each stage is a separate DAG submission — Agent Runs reconstructs the full lifecycle so you don't have to hunt through individual DAG entries.

> Flask is the only external dependency. The core engine has zero external dependencies.

---

## The Capability Spectrum

Titan scales with your complexity, from a simple script runner to an autonomous agent host:

1. **Level 1: Distributed Task Scheduler**
   Run Python/Shell scripts in sequence or parallel across a cluster. On-demand and delayed execution — cron-style recurring schedules are on the v2 roadmap.

2. **Level 2: Service Orchestrator**
   Deploy long-running APIs and services as permanent workers. Workers re-register with the Master after job completion — similar in model to Nomad for mixed batch + service workloads.

3. **Level 3: Agentic Execution Runtime (Autonomous Mode)**
   Infrastructure-aware substrate where software agents programmatically spawn compute tasks based on LLM decisions.

---

## One Runtime, Two Patterns

| Feature | Static Workflows (DevOps) | Runtime-Defined (Agentic) |
| :--- | :--- | :--- |
| **Requirement** | YAML Definition + Java Binary | Titan Python SDK |
| **Definition** | Deterministic DAGs defined beforehand | Graphs constructed at runtime |
| **Use Case** | Nightly ETL, Backups, Reporting | AI Agents, Self-Healing Loops |

---

## Key Features

### 1. Universal Workload Support
Orchestrate ephemeral scripts, long-running services, and hybrid DAGs (e.g., Python script → Java Service → Shell cleanup) in a single zero-dependency binary.

### 2. Visual DAG Constructor
Build and deploy pipelines without writing code. Drag nodes, draw edges, configure jobs, and hit Deploy — the Constructor submits directly to the master and opens the live visualizer. HITL gates, GPU routing, sticky scheduling, and priority are all configurable from the UI.

### 3. Human-in-the-Loop (HITL) Gates
Pause a DAG at any checkpoint and wait for a human Approve/Reject before downstream jobs run. Gate injection is automatic — set a message in the Constructor or SDK and the server injects the gate job. Configurable timeout (default 48 hours). Approve/Reject from the DAG Pipelines dashboard.

### 4. Capability-Aware Routing
- **Permanent vs. Ephemeral Workers** — mark core nodes as permanent so they stay alive; burst workers decommission automatically after 45s of idle time
- **Hardware Routing** — tag workers with `GPU` or `HIGH_MEM`; jobs with a matching requirement are held until a capable node is free

### 5. Scaling & Dispatch
- **Process-Level Auto-Scaling** — when a worker's queue saturates, it can spawn child worker processes on the same machine to absorb the spike
- **Least-Connection Routing** — new jobs go to the worker with the lowest active thread count
- **Custom TCP Protocol (TITAN_PROTO)** — fixed-header binary framing over raw TCP; no JSON serialization overhead on the dispatch path

### 6. Failure Handling
- **Crash Recovery (AOF)** — TitanStore writes every state transition to an append-only file; if the Master restarts, it replays the file and resumes in-flight DAGs where they left off
- **Worker Re-registration** — workers re-register with the Master every 30 seconds; the cluster recovers through a Master restart without manual intervention
- **Orphan Cleanup** — on startup, workers scan for and kill leftover processes from a previous crash before accepting new work
- **Callback Retry** — job completion callbacks retry with exponential backoff (up to 5 attempts) to handle transient network issues

---

## Architecture

<p align="center">
  <img src="./screenshots/Titan_L1_diagram_final.png" alt="Titan High Level Architecture" width="800"/>
</p>

---

## Demos in Action

### 1. Visual DAG Constructor
*Build a pipeline by dragging nodes and drawing edges — deploy directly to the cluster with one click.*

https://github.com/user-attachments/assets/4f63abbc-e5e9-435e-83cb-bafe6eb9883e

### 2. Human-in-the-Loop (HITL) Gate
*A DAG pauses at a checkpoint and waits for a human Approve/Reject before downstream jobs resume.*

https://github.com/user-attachments/assets/bbdb6772-a9ee-4094-85b4-0516a0543c62

### 3. HITL on a Complex Graph
*HITL gate mid-execution on a multi-branch pipeline.*

https://github.com/user-attachments/assets/724e3f3d-1d75-407e-a0c2-cd43f2939ad5

### 4. Agentic AI Workflow
*A multi-stage agent loop grouped into a single timeline in the Agent Runs view.*

https://github.com/user-attachments/assets/d6c40a50-9afc-45f6-abfc-b50c94324363

<details>
  <summary><b>View More: Dynamic DAG Execution, Reactive Scaling, GPU Routing, Fanout</b></summary>
  <br>

  **Control Plane: Dynamic DAG Execution**

  https://github.com/user-attachments/assets/5731c0b8-d392-4890-a3c5-f7e9cf611d65

  **Reactive Worker Scaling**

  https://github.com/user-attachments/assets/3f7d41df-654a-45d9-a49e-85978fad9172

  **GPU Affinity Routing**
  <video src="https://github.com/user-attachments/assets/9a1abc1c-d01f-4998-8c74-30409113ec85" controls="controls" style="max-width: 100%;"></video>

  **Parallel Execution (Fanout)**
  <video src="https://github.com/user-attachments/assets/812fd793-eab4-499e-9364-f1d3ccbbcc59" controls="controls" style="max-width: 100%;"></video>

  **Full Load Cycle (Scale Up & Descale)**
  <video src="https://github.com/user-attachments/assets/49afd1c6-bed3-444b-8d12-adff07832d8b" controls="controls" style="max-width: 100%;"></video>
</details>

---

## Quick Start

```bash
# 1. Build the engine
mvn clean package -DskipTests

# 2. Start the cluster
./titan-dev.sh up

# 3. Open the dashboard
open http://localhost:5000
```

Full guide: **[5-Minute Quickstart](https://ramn51.github.io/titan-orchestrator/getting-started/)**

---

## Cloud Deployment

Titan runs locally out of the box. When you're ready to move to the cloud, `package_cloud.sh` builds two deployment bundles from your local build:

```bash
./package_cloud.sh
# → titan-master-bundle.zip  (~2.3 MB) — everything needed on the Master VM
# → titan-worker-bundle.zip  (~120 KB) — Worker.jar + titan_sdk for remote workers
```

Two patterns are documented:

- **[Multi-VM Setup](https://ramn51.github.io/titan-orchestrator/deployment/cloud/)** — permanent cluster on GCP / AWS / Azure with open ports
- **[Remote GPU via SSH Tunnel](https://ramn51.github.io/titan-orchestrator/deployment/remote-gpu-worker/)** — keep your local machine as the Master, tunnel a RunPod or cloud GPU as a worker with no firewall changes

---

## Contributing

Titan is an experimental runtime engineered from first principles by a single developer. Bug reports, edge case findings, and contributions are highly encouraged.

[Report an Issue](https://github.com/ramn51/titan-orchestrator/issues) · [How to Contribute](https://ramn51.github.io/titan-orchestrator/contributing/)

---

## License

Licensed under the [Apache License 2.0](LICENSE).
© 2026 **Ram Narayanan A S**.

*Engineered from first principles to deconstruct the fundamental primitives of distributed orchestration.*
