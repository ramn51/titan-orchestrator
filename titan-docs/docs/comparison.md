# How Titan Compares

Titan is designed to cover three capability tiers in a single binary — task scheduling, service orchestration, and agentic execution. Most orchestrators specialise in one of these and do it very well; Titan trades depth for breadth. The tables below help you decide whether that tradeoff makes sense for your use case.

!!! tip "TL;DR"
    Titan is a **distributed task scheduler**, a **service orchestrator**, and an **agentic runtime** — in one lightweight binary. It does not replace Kubernetes or Temporal in production today, and it is not a managed service. It is a self-hosted, single-Master, research-grade runtime (v1.0) designed for teams that want to own their execution substrate.

---

## The three tiers Titan covers

| Tier | Mental Model | What You Run | Status |
|------|-------------|--------------|--------|
| **T1 — Distributed Task Scheduler** | Job runner / batch orchestrator | One-off scripts, parallel batch jobs, static DAGs (YAML or SDK), GPU/CPU-routed workloads | ✅ Available |
| **T2 — Service Orchestrator** | Lightweight platform | Long-running APIs, daemons, persistent workers with auto-restart and port management | ✅ Available |
| **T3 — Agentic Runtime** | Autonomous execution substrate | Self-mutating DAGs, LLM-driven agents, stateful recursive workers, multi-agent pipelines with HITL gates | ✅ Available |

!!! note "On scheduling primitives"
    Titan ships with a `DelayQueue`-based `ClockWatcher` thread for delayed and one-off scheduled execution at millisecond precision. **Cron-expression style recurring schedules (`0 */5 * * *`) are on the roadmap.** For now, treat Titan as event-driven and on-demand, not as a drop-in cron replacement.

---

## Is Titan for me?

| If you want to... | Titan is... | Better alternatives |
|---|---|---|
| Run a few Python scripts on one machine | **Overkill** | cron, APScheduler |
| Orchestrate ETL with strong data lineage and asset awareness | **Not the right tool** | Dagster, Prefect |
| Run mission-critical production workflows with HA guarantees today | **Too early** (Master is currently SPOF, Raft on v2 roadmap) | Temporal, Airflow |
| Build agentic systems that spawn tasks dynamically at runtime | **A strong fit** | LangGraph (single-process), Ray (heavier) |
| Distribute compute across heterogeneous nodes (CPU/GPU) without Kubernetes | **A strong fit** | Nomad, Ray |
| Pause a DAG for human approval mid-run | **A strong fit** (native HITL) | Airflow (custom sensors), Prefect |
| Mix long-running services and batch jobs in one runtime | **A strong fit** | Nomad |
| Learn how distributed orchestrators work under the hood | **An excellent fit** | (most alternatives are black boxes) |

---

## What Titan is vs. what Titan isn't

| Titan **is** | Titan **is not** |
|---|---|
| A lightweight distributed execution runtime | A managed cloud service |
| A research / experimental engine at v1.0 | A production-hardened scheduler |
| Planner-agnostic (you bring the logic) | An opinionated framework with prebuilt operators |
| Capability-aware (GPU/CPU/custom tags) | A Kubernetes replacement |
| Process-isolated by default | Container-isolated (Docker is on the v2 roadmap) |
| Single-Master topology today | Highly available today (Raft is v2) |
| Built for dynamic, mutating DAGs and agents | A static-only DAG runner |

---

## Landscape comparison

Where Titan sits relative to the tools engineers most often consider alongside it. **Honest scoring** — Titan loses some rows on purpose.

| Capability | **Titan** | Airflow | Dagster | Prefect | Temporal | Ray | Nomad |
|---|---|---|---|---|---|---|---|
| **Primary model** | Distributed DAG + service + agent runtime | Static DAG scheduler | Asset-oriented orchestrator | DAG/flow runner | Durable workflow engine | Distributed compute framework | Cluster scheduler |
| **Static DAGs (YAML)** | ✅ | ✅ | ✅ | ✅ | ⚠️ Code only | ⚠️ Code only | ⚠️ Job specs |
| **Static DAGs (programmatic SDK)** | ✅ | ✅ Python | ✅ Python | ✅ Python | ✅ Multi-lang | ✅ Python | ❌ |
| **Dynamic DAG mutation at runtime** | ✅ Native | ⚠️ DynamicTaskMapping (limited) | ⚠️ Dynamic graphs | ✅ | ✅ | ✅ | ❌ |
| **Agentic workflows (tasks spawn tasks at runtime)** | ✅ Native | ❌ | ❌ | ⚠️ Possible | ⚠️ Possible | ✅ | ❌ |
| **Capability-based routing (GPU/CPU/custom tags)** | ✅ Native | ⚠️ Queues / pools | ⚠️ Tags | ⚠️ Work pools | ❌ | ✅ | ✅ |
| **Affinity-based routing (worker tag matching)** | ✅ Native | ❌ | ❌ | ❌ | ❌ | ⚠️ | ⚠️ |
| **Long-running services + batch in one runtime** | ✅ | ❌ | ❌ | ❌ | ⚠️ | ✅ | ✅ |
| **Cron / recurring schedules** | 🔜 Roadmap (delayed exec available today) | ✅ | ✅ | ✅ | ✅ | ⚠️ | ⚠️ |
| **Human-in-the-loop gates** | ✅ Native (Approve / Reject in dashboard) | ⚠️ Custom sensors | ⚠️ Custom | ⚠️ Custom | ⚠️ Signals | ❌ | ❌ |
| **Visual DAG builder (drag-and-drop)** | ✅ Native | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Live DAG visualizer with per-node status** | ✅ Native | ✅ | ✅ | ✅ | ✅ | ⚠️ | ❌ |
| **Master HA / leader election** | ❌ v2 roadmap | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **mTLS / auth** | ❌ v2 roadmap | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Container isolation** | ❌ v2 roadmap (process isolation today) | ✅ | ✅ | ✅ | N/A | ⚠️ | ✅ |
| **External deps to run core** | None (Java + optional Redis-like store) | Postgres, scheduler, webserver | Postgres, daemon | Server, agents | Cassandra / SQL, history service | Head node + workers | Consul (optional) |
| **Time to first job** | ~1 command (`./titan-dev up`) | High | Medium | Medium | High | Medium | Medium |
| **Production-ready** | ❌ Research v1.0 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

!!! warning "Read me first"
    This table compares **categories and capabilities** — not raw quality. Airflow, Dagster, Prefect, and Temporal are mature production systems with thousands of contributors. Titan is a single-developer research runtime that explores the *primitives* of orchestration without their operational weight. Pick the right tool for your risk tolerance.

---

## What Titan replaces (or tries to)

| Reaching for... | Why people reach for it | What Titan offers instead | What you lose |
|---|---|---|---|
| **Airflow** for distributed Python DAGs | Mature scheduler, big community | Lighter footprint, native dynamic DAGs, no Postgres required, agentic-first | Mature ecosystem, plugin library, recurring cron today |
| **Celery** for distributed task queues | Simple worker pool | Capability-aware routing, DAG dependencies, visual dashboard, HITL gates | Broker flexibility (Redis/RabbitMQ choice), language SDKs |
| **Nomad** for mixed batch + service workloads | Single binary, multi-workload | Same multi-workload model with DAG dependencies and live visualization baked in | Production hardening, ACLs, Vault integration |
| **Ray** for agentic / dynamic compute | Distributed Python primitives | DAG-first declarative deps, YAML option, built-in dashboard | Actor model, GPU memory management, ML ecosystem |
| **Kubernetes Jobs + CronJobs** | Ubiquitous infra | Zero infra to install; works on bare VMs or laptops; DAG semantics built in | K8s ecosystem, ingress, service mesh, secrets management |
| **LangGraph / agent frameworks** | In-process agent orchestration | Agent logic runs distributed across worker nodes; TitanStore for cross-node state; HITL gates | LangGraph's tight LLM provider integration, prebuilt agent patterns, single-process simplicity |
| **A custom in-house DAG runner** | Full control | Skip rebuilding scheduling, retry, fan-out, HITL, and dashboarding from scratch | Total control over every primitive |

---

## Feature matrix by user type

| You are a... | You probably want | Start here |
|---|---|---|
| **Backend engineer** exploring distributed systems | Architecture deep-dive, TITAN_PROTO, AOF | [Architecture → System Design](architecture/design.md) |
| **ML engineer** running training / inference pipelines | YAML DAGs, GPU routing, HITL gates | [Examples → Static Pipelines](examples/yaml.md), [HITL](examples/hitl.md) |
| **AI / agent developer** building autonomous workflows | Python SDK, dynamic DAGs, multi-agent example | [Examples → Multi-Agent Research](examples/research-pipeline.md) |
| **Platform engineer** evaluating orchestrators | This page, limitations, roadmap | [Architecture → Limitations](architecture/design.md#6-limitations--design-constraints) |
| **Researcher / learner** wanting to read clean code | Java engine docs, Architecture | [Architecture → Internals](architecture/internals.md), [JavaDocs](javadocs/docs/index.html) |
| **First-time visitor** just curious | 5-minute quickstart | [Getting Started](getting-started.md) |

---

## Capability tiers in detail

### Tier 1 — Distributed Task Scheduler

You can use Titan purely as a remote job runner. The CLI and Python SDK let you submit standalone scripts, static YAML pipelines, or programmatically defined DAGs to the cluster — Titan handles dispatch, capability routing, retries, and crash recovery.

**Use this tier when:** you have batch workloads, ETL pipelines, ML training jobs, or any "run this graph of scripts across these machines" problem.

**Key features:** YAML pipelines, Python SDK, GPU/CPU capability tags, affinity routing, AOF-backed state recovery on Master restart, fan-out/fan-in patterns.

[→ Static Pipelines (YAML)](examples/yaml.md) · [→ Parallel Analyst Pipeline](examples/comp-intel-pipeline.md)

### Tier 2 — Service Orchestrator

The same runtime that schedules batch jobs can deploy long-running services. Pass `isPermanent=true` to a worker and it becomes a protected node; deploy a service payload to it with port management and auto-restart on crash.

**Use this tier when:** you want a single substrate for both your batch jobs and your sidecar / always-on services — similar to what Nomad provides for mixed workloads.

**Key features:** Permanent workers, port management, capability tagging for service nodes. Workers re-register with the Master after job completion — note this is process re-registration, not OS-level service management.

[→ Getting Started](getting-started.md)

### Tier 3 — Agentic Runtime

Titan's agentic mode. Tasks can inspect logs, call LLMs, mutate the DAG at runtime, spawn new tasks on different hardware, and use TitanStore as shared agent memory. Combined with native HITL gates, this is a runtime for distributed agent workflows with dynamic task generation and human-in-the-loop checkpoints.

**Use this tier when:** you're building agentic systems that need to run distributed (not in one Python process), with dynamic task generation, shared state, and human-in-the-loop checkpoints.

**Key features:** Dynamic DAG mutation, LLM-driven self-healing loops, TitanStore as agent memory, HITL Approve/Reject gates, multi-agent fan-out and synthesis.

[→ Agentic Examples](examples/agentic.md) · [→ Multi-Agent Research Pipeline](examples/research-pipeline.md) · [→ HITL Pipelines](examples/hitl.md)

---

## When *not* to use Titan

- **You need production-grade HA today.** The Master is a SPOF in v1. Raft-based leader election is on the v2 roadmap.
- **You need encrypted, authenticated cluster comms over a public network.** mTLS is on the v2 roadmap; run Titan strictly inside a trusted VPC/LAN for now.
- **Your workloads need strict filesystem isolation.** Titan uses process-level isolation today; Docker drivers are on the v2 roadmap.
- **You need a managed service or commercial support.** Titan is a solo OSS project under Apache 2.0 with community contributions only.
- **Your team is allergic to running a JVM.** The control plane is Java 17+.

---

[← Back to Home](index.md) · [🚀 5-Minute Quickstart](getting-started.md) · [🧠 Architecture Deep Dive](architecture/design.md)
