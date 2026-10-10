# External agent benchmark fit for Titan

Assessment date: 2026-10-09.

## Decision

Use **ScienceAgentBench first**, after Titan exposes a tested versioned mutation
contract. Run a small pinned subset from the upstream `verified` split and keep
the official evaluator unchanged. Report scientific task quality separately
from Titan runtime correctness and performance.

Use **MLE-bench later on VMs with GPU capacity**. It is useful for long-running,
resource-aware ML experimentation, but it is too large and costly to serve as
the first agentic runtime test. Generic **AgentBench** is optional breadth rather
than a central scientific systems claim. **MLAgentBench** is an agent scaffold,
not a substitute for MLE-bench's external evaluation protocol.

## Comparison

| Benchmark | Fit for Titan | Strong use | What it does not establish | Recommendation |
|---|---:|---|---|---|
| [ScienceAgentBench](https://github.com/OSU-NLP-Group/ScienceAgentBench) | 9/10 | Authentic scientific data tasks; generate, execute, inspect, repair and grade Python programs | Distributed scheduling correctness, mutation races, worker scaling | First external pilot |
| [MLE-bench](https://github.com/openai/mle-bench) | 7/10 | Long ML experiments, parallel trials, resource placement, recovery and cost | A cheap local runtime benchmark | VM/GPU phase |
| [AgentBench](https://github.com/THUDM/AgentBench) | 5/10 | General multi-turn agent and environment interaction | Scientific workflow or DAG-specific claims | Optional breadth |
| [MLAgentBench](https://github.com/JunShern/MLAgentBench) | 6/10 | Possible iterative ML agent workload producer | Standard systems evaluation by itself | Adapter candidate |

## ScienceAgentBench evaluation

The official benchmark contains 102 tasks adapted from 44 peer-reviewed papers
across four disciplines. Each task asks an agent to produce a self-contained
Python program and grades execution output, task success, and cost. The current
upstream instructions require the verified dataset split and verified benchmark
artifacts. The complete data bundle must not be redistributed.

### Why it fits

The agent's natural loop maps cleanly to a Titan runtime graph:

```text
inspect data -> plan -> generate program -> execute -> validate
                                      ^                   |
                                      +------ repair <----+
```

Titan can add value by distributing inspection, execution, validation, and
alternative repair attempts; enforcing budgets; retaining artifacts; handling
worker loss; and showing the execution timeline. Runtime mutations matter when
the validator decides whether to finish, repair, or create a different analysis
branch.

### Adapter rule

Do not rewrite the tasks or grader. Build a thin adapter that:

1. reads a pinned upstream task and records its source revision;
2. creates one Titan run ID and a seeded initial plan;
3. turns tool calls into versioned DAG mutations;
4. stores programs and outputs as run artifacts;
5. invokes the unchanged upstream evaluator;
6. emits two result sections:
   - upstream task metrics: valid execution, success, cost and tokens;
   - Titan metrics: mutation latency, queue wait, execution time, retries,
     worker placement, graph size, duplicate admission and recovery.

Start with 6–10 tasks covering different disciplines and data formats. Use one
fixed model configuration and three agent seeds. Compare the same agent directly
against the upstream environment so Titan overhead and behavior are visible.

## MLE-bench evaluation

MLE-bench uses 75 Kaggle competitions. Its recommended full configuration is
resource heavy, and its low-complexity split still contains 22 competitions and
about 158 GB of data. It is therefore useful once remote workers and GPU labels
are available, not for today's laptop campaign.

The Titan-specific value is scheduling parallel experiments, capability-aware
GPU placement, trial cancellation, artifact movement, service readiness, and
recovery from worker loss. Start with one competition and a fixed time budget.
Only after that succeeds should the low-complexity split be considered.

## AgentBench and MLAgentBench

AgentBench covers eight interactive environments and mainly measures agent
reasoning and decision making. It can show that Titan is general, but it gives a
weaker scientific-runtime story than ScienceAgentBench.

MLAgentBench can supply an iterative ML agent whose experiments become Titan
jobs. Treat it as an agent implementation to host, while MLE-bench supplies the
stronger standardized task and grading layer.

## Required gate before the pilot

The current Titan SDK submits complete DAG payloads. Existing planner and healer
examples submit another DAG for each iteration. Before claiming native runtime
mutation, implement and test a run-scoped contract with:

- atomic mutation validation;
- stable run and mutation IDs;
- expected graph version;
- idempotent retries;
- parent completion before child registration;
- exactly-once readiness;
- cancellation and late callback behavior;
- a replayable mutation journal.

The local benchmark package in this directory is the reference oracle for that
gate. It does not change the production scheduler.
