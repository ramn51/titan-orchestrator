# Runtime DAG mutation benchmark

This package defines and tests the semantics Titan needs before exposing true
in-run DAG mutation. It uses only the Python standard library and remains
separate from production scheduling code.

## Why it exists

Titan currently supports:

- registering a DAG before admitting its nodes;
- resolving a parent that completed before its child arrived;
- concurrent parent completion without duplicate child queueing;
- indexed cancellation cascades;
- agent loops that submit a new DAG for each iteration;
- research feedback loops that DEEPEN on evaluator gaps (on `master`);
- code review loops that spawn only the flagged fixers (on `master`);
- runtime-sized document, research, and competitive-intelligence fan-out.

Titan does not currently expose a run-scoped mutation transaction. The SDK and
master protocol have no mutation ID, expected graph version, or atomic mutation
response. Existing `dynamic_switch.py` selects a static graph before submission.
The planner, healer, research, and code-generation examples evolve their
workflows across separate DAG submissions. On `master`, `agent_run_id` groups
those DAGs in the Agent Runs UI but explicitly has no effect on scheduler
execution.

See [EXISTING_AGENT_EXAMPLES.md](EXISTING_AGENT_EXAMPLES.md) for the audited
inventory and the exact distinction between runtime workflow evolution and an
atomic mutation of one live DAG.

## Contract under test

A future mutation request should contain:

- `run_id`: the execution being changed;
- `mutation_id`: an idempotency key;
- `expected_version`: optimistic concurrency control;
- `add`: new nodes and their declared parent IDs;
- `cancel`: roots whose live descendants should be cancelled.

The whole request either applies or leaves the graph untouched. Parent IDs must
already exist or be present in the same mutation. Cycles, duplicate node IDs,
stale versions, and a reused mutation ID with different content are rejected.
An identical retried mutation returns its first result without applying twice.
Every eligible node becomes ready exactly once.

## Run it

From the repository root:

```powershell
python -m unittest titan_test_suite.dynamic_dag_benchmark.test_dynamic_dag -v

python -m titan_test_suite.dynamic_dag_benchmark.runner `
  --scenarios all `
  --passes 3 `
  --scale 64 `
  --seed 20261009 `
  --output-dir benchmark_results/dynamic-dag-local-2026-10-09

python -m titan_test_suite.dynamic_dag_benchmark.verify `
  benchmark_results/dynamic-dag-local-2026-10-09/events.jsonl `
  --expected benchmark_results/dynamic-dag-local-2026-10-09/expected.json `
  --json-out benchmark_results/dynamic-dag-local-2026-10-09/replay-verification.json
```

Live benchmarks use isolated DAG metadata by default. To make a live run appear
in the normal DAG Pipelines and Cluster Monitor pages, keep the dashboard and
master running and add:

```powershell
python -m titan_test_suite.dynamic_dag_benchmark.live_runner `
  --output-dir benchmark_results/live-visible `
  --passes 1 --scenarios all --dashboard-visible
```

This registers only the accepted DAG metadata in the normal append-oriented
journal. The report and verification evidence remain in the selected output
directory.

The live reliability runner creates dashboard-visible success, intentional
failure, cancellation cascade, HITL approval/rejection, capability release and
worker-loss recovery pipelines:

```powershell
python -m titan_test_suite.dynamic_dag_benchmark.live_reliability_runner `
  --output-dir benchmark_results/live-reliability-visible `
  --dashboard-visible
```

Use `--scenarios late-child,concurrent-mutation` to select cases. `--scale`
controls width or iteration count. `--passes` controls repetition. The report
contains reference-model timings only; it must not be presented as Titan engine
throughput.

## Live integration gate

Do not connect this factory to production until the master has a run-scoped,
atomic mutation contract. Reusing plain `OP_SUBMIT_DAG` with global job IDs would
leave ambiguous retry, redeploy, and concurrent-writer behavior. The preferred
future design can extend the existing DAG submission command with a versioned
envelope rather than consuming another opcode, while preserving the legacy
payload for current clients.

## ScienceAgentBench fit

[ScienceAgentBench](https://github.com/OSU-NLP-Group/ScienceAgentBench) is a
useful application-level layer after this scheduler contract exists. Its 102
scientific tasks assess generated Python programs, execution output, and agent
cost across four disciplines. It does not exercise scheduler admission races or
graph mutation correctness on its own. A Titan adapter should turn each agent
trajectory into versioned tool and evaluation nodes, then retain the benchmark's
official evaluator unchanged.
