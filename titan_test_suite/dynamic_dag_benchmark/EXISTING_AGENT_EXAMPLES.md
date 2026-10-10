# Existing Titan agent workflow inventory

This inventory distinguishes application-level workflow evolution from a
scheduler mutation of one live DAG. Both are useful, but they prove different
properties.

## Result

Titan already contains meaningful dynamic agent applications. The research and
code-generation agents on `master` make decisions from prior results and submit
new work whose shape was not known at the start. The document agent chooses its
fan-out width from a repository scan. The planner and healer repeat this pattern
on the current branch.

These examples evolve a logical workflow **across multiple DAG submissions** or
construct a runtime-sized DAG immediately before submission. They do not mutate
an already admitted DAG as one atomic scheduler operation. On `master`, a shared
`agent_run_id` reconstructs the stages in the Agent Runs dashboard; the SDK
states that this value has no effect on scheduling or execution.

## Audited examples

| Example | Location | Decision made at runtime | Submission model | Benchmark mapping |
|---|---|---|---|---|
| LLM planner | Current branch: `titan_test_suite/examples/agents_examples/demo_agentic_planner.py` | LLM generates the next set of jobs and dependencies after observing logs | One new DAG per iteration | `iterative-agent` |
| Code healer | Current branch: `titan_test_suite/examples/agents_examples/code_healer_agent.py` and `perm_files/pytests/self_healer_agent_tests/code_healer.py` | Diagnose/read/fix/verify actions depend on prior execution logs | One new DAG per repair cycle | `recursive-agent` |
| Document generator | Current branch: `titan_test_suite/examples/agents_examples/doc_generator_agent/tools/doc_dispatcher.py` | Repository scan determines the count of file agents | One runtime-sized DAG; no feedback loop | `document-agent-fanout` |
| Unified mock agent | Current branch: `perm_files/pytests/agentic_ai_tests_static/unified_agent.py` | Mock architect or ReAct policy produces the next iteration | Separate DAG per iteration | `iterative-agent` |
| Recursive pattern | Current docs: `titan-docs/docs/examples/agentic.md` | A worker reads shared state and submits successor work | Individual job submissions from a worker | `recursive-agent` |
| Research agent | `master`: `titan_test_suite/examples/agents_examples/research_agent/research_agent.py` | Evaluator returns `SYNTHESIZE` or `DEEPEN`; gaps become the next researcher topics | PLAN, ITER, EVAL, and SYNTH are separate DAGs sharing `agent_run_id` | `research-agent-loop` |
| Code-generation agent | `master`: `titan_test_suite/examples/agents_examples/code_gen_agent/code_gen_agent.py` | Reviewer returns `APPROVE` or `REQUEST_CHANGES`; only affected components receive fixers | PLAN, GEN, REVIEW, FIX, and INTEGRATE are separate DAGs sharing `agent_run_id` | `code-review-repair` |
| Research pipeline with HITL | `master`: `titan_test_suite/examples/agents_examples/research_pipeline/research_pipeline.py` | Planner determines subtopics and therefore parallel width | Planner DAG followed by a research/HITL/synthesis DAG | `document-agent-fanout` plus HITL tests |
| Competitive intelligence | `master`: `titan_test_suite/examples/agents_examples/comp_intel/comp_intel_pipeline.py` | User-selected frameworks determine parallel analysts | One runtime-sized fan-out/fan-in DAG | `fan-out-in` |

There is no separate repository-root `titan_examples` directory in the audited
checkout. The examples are split between `titan_test_suite`, `perm_files/pytests`,
and `titan-docs`. The richer research and code-generation directories are
present on `master` and absent from `scheduler-quick-wins`.

## What the local reference benchmark now covers

- `research-agent-loop`: repeated planner-driven breadth, evaluation, and
  eventual synthesis.
- `code-review-repair`: selective branch creation for flagged components.
- `document-agent-fanout`: runtime width followed by a complete fan-in.
- `recursive-agent`: one successor admitted per observation cycle.
- `conditional-agent`: exactly one deterministic branch is admitted.
- `concurrent-mutation`: many independent producers mutate the same run without
  lost updates.

The independent event replayer checks dependency ordering, graph versions,
terminal state, graph identity, and exactly-once readiness for every scenario.

## Production integration boundary

The next engine capability should be a run-scoped, versioned mutation envelope:
`run_id`, `mutation_id`, `expected_version`, additions, and cancellations. It can
extend the existing DAG submission command to avoid another protocol opcode.
Legacy DAG payloads should keep their current behavior. Until that contract
exists, the existing examples are valid orchestrator-driven agent workflows but
should not be described as atomic mutation of one live scheduler graph.
