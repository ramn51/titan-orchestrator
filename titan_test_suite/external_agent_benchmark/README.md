# External agent benchmark adapters

This module maps public agent workloads onto Titan without changing scheduler
semantics. It currently includes task 1 from the official LLMCompiler
ParallelQA dataset and `parallel_0` from BFCL V4.

## What the demonstration executes

```text
planner
  +-- lookup Mariana ---- transform 20% shallower --+
  +-- lookup Puerto Rico - transform 20% deeper ----+--> compare --> answer/verify
```

The lookup branches and transforms can execute concurrently.  The comparison
waits for both branches and the answer stage verifies the upstream label.

This initial mode uses a pinned reference plan and pinned factual values.  It
therefore tests Titan's execution of the public task's tool graph, dependency
joins, dashboard visibility and artifact production.  It does **not** measure
LLM planning quality, Wikipedia availability or the published LLMCompiler
accuracy result.  A later live-planner mode must keep the model, prompt,
temperature and retrieval implementation fixed across all compared runtimes.

## Run

```powershell
python -m titan_test_suite.external_agent_benchmark.runner --case 1 --host 127.0.0.1 --output benchmark_results/parallelqa-demo
```

Use the master's reachable address with `--host` for a remote cluster.  Run the
runner on the master host if its dashboard must discover the run through the
same local journal.

The BFCL graph can be run with:

```powershell
python -m titan_test_suite.external_agent_benchmark.bfcl_runner --case parallel_0 --host 127.0.0.1 --output benchmark_results/bfcl-demo
```

It compiles BFCL's two expected calls into parallel Titan tool jobs and verifies
their arguments at a join stage. The initial mode deliberately uses the pinned
ground truth and deterministic mock tools; it measures orchestration only. A
model-scoring mode must generate the calls without seeing the ground truth.

Live model scoring is available without writing the key to a file:

```powershell
$env:GEMINI_API_KEY = Read-Host "Gemini API key"
python -m titan_test_suite.external_agent_benchmark.bfcl_runner --case parallel_0 --planner gemini --host 127.0.0.1 --output benchmark_results/bfcl-gemini
Remove-Item Env:GEMINI_API_KEY
```

The summary records the model name and planned calls but never the API key.
