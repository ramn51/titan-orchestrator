# Titan task granularity benchmark

This module measures the minimum useful task size for a controlled Titan
configuration. It follows the Task Bench `METG(50%)` convention and reports the
smallest tested task duration whose scheduler-window useful-work efficiency is
at least 50%.

It also provides optional suites for graph patterns, task-count scaling, a
single-host wait/compute overlap proxy, and load imbalance. The runner uses the
completed span journal for evidence and checks completion, dependency order,
duplicate attempts, and the observed worker count.

## Controlled run

Start Titan with autoscaling disabled and a known worker/slot configuration,
then run:

```powershell
python titan_test_suite/task_granularity_benchmark/runner.py --live `
  --configured-workers 1 --slots-per-worker 4 `
  --warmup-passes 1 --passes 5 `
  --durations-ms 10,20,50,100,200,500,1000,2000
```

The output directory contains `summary.json` and a self-contained `report.html`.
The warm-up pass for every duration is retained as audit evidence but excluded
from METG, latency percentiles, confidence intervals, and headline values. The
measured results report queue and execution p50/p95/p99, plus min/max, standard
deviation, coefficient of variation, and a bootstrap 95% confidence interval
for pass-level metrics.
Use `--dashboard-visible` when the synthetic runs should also appear in the
normal DAG dashboard. The default isolated journal keeps benchmark traffic out
of the product run list. After a dashboard-visible submission, refresh the DAG
Pipelines page once to discover the new run; the opened DAG view then polls live
status without a page reload.

## Interpretation

- The granularity sweep is an engine measurement. Python process launch is
  intentionally included because it is part of Titan's current execution path.
- The overlap suite is a local latency-overlap proxy. It is not a network
  communication-hiding result.
- METG depends on hardware, worker count, slots, executor, and network. Record
  all of them whenever publishing a result.
