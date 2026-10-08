# WfCommons benchmark

`benchmark.py` reads an official WfFormat instance, validates its graph, and
reports its shape. Optional live mode submits a small parent-closed sample to
Titan. Each task runs `stage.py`, a tiny surrogate. This measures orchestration,
not the original scientific programs, runtime, memory, or file I/O.

Source: [WfInstances](https://github.com/wfcommons/WfInstances/blob/main/README.md). The adapter accepts official Montage, Seismology, SoyKB, and 1000Genome JSON instances. Download a file from the matching `pegasus/<family>/` directory and run:

```powershell
python titan_test_suite/wfcommons_benchmark/benchmark.py path/to/montage-chameleon-2mass-025d-001.json --output wfcommons-inspect.json
python titan_test_suite/wfcommons_benchmark/benchmark.py path/to/montage-chameleon-2mass-025d-001.json --live --max-tasks 619 --output wfcommons-live.json
```

Live mode caps the graph at 10,000 tasks and each surrogate at 2 seconds. Set
`--max-tasks` to the instance task count for a full shape check, or use 24 for
a quick parent-closed smoke test. Large local runs should be preceded by an idle
cluster, memory, disk, and persisted-span preflight. It runs the source graph using a fixed-duration
surrogate; it does not run the scientific applications or replay task measurements.
For controlled capacity tests, set `titan.worker.slots`, `titan.scheduler.autoscale`,
and `titan.scheduler.max.workers` in `config.properties` before starting workers
and master. Record the live fleet and advertised slot counts for every run.

## Verify persisted results

`verify.py` checks benchmark evidence without contacting a live Titan process.
It reconstructs every expected Titan job ID and parent edge from the original
WfFormat input, then verifies the persisted span journal, final status, attempts,
timestamps, and the input SHA-256 stored in the benchmark result. It exits with
status `0` only when every supplied case passes, `1` for a correctness failure,
and `2` for an unreadable input.

```powershell
python titan_test_suite/wfcommons_benchmark/verify.py `
  --case path/to/montage.json path/to/montage-result.json `
  --case path/to/seismology.json path/to/seismology-result.json `
  --spans telemetry/spans-2026-09-21.jsonl `
  --telemetry outputs/cluster-final.json `
  --manifest outputs/titan_dag_manifest.json `
  --json-out outputs/verification.json `
  --html-out outputs/verification.html
```

Pass more than one day-partitioned journal after `--spans` when a run crosses
midnight. Retries and duplicate span records fail verification by default. Use
`--allow-retries` only when retries are an expected part of the test. Use
`--exclude-timing DAG_NAME` when a known interruption makes elapsed performance
invalid; correctness checks still run.

Each run section reports total benchmark runtime, the span execution window,
workers that executed tasks, peak concurrent slots, and a slot-occupancy graph.
When `--telemetry` is supplied, it also graphs actual registered worker count,
total advertised capacity, and used slots during that run. Without telemetry,
the report does not infer fleet scaling from job spans.

New benchmark results embed the exact submitted job-to-source-task mapping, so
sampled graphs are reproducible and independently checked against WfFormat.
`--manifest` is a fallback for older result files that predate that mapping; in
that mode the report clearly states that it verified the deployed graph shape
but could not map every sampled node back to a WfFormat task ID.
