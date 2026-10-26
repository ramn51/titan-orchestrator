# Benchmarking Titan

Titan's benchmark portfolio evaluates four separate questions:

1. **Correctness:** are dependencies released once and in the right order?
2. **Distributed execution:** does the same runtime work across machines and worker layouts?
3. **Recovery:** what happens when workers, the master, Store connections or callbacks fail?
4. **Agentic execution:** can runtime-planned stages become observable and recoverable DAGs?

[Open the full interactive benchmark report](titan-benchmark-report.html){ .md-button .md-button--primary }

The report is the evidence source. This page explains why the measurements matter to Titan and how to interpret them.

!!! note "Report snapshot"
    The linked portfolio contains benchmark evidence recorded through October 11, 2026. Newer MCP verification is documented separately in the [MCP Control Plane](../integrations/mcp.md).

!!! info "Living evidence portfolio"
    The report evolves as Titan is tested on additional workflows, topologies and failure cases. Headline totals can increase or be refined when new runs are verified; every published snapshot states its evidence cutoff so its numbers remain interpretable.

## Evidence at a glance

| Evidence | Recorded result | What it establishes |
| --- | ---: | --- |
| Selected workflow ledger | 56,432 jobs | A conservative count of non-overlapping WfCommons evidence |
| Dependency edges | 129,596 | Dependency release was exercised substantially beyond small examples |
| Selected WfCommons DAG runs | 44 | Repeated coverage across multiple scientific graph families |
| Workflow families | 4 | Montage, Seismology, SoyKB and 1000Genome |
| Largest strict run | 9,805 jobs / 24,027 edges | Large DAG admission, dependency tracking and completion |
| Distributed GCP workload runs | 35 | 24 two-VM and 11 three-VM workload runs |
| Local lifecycle and fault cases | 14/14 passed | Expected terminal state for the exercised single-machine cases |
| Two-VM fault campaign | 5/7 complete passes | Five cases satisfied every invariant; two produced actionable findings |
| Three-VM fault campaign | 6/8 complete passes | Recovery across two worker hosts, with two actionable findings |
| Observed throughput range | 5.7–16 tasks/s | Current operating range across different graphs and configurations |

The selected workflow count excludes scaling-only cells, graph-shape probes, data-flow probes, recovery cases, unit tests and model-only DAGs. It should not be interpreted as Titan's lifetime job count.

## Scientific workflow correctness

Titan ran graph structures from four WfCommons families:

- **Montage:** wide astronomy workflows with substantial fan-out and fan-in
- **Seismology:** large convergence points and high parent counts
- **SoyKB:** deeper genomics-style processing stages
- **1000Genome:** multi-stage genomics graphs with several roots and leaves

These workflows matter because they exercise different scheduler risks. Wide graphs pressure admission and worker capacity. Deep graphs amplify release latency. Fan-in graphs test concurrent parent completion and exactly-once child admission.

The largest completed graph contained 9,805 jobs and 24,027 edges with no recorded dependency-order violation.

!!! note "Scope"
    The WfCommons runs preserve real workflow graph structures but mostly use short Python surrogate tasks. They test Titan's scheduling, dependencies, RPC, callbacks and persistence. They do not claim performance for the original scientific binaries.

## Single-machine evidence

The local campaign places the master, Store, dashboard and worker processes on one Windows machine. It provides the broadest coverage of:

- Workflow families and DAG shapes
- Repeatability
- Slot and worker-process configurations
- Task granularity
- Dynamic agent stages
- Lifecycle and fault behavior

Local results are useful for correctness and scheduler analysis. They are not evidence of cross-machine network behavior.

## Distributed GCP evidence

The report separates two topologies rather than averaging them.

### Two VMs

- VM 1: Titan master, TitanStore and dashboard
- VM 2: remote worker processes

This topology verifies remote execution, cross-VM control traffic, scientific workflows, agentic cases, concurrency and selected recovery paths. It also measures slot and worker-process layouts on one worker host.

### Three VMs

- VM 1: Titan master, TitanStore and dashboard
- VM 2: worker host
- VM 3: second worker host

This topology adds real worker-host distribution. It covers controlled WfCommons execution on both hosts, concurrent Montage and SoyKB submissions, cross-worker KV and artifact transfer, and failures where work can move to a surviving worker.

Numbers from the two topologies are not combined. A fair comparison requires the same graph, surrogate duration, runtime version, worker layout and hardware.

## Performance interpretation

Observed throughput ranges from 5.7 to 16 tasks per second across workflow shapes and configurations.

This is useful as a measured operating range, not as a universal Titan throughput number. Short tasks are affected by process startup, RPC, persistence, queueing and callback overhead. Different DAG shapes also expose different amounts of parallel work.

For Titan, the current performance evidence means:

- The runtime completes large DAGs on local and distributed deployments.
- Additional workers help only when the graph exposes enough ready work and the dispatch path can feed them.
- Slot count alone does not guarantee speedup.
- Extremely short tasks expose orchestration overhead more strongly than scientific tasks lasting seconds or minutes.

The report keeps performance separate from correctness. A slower completed run can still be strong correctness evidence.

## Recovery and fault injection

Fault campaigns cover duplicate callbacks, late callbacks after cancellation, Store interruption, master interruption, service readiness failure, descendant-process cleanup and worker restart behavior.

A case passes only when all stated invariants hold. The distributed campaigns therefore retain findings instead of counting partial recovery as success.

The findings are useful engineering evidence:

- A terminal status write missed during a Store outage was not reconciled after reconnect.
- A stale service endpoint could remain advertised after a worker restarted quickly.

These results do not invalidate the passing cases. They identify the exact recovery boundaries that need work before a stronger availability claim.

## Agentic runtime evidence

Agentic tests separate two concerns:

- **Planning:** the model chooses tools, arguments, stages or branches.
- **Execution:** Titan validates, schedules, runs and records the submitted stages.

Model answer accuracy is not presented as scheduler correctness. Titan's relevant evidence is whether every submitted stage is represented as a DAG, executed under the normal lifecycle and visible through agent-run and DAG views.

This supports Titan's main agentic-runtime claim: nondeterministic planning can produce dynamic work while execution remains observable and governed.

## Data-flow evidence

The data-flow suites check more than process completion:

- Parent tasks produce deterministic data.
- Children read and transform it.
- Fan-in tasks combine the results.
- Verifiers compare filenames, sizes and SHA-256 hashes.
- Three-VM cases distinguish shared-host files from explicit cross-worker data paths.

This matters because dependency correctness alone does not prove that downstream tasks received usable data.

## What is established

- Titan executes multiple real scientific graph structures at useful scale.
- Dependency ordering held for the selected evidence.
- The runtime operates across one, two and three machines.
- Dynamic agent stages are executed and traced as normal Titan DAGs.
- Cancellation, approval, service lifecycle and several failure paths work end to end.
- Fault injection exposed concrete recovery gaps instead of hiding them.

!!! warning "Evidence limits"
    The portfolio does not yet support claims of complete runtime comparison, production high availability, or performance for the original scientific binaries. The interactive report records the exact limitations beside each result.

## Reading the full report

The interactive report contains:

- Exact workflow and topology breakdowns
- Per-run configuration and evidence strength
- Scientific DAG visualizations
- Local, two-VM and three-VM sections
- Throughput, scaling and latency charts
- Agent-run diagrams
- Expandable fault-injection flows
- Known limitations and pending work

[Open the full interactive benchmark report](titan-benchmark-report.html){ .md-button }
