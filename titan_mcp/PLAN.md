# Titan MCP control plane: execution and validation plan

## Boundary

The MCP integration is an optional Python stdio sidecar. It does not modify Titan Master, Scheduler, Worker, TitanStore, the binary protocol or the opcode list. It delegates to `TitanClient` and the existing `OP_STATS_JSON` projections.

## Tool policy

### Read-only profile

Cluster status, workers, services, queue lanes, job list/details/logs, execution timeline, metrics, persisted span history, service resolution, namespaced KV reads and set-member reads.

### Operator profile

Adds job/DAG submission, service submission, agent-stage metadata, cancellation, HITL decisions, namespaced KV writes and guarded asset transfer.

### Intentionally excluded

Worker termination, stats deletion, unrestricted Store keys, arbitrary command execution and filesystem access outside configured roots. Fault injection remains an explicit benchmark/operator action.

## Delivery phases

1. **Protocol and safety foundation:** stdio lifecycle, deterministic `tools/list`, structured results, operator gate, namespace enforcement and path confinement.
2. **Existing Titan operations:** observability projections, job/DAG submission, cancellation, HITL, services, assets and workflow KV.
3. **Contract validation:** unit and protocol tests using a fake gateway; malformed input, unknown tools, disconnected master, response bounds and filesystem attacks.
4. **Local live validation:** real master/store/worker; chain, fan-out/fan-in, service, HITL, cancellation, logs, assets and queue/timeline inspection through MCP only.
5. **Distributed validation:** two-VM master/store plus worker; submit from an MCP host, verify cross-machine execution, recovery visibility and artifact hashes.
6. **Client compatibility:** MCP Inspector plus at least two independent MCP hosts. Capture initialization version, advertised schema and every tool result.
7. **Reliability campaign:** master unavailable, Store unavailable, worker loss, partial response, timeout, duplicate call, cancellation race, late callback, oversized file, path traversal and malformed stats JSON.

## Merge gates

- Existing Java and Python suites pass without modification.
- MCP unit/contract suite passes on Windows and Linux.
- No stdout contamination during stdio operation.
- Read-only mode cannot perform mutations.
- Operator mode cannot access internal Store keys or files outside allowed roots.
- Every live mutation is visible through existing Titan dashboard and timeline.
- One 1000-task MCP-submitted DAG completes with the same correctness verifier as direct SDK submission.
- MCP and direct SDK results match for status, logs, services, cancellation and HITL.
- Disconnects return bounded tool errors and never crash the MCP process.

## Evidence to archive

Tool schemas, environment with secrets removed, exact commands, Git/JAR hashes, protocol transcripts, local and distributed result JSON, failure-injection logs, task counts, correctness oracle results, latency percentiles and an HTML summary generated from the raw evidence.

## Status on 2026-10-25

- Implemented as new files under `titan_mcp`; no engine, Store, worker, scheduler, dashboard, wire-format or opcode changes.
- 30 tools exposed through read-only and operator profiles.
- 20 dependency-free contract/protocol and scale tests pass, including a 2,000-node DAG validation case.
- Two-VM live smoke passes with one master/Store VM and one worker VM.
- Live checks passed for dry-run validation, capability discovery, dependency execution, DAG reconstruction, agent-stage grouping, pending approval discovery, bounded waiting, failure summaries, logs, namespaced KV, artifact SHA-256, cancellation, HITL, service readiness/resolution/stop and timeline visibility.
- Post-run state was idle: empty queue, no active job and no remaining service endpoint.
- Official MCP Inspector CLI compatibility passes for initialization, all 30 advertised schemas and read-only mutation rejection.
- Pending before a production claim: two additional independent-host compatibility checks, 1000-task load, full disconnection/failure campaign and Windows/macOS/Linux live matrix.
