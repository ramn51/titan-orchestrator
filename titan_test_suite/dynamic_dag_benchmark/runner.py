"""Run deterministic, dependency-free dynamic DAG correctness benchmarks."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import html
import json
from pathlib import Path
import statistics
import time

from .model import GraphRun, Mutation, MutationRejected, NodeSpec
from .scenarios import SCENARIOS, run_scenario
from .verify import verify_events


def contract_probes() -> dict:
    probes = {}

    run = GraphRun("probe-idempotency")
    mutation = Mutation.create("m1", [NodeSpec.create("root")], expected_version=0)
    first = run.apply(mutation)
    second = run.apply(mutation)
    probes["idempotent-retry"] = first["applied"] and second["duplicate"] and run.version == 1
    try:
        run.apply(Mutation.create("m1", [NodeSpec.create("other")]))
        probes["mutation-id-conflict"] = False
    except MutationRejected:
        probes["mutation-id-conflict"] = len(run.nodes) == 1

    try:
        run.apply(Mutation.create("stale", [NodeSpec.create("stale-node")], expected_version=0))
        probes["stale-version"] = False
    except MutationRejected:
        probes["stale-version"] = "stale-node" not in run.nodes

    atomic = GraphRun("probe-atomic")
    try:
        atomic.apply(Mutation.create("cycle", [
            NodeSpec.create("left", ["right"]),
            NodeSpec.create("right", ["left"]),
        ], expected_version=0))
        probes["cycle-is-atomic"] = False
    except MutationRejected:
        probes["cycle-is-atomic"] = atomic.version == 0 and not atomic.nodes
    try:
        atomic.apply(Mutation.create("missing", [NodeSpec.create("child", ["absent"])],
                                     expected_version=0))
        probes["missing-parent-is-atomic"] = False
    except MutationRejected:
        probes["missing-parent-is-atomic"] = atomic.version == 0 and not atomic.nodes
    return probes


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def aggregate(cases: list[dict]) -> list[dict]:
    groups = {}
    for case in cases:
        groups.setdefault(case["scenario"], []).append(case)
    rows = []
    for scenario, members in groups.items():
        times = [member["wall_ms"] for member in members]
        rows.append({
            "scenario": scenario,
            "passes": len(members),
            "nodes": max(member["snapshot"]["nodes"] for member in members),
            "edges": max(member["snapshot"]["edges"] for member in members),
            "median_ms": statistics.median(times),
            "p95_ms": _percentile(times, .95),
            "median_nodes_per_second": statistics.median(
                member["nodes_per_second"] for member in members),
            "passed": all(member["verification"]["passed"] for member in members),
        })
    return sorted(rows, key=lambda item: item["scenario"])


def render_html(report: dict) -> str:
    rows = report["aggregate"]
    max_time = max((row["median_ms"] for row in rows), default=1)
    scenario_rows = []
    for row in rows:
        width = max(2, row["median_ms"] / max_time * 100)
        verdict = "PASS" if row["passed"] else "FAIL"
        scenario_rows.append(f"""
          <tr><td><strong>{html.escape(row['scenario'])}</strong></td><td>{row['passes']}</td>
          <td>{row['nodes']:,}</td><td>{row['edges']:,}</td>
          <td><div class="bar"><span style="width:{width:.1f}%"></span></div>
              {row['median_ms']:.3f} ms</td><td>{row['p95_ms']:.3f} ms</td>
          <td>{row['median_nodes_per_second']:,.0f}</td>
          <td class="{'ok' if row['passed'] else 'bad'}">{verdict}</td></tr>""")
    probe_rows = "".join(
        f"<li><span>{html.escape(name.replace('-', ' ').title())}</span>"
        f"<b class=\"{'ok' if passed else 'bad'}\">{'PASS' if passed else 'FAIL'}</b></li>"
        for name, passed in report["contract_probes"].items())
    audit_rows = "".join(
        f"<tr><td>{html.escape(item['capability'])}</td><td>{html.escape(item['current'])}</td>"
        f"<td>{html.escape(item['evidence'])}</td></tr>" for item in report["titan_audit"])
    external_rows = "".join(
        f"<tr><td><strong>{html.escape(item['name'])}</strong></td>"
        f"<td>{item['fit']}/10</td><td>{html.escape(item['use'])}</td>"
        f"<td>{html.escape(item['limit'])}</td><td>{html.escape(item['decision'])}</td></tr>"
        for item in report["external_benchmarks"])
    agent_rows = "".join(
        f"<tr><td><strong>{html.escape(item['example'])}</strong></td>"
        f"<td>{html.escape(item['branch'])}</td><td>{html.escape(item['shape'])}</td>"
        f"<td>{html.escape(item['boundary'])}</td>"
        f"<td><code>{html.escape(item['scenario'])}</code></td></tr>"
        for item in report["agent_examples"])
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Titan Runtime DAG Benchmark</title><style>
:root{{--bg:#08110f;--panel:#101d1a;--line:#284239;--text:#eaf7f1;--muted:#9ab5aa;--green:#43e8aa;--red:#ff6f86;--amber:#ffc75f}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 Inter,Segoe UI,sans-serif}}
main{{max-width:1180px;margin:auto;padding:28px}}h1{{font-size:28px;margin:0 0 5px}}h2{{font-size:18px;margin:0 0 12px}}
.muted{{color:var(--muted)}}.grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:22px 0}}
.card,section{{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px}}.card b{{display:block;font-size:24px}}
section{{margin:14px 0}}table{{width:100%;border-collapse:collapse}}th,td{{padding:9px 8px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top}}
th{{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.06em}}.ok{{color:var(--green)}}.bad{{color:var(--red)}}
.bar{{display:inline-block;width:110px;height:7px;background:#20312c;border-radius:9px;margin-right:8px;vertical-align:middle;overflow:hidden}}
.bar span{{display:block;height:100%;background:var(--green)}}ul.checks{{list-style:none;padding:0;margin:0;display:grid;grid-template-columns:repeat(2,1fr);gap:8px}}
.checks li{{display:flex;justify-content:space-between;border:1px solid var(--line);padding:9px;border-radius:7px}}code{{color:#b8f5dc}}
details{{border-top:1px solid var(--line);padding-top:10px;margin-top:10px}}a{{color:var(--green)}}
@media(max-width:800px){{.grid{{grid-template-columns:repeat(2,1fr)}}main{{padding:14px}}.table-wrap{{overflow:auto}}}}
</style></head><body><main>
<p class="muted">TITAN · CORRECTNESS-FIRST BENCHMARK</p><h1>Runtime DAG mutation foundation</h1>
<p class="muted">Generated {html.escape(report['generated_at'])}. This measures the dependency-free reference model and verifier. It is not a production scheduler throughput claim.</p>
<div class="grid"><div class="card"><span>Verdict</span><b class="{'ok' if report['passed'] else 'bad'}">{'PASS' if report['passed'] else 'FAIL'}</b></div>
<div class="card"><span>Cases</span><b>{len(report['cases'])}</b></div><div class="card"><span>Events verified</span><b>{report['events']:,}</b></div>
<div class="card"><span>Maximum graph</span><b>{max((c['snapshot']['nodes'] for c in report['cases']),default=0):,} nodes</b></div></div>
<section><h2>What this establishes</h2><ul class="checks">{probe_rows}</ul></section>
<section><h2>Scenario results</h2><div class="table-wrap"><table><thead><tr><th>Scenario</th><th>Passes</th><th>Nodes</th><th>Edges</th><th>Median</th><th>P95</th><th>Nodes/s</th><th>Result</th></tr></thead><tbody>{''.join(scenario_rows)}</tbody></table></div></section>
<section><h2>Titan engine audit</h2><div class="table-wrap"><table><thead><tr><th>Capability</th><th>Current state</th><th>Evidence</th></tr></thead><tbody>{audit_rows}</tbody></table></div></section>
<section><h2>Existing Titan agent patterns</h2><div class="table-wrap"><table><thead><tr><th>Example</th><th>Where</th><th>Runtime shape</th><th>Scheduling boundary</th><th>Reference scenario</th></tr></thead><tbody>{agent_rows}</tbody></table></div></section>
<section><h2>External agent benchmark fit</h2><div class="table-wrap"><table><thead><tr><th>Benchmark</th><th>Fit</th><th>What Titan can demonstrate</th><th>Limit</th><th>Decision</th></tr></thead><tbody>{external_rows}</tbody></table></div></section>
<section><details><summary>Interpretation and limits</summary><p>The existing scheduler supports late child admission, concurrent fan-in completion, indexed dependency release, and exactly-once removal from its waiting room. It does not yet expose a run-scoped atomic mutation contract with a mutation ID and graph version. The reference model defines and stress-tests that missing contract before production integration.</p><p>ScienceAgentBench is useful later as an application-level scientific agent workload: it evaluates generated scientific Python programs, execution results, and cost across 102 tasks. It does not directly test scheduler mutation races, so it complements this harness rather than replacing it.</p></details></section>
<section><h2>Artifacts</h2><p><code>summary.json</code> contains every case and timing. <code>events.jsonl</code> is the append-only replay journal. <code>verification.json</code> is produced by an independent state replay.</p></section>
</main></body></html>"""


def titan_audit() -> list[dict]:
    return [
        {"capability": "Late child after parent completion", "current": "Implemented and unit tested",
         "evidence": "Scheduler.resolveCompletedParents and DagAdmissionConcurrencyTest"},
        {"capability": "Concurrent fan-in release", "current": "Implemented and unit tested",
         "evidence": "synchronized admission/unlock, concurrent satisfied dependencies, waiting-room conditional remove"},
        {"capability": "Cancellation cascade", "current": "Implemented and indexed",
         "evidence": "Scheduler.cancelJob traverses childrenByParent"},
        {"capability": "Runtime agent feedback loops", "current": "Implemented at orchestrator level",
         "evidence": "Planner/healer on this branch; research DEEPEN and code REQUEST_CHANGES loops on master"},
        {"capability": "Runtime-sized agent fan-out", "current": "Implemented at orchestrator level",
         "evidence": "Document scanner, research planner, research pipeline, and competitive-intelligence examples"},
        {"capability": "Cross-stage Agent Run grouping", "current": "Implemented on master; passive UI metadata",
         "evidence": "agent_run_id groups DAG submissions and explicitly has no scheduling/execution effect"},
        {"capability": "In-run atomic graph mutation", "current": "Not exposed",
         "evidence": "No run ID, mutation ID, expected graph version, or mutation response in SDK/server"},
        {"capability": "Dynamic branch example", "current": "Pre-submit selection only",
         "evidence": "dynamic_switch.py chooses a static DAG before submit"},
    ]


def agent_examples() -> list[dict]:
    return [
        {"example": "LLM planner", "branch": "current + master",
         "shape": "Observe results, then submit a newly generated DAG on each iteration",
         "boundary": "Multiple independent DAG submissions", "scenario": "iterative-agent"},
        {"example": "Code healer", "branch": "current + master",
         "shape": "Diagnose, read, fix, verify; the next cycle depends on prior logs",
         "boundary": "Multiple independent DAG submissions", "scenario": "recursive-agent"},
        {"example": "Document generator", "branch": "current + master",
         "shape": "Repository scan decides the number of parallel file-documentation jobs",
         "boundary": "Runtime-sized single DAG", "scenario": "document-agent-fanout"},
        {"example": "Research agent", "branch": "master",
         "shape": "Plan, parallel research, evaluate, DEEPEN on gaps, then synthesize",
         "boundary": "Multiple DAGs grouped by agent_run_id", "scenario": "research-agent-loop"},
        {"example": "Code generation agent", "branch": "master",
         "shape": "Generate, review, spawn targeted fixers, re-review, then integrate",
         "boundary": "Multiple DAGs grouped by agent_run_id", "scenario": "code-review-repair"},
        {"example": "Research + HITL pipeline", "branch": "master",
         "shape": "Planner chooses width; parallel research joins at approval and synthesis",
         "boundary": "Two DAG stages grouped by agent_run_id", "scenario": "document-agent-fanout"},
        {"example": "Competitive intelligence", "branch": "master",
         "shape": "Input frameworks determine parallel analysts and a synthesis fan-in",
         "boundary": "Runtime-sized single DAG", "scenario": "fan-out-in"},
    ]


def external_benchmarks() -> list[dict]:
    return [
        {"name": "ScienceAgentBench", "fit": 9,
         "use": "Scientific code generation, execute/inspect/repair loops, heterogeneous data tasks, and end-to-end success beside Titan runtime traces.",
         "limit": "Measures agent output quality; its upstream task is a standalone program and does not create a distributed DAG by itself.",
         "decision": "First external pilot: pinned verified subset, upstream grader unchanged."},
        {"name": "MLE-bench", "fit": 7,
         "use": "Long-running ML experimentation, resource placement, parallel trials, failure recovery, and cost-aware scheduling.",
         "limit": "Full evaluation is expensive: 75 competitions; even the 22-case low-complexity split is about 158 GB.",
         "decision": "VM/GPU phase after multi-machine Titan validation; begin with one competition."},
        {"name": "AgentBench", "fit": 5,
         "use": "Generic multi-turn environment and tool interaction checks across eight environments.",
         "limit": "Broad LLM-agent reasoning score with weak alignment to scientific DAG scheduling and runtime mutation.",
         "decision": "Optional breadth result, not a core Titan systems benchmark."},
        {"name": "MLAgentBench", "fit": 6,
         "use": "A candidate agent implementation or scaffold for iterative ML experimentation.",
         "limit": "It is better treated as a workload producer; MLE-bench provides the stronger external evaluation protocol.",
         "decision": "Adapter candidate, not the primary benchmark claim."},
    ]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenarios", default="all",
                        help="Comma-separated names or 'all'")
    parser.add_argument("--passes", type=int, default=3)
    parser.add_argument("--scale", type=int, default=64,
                        help="Scenario width/iteration control")
    parser.add_argument("--seed", type=int, default=20261009)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.passes < 1 or args.scale < 1:
        parser.error("--passes and --scale must be positive")
    selected = list(SCENARIOS) if args.scenarios == "all" else [
        item.strip() for item in args.scenarios.split(",") if item.strip()]
    unknown = sorted(set(selected) - set(SCENARIOS))
    if unknown:
        parser.error("unknown scenarios: " + ", ".join(unknown))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    cases = []
    expected = {"cases": {}}
    journal_path = args.output_dir / "events.jsonl"
    with journal_path.open("w", encoding="utf-8", newline="\n") as journal:
        for pass_index in range(1, args.passes + 1):
            for scenario_index, scenario in enumerate(selected):
                seed = args.seed + pass_index * 10_000 + scenario_index
                case_id = f"{scenario}-pass-{pass_index}"
                started = time.perf_counter_ns()
                run = run_scenario(scenario, args.scale, seed, case_id)
                wall_ms = (time.perf_counter_ns() - started) / 1_000_000
                snapshot = run.snapshot()
                verification = verify_events(run.events, snapshot)
                seconds = max(wall_ms / 1000, 1e-9)
                case = {
                    "case_id": case_id, "scenario": scenario, "pass": pass_index,
                    "seed": seed, "scale": args.scale, "wall_ms": wall_ms,
                    "nodes_per_second": snapshot["nodes"] / seconds,
                    "mutations_per_second": snapshot["version"] / seconds,
                    "snapshot": snapshot, "verification": verification,
                }
                cases.append(case)
                expected["cases"][case_id] = {"snapshot": snapshot}
                for event in run.events:
                    journal.write(json.dumps({"case_id": case_id, **event}, separators=(",", ":")) + "\n")

    probes = contract_probes()
    report = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "command": {"scenarios": selected, "passes": args.passes,
                    "scale": args.scale, "seed": args.seed},
        "passed": all(case["verification"]["passed"] for case in cases) and all(probes.values()),
        "events": sum(case["snapshot"]["event_count"] for case in cases),
        "contract_probes": probes,
        "aggregate": aggregate(cases),
        "titan_audit": titan_audit(),
        "agent_examples": agent_examples(),
        "external_benchmarks": external_benchmarks(),
        "cases": cases,
    }
    (args.output_dir / "expected.json").write_text(json.dumps(expected, indent=2), encoding="utf-8")
    (args.output_dir / "summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    verification = {"passed": report["passed"], "cases": {
        case["case_id"]: case["verification"] for case in cases}}
    (args.output_dir / "verification.json").write_text(json.dumps(verification, indent=2), encoding="utf-8")
    (args.output_dir / "report.html").write_text(render_html(report), encoding="utf-8")
    print(json.dumps({"passed": report["passed"], "cases": len(cases),
                      "events": report["events"], "output": str(args.output_dir)}, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
