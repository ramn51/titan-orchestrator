"""Run agent-shaped benchmarks through a live Titan master and worker fleet.

The runner uses deterministic, short worker tasks for scheduler measurements.
With --gemini it also asks Gemini to make constrained decisions during the
first pass. The API key is read with getpass and is never persisted.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
import getpass
import html
import json
import os
from pathlib import Path
import statistics
import time
import urllib.request

from titan_sdk import TitanClient, TitanJob


TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}


@dataclass(frozen=True)
class Spec:
    node_id: str
    parents: tuple[str, ...] = ()
    stage: str = "task"
    priority: int = 5


@dataclass
class Case:
    name: str
    pass_index: int
    expected: dict[str, Spec] = field(default_factory=dict)
    submissions: list[dict] = field(default_factory=list)
    decisions: list[dict] = field(default_factory=list)
    started_ms: int = 0
    ended_ms: int = 0
    statuses: dict[str, str] = field(default_factory=dict)
    error: str = ""
    agent_run_id: str = ""


def configure_dag_persistence(output_dir: Path, dashboard_visible: bool,
                              dashboard_manifest: Path) -> dict[str, str]:
    """Select where SDK DAG metadata is written for this benchmark process.

    Isolated storage remains the default for reproducible benchmark artifacts.
    Dashboard-visible runs use the dashboard's normal journal while keeping the
    benchmark summary and verification files in ``output_dir``.
    """
    if dashboard_visible:
        manifest = dashboard_manifest.resolve()
        journal = manifest.with_name(".titan_dag_journal")
        history = manifest.with_name(".titan_dag_run_history.jsonl")
    else:
        manifest = (output_dir / "manifest.json").resolve()
        journal = (output_dir / "dag-journal").resolve()
        history = (output_dir / "run-history.jsonl").resolve()
    configured = {
        "TITAN_DAG_MANIFEST": str(manifest),
        "TITAN_DAG_JOURNAL_DIR": str(journal),
        "TITAN_DAG_RUN_HISTORY": str(history),
        "TITAN_DAG_WORKSPACE_DIR": str(Path("titan_workspace").resolve()),
    }
    os.environ.update(configured)
    return configured


class GeminiJudge:
    def __init__(self, key: str, model: str):
        self._key = key
        self.model = model if model.startswith("models/") else "models/" + model

    def choose(self, rule: str, observation: str, allowed: tuple[str, ...]) -> str:
        prompt = (
            "You are a deterministic workflow router. " + rule + "\n"
            "Observation: " + observation + "\n"
            "Return exactly one token from: " + ", ".join(allowed)
        )
        body = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0, "maxOutputTokens": 256,
                                  "thinkingConfig": {"thinkingBudget": 0}},
        }).encode("utf-8")
        request = urllib.request.Request(
            "https://generativelanguage.googleapis.com/v1beta/"
            + self.model + ":generateContent",
            data=body,
            headers={"x-goog-api-key": self._key, "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=45) as response:
            result = json.load(response)
        candidate = (result.get("candidates") or [{}])[0]
        parts = (candidate.get("content") or {}).get("parts") or []
        if not parts or not isinstance(parts[0].get("text"), str):
            raise RuntimeError(
                "Gemini returned no decision text; finishReason="
                + str(candidate.get("finishReason", "unknown")))
        text = parts[0]["text"].strip().upper()
        matches = [choice for choice in allowed if choice in text]
        if len(matches) != 1:
            raise RuntimeError(f"Gemini returned an invalid routing decision: {text!r}")
        return matches[0]


class LiveSuite:
    def __init__(self, output: Path, seconds: float, timeout: float,
                 gemini: GeminiJudge | None = None):
        self.output = output
        self.seconds = seconds
        self.timeout = timeout
        self.gemini = gemini
        self.client = TitanClient()
        self.script = str(Path(__file__).with_name("live_stage.py"))

    @staticmethod
    def _full(node_id: str) -> str:
        return "DAG-" + node_id

    def submit(self, case: Case, label: str, specs: list[Spec]) -> None:
        for spec in specs:
            if spec.node_id in case.expected:
                raise RuntimeError(f"duplicate benchmark node: {spec.node_id}")
            case.expected[spec.node_id] = spec
        jobs = [TitanJob(
            spec.node_id,
            self.script,
            args=f"{spec.node_id} {case.name} {spec.stage} {self.seconds}",
            parents=list(spec.parents),
            priority=spec.priority,
        ) for spec in specs]
        dag_name = f"AGENTBENCH_{case.name.upper()}_{label}_{case.pass_index}_{case.agent_run_id}"
        started = time.time_ns()
        response = self.client.submit_dag(
            dag_name, jobs, agent_run_id=case.agent_run_id,
            agent_run_name=case.name.replace("-", " ").title(), agent_stage=label,
            agent_stage_order=len(case.submissions))
        ended = time.time_ns()
        case.submissions.append({
            "dag": dag_name,
            "label": label,
            "jobs": [self._full(spec.node_id) for spec in specs],
            "submit_ms": round((ended - started) / 1_000_000, 3),
            "response": response,
        })
        if response != "DAG_ACCEPTED":
            raise RuntimeError(f"{dag_name} rejected: {response}")

    def wait(self, case: Case, node_ids: list[str]) -> None:
        pending = set(node_ids)
        deadline = time.monotonic() + self.timeout
        while pending and time.monotonic() < deadline:
            for node_id in list(pending):
                status = self.client.get_job_status(self._full(node_id)).strip()
                case.statuses[self._full(node_id)] = status
                if status in TERMINAL:
                    pending.remove(node_id)
                    if status != "COMPLETED":
                        raise RuntimeError(f"{self._full(node_id)} ended as {status}")
            if pending:
                time.sleep(0.15)
        if pending:
            raise TimeoutError("timed out waiting for " + ", ".join(sorted(pending)))

    def phase(self, case: Case, label: str, specs: list[Spec]) -> None:
        self.submit(case, label, specs)
        self.wait(case, [spec.node_id for spec in specs])

    def research(self, token: str, pass_index: int) -> Case:
        case = Case("research", pass_index, started_ms=int(time.time() * 1000), agent_run_id=token)
        plan = f"{token}_rp"
        self.phase(case, "PLAN", [Spec(plan, stage="planner")])
        prior = plan
        widths = [6, 3]
        for iteration, width in enumerate(widths, 1):
            workers = [Spec(f"{token}_rr{iteration}_{i}", (prior,), "researcher")
                       for i in range(width)]
            self.phase(case, f"RESEARCH{iteration}", workers)
            evaluator = Spec(f"{token}_re{iteration}",
                             tuple(spec.node_id for spec in workers), "evaluator", 6)
            self.phase(case, f"EVAL{iteration}", [evaluator])
            if self.gemini and pass_index == 1:
                observation = ("MISSING independent replication" if iteration == 1
                               else "COMPLETE with independent replication")
                decision = self.gemini.choose(
                    "If the observation contains MISSING choose DEEPEN; otherwise choose SYNTHESIZE.",
                    observation, ("DEEPEN", "SYNTHESIZE"))
            else:
                decision = "DEEPEN" if iteration == 1 else "SYNTHESIZE"
            case.decisions.append({"stage": evaluator.node_id, "decision": decision,
                                   "source": "gemini" if self.gemini and pass_index == 1 else "rule"})
            prior = evaluator.node_id
            if decision == "SYNTHESIZE":
                break
        self.phase(case, "SYNTH", [Spec(f"{token}_rs", (prior,), "synthesizer", 7)])
        case.ended_ms = int(time.time() * 1000)
        return case

    def code_review(self, token: str, pass_index: int) -> Case:
        case = Case("code-review", pass_index, started_ms=int(time.time() * 1000), agent_run_id=token)
        plan = f"{token}_cp"
        self.phase(case, "PLAN", [Spec(plan, stage="planner")])
        generators = [Spec(f"{token}_cg{i}", (plan,), "generator") for i in range(8)]
        self.phase(case, "GENERATE", generators)
        review1 = f"{token}_cr1"
        self.phase(case, "REVIEW1", [Spec(
            review1, tuple(spec.node_id for spec in generators), "reviewer", 6)])
        flagged = (1, 4, 7)
        fixers = [Spec(f"{token}_cf{i}", (review1, f"{token}_cg{i}"), "fixer", 7)
                  for i in flagged]
        case.decisions.append({"stage": review1, "decision": "REQUEST_CHANGES",
                               "flagged_components": list(flagged), "source": "rule"})
        self.phase(case, "FIX", fixers)
        review2 = f"{token}_cr2"
        self.phase(case, "REVIEW2", [Spec(
            review2, tuple(spec.node_id for spec in fixers), "reviewer", 6)])
        case.decisions.append({"stage": review2, "decision": "APPROVE", "source": "rule"})
        self.phase(case, "INTEGRATE", [Spec(f"{token}_ci", (review2,), "integrator", 7)])
        case.ended_ms = int(time.time() * 1000)
        return case

    def document(self, token: str, pass_index: int) -> Case:
        case = Case("document", pass_index, started_ms=int(time.time() * 1000), agent_run_id=token)
        scan = f"{token}_ds"
        self.phase(case, "SCAN", [Spec(scan, stage="scanner")])
        files = [Spec(f"{token}_df{i:02d}", (scan,), "document-file") for i in range(24)]
        self.phase(case, "FILES", files)
        self.phase(case, "INDEX", [Spec(
            f"{token}_di", tuple(spec.node_id for spec in files), "indexer")])
        case.ended_ms = int(time.time() * 1000)
        return case

    def conditional(self, token: str, pass_index: int) -> Case:
        case = Case("conditional", pass_index, started_ms=int(time.time() * 1000), agent_run_id=token)
        classifier = f"{token}_bc"
        self.phase(case, "CLASSIFY", [Spec(classifier, stage="classifier")])
        if self.gemini and pass_index == 1:
            branch = self.gemini.choose(
                "If correctness is required choose QUALITY; otherwise choose SPEED.",
                "Correctness is required for this scientific result.", ("QUALITY", "SPEED"))
        else:
            branch = "QUALITY"
        case.decisions.append({"stage": classifier, "decision": branch,
                               "source": "gemini" if self.gemini and pass_index == 1 else "rule"})
        jobs = [Spec(f"{token}_{branch.lower()}{i}", (classifier,), branch.lower())
                for i in range(12)]
        self.phase(case, branch, jobs)
        self.phase(case, "REPORT", [Spec(
            f"{token}_br", tuple(spec.node_id for spec in jobs), "report")])
        case.ended_ms = int(time.time() * 1000)
        return case

    def recursive(self, token: str, pass_index: int) -> Case:
        case = Case("recursive", pass_index, started_ms=int(time.time() * 1000), agent_run_id=token)
        prior = ""
        for attempt in range(6):
            current = f"{token}_ra{attempt}"
            parents = (prior,) if prior else ()
            self.phase(case, f"ATTEMPT{attempt}", [Spec(current, parents, "agent-attempt")])
            prior = current
        self.phase(case, "TERMINAL", [Spec(f"{token}_rt", (prior,), "finish")])
        case.ended_ms = int(time.time() * 1000)
        return case

    def concurrent_clients(self, token: str, pass_index: int) -> Case:
        case = Case("concurrent-clients", pass_index, started_ms=int(time.time() * 1000), agent_run_id=token)
        groups = []
        for client_index in range(4):
            root = Spec(f"{token}_mc{client_index}r", stage="client-root")
            leaves = [Spec(f"{token}_mc{client_index}w{i}", (root.node_id,), "client-work")
                      for i in range(7)]
            groups.append((f"CLIENT{client_index}", [root, *leaves]))
        for _, specs in groups:
            for spec in specs:
                case.expected[spec.node_id] = spec

        def submit_group(group):
            label, specs = group
            jobs = [TitanJob(spec.node_id, self.script,
                             args=f"{spec.node_id} {case.name} {spec.stage} {self.seconds}",
                             parents=list(spec.parents)) for spec in specs]
            started = time.time_ns()
            dag_name = f"AGENTBENCH_{case.name.upper()}_{label}_{pass_index}_{case.agent_run_id}"
            response = TitanClient().submit_dag(
                dag_name, jobs,
                agent_run_id=case.agent_run_id, agent_run_name="Concurrent Clients",
                agent_stage=label, agent_stage_order=0)
            return label, dag_name, specs, response, round((time.time_ns() - started) / 1_000_000, 3)

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(submit_group, groups))
        for label, dag_name, specs, response, submit_ms in results:
            case.submissions.append({"dag": dag_name,
                                     "label": label,
                                     "jobs": [self._full(spec.node_id) for spec in specs],
                                     "submit_ms": submit_ms, "response": response})
            if response != "DAG_ACCEPTED":
                raise RuntimeError(f"concurrent client {label} rejected: {response}")
        self.wait(case, list(case.expected))
        case.ended_ms = int(time.time() * 1000)
        return case


def read_spans(telemetry: Path, expected: set[str]) -> dict[str, list[dict]]:
    result = {job_id: [] for job_id in expected}
    for path in telemetry.glob("spans-*.jsonl"):
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line in stream:
                try:
                    span = json.loads(line)
                except ValueError:
                    continue
                job_id = span.get("job_id")
                if job_id in result:
                    result[job_id].append(span)
    return result


def verify_cases(cases: list[Case], telemetry: Path) -> dict:
    expected = {"DAG-" + node_id for case in cases for node_id in case.expected}
    grouped = read_spans(telemetry, expected)
    results = []
    for case in cases:
        errors = []
        completed = {}
        queue_ms, dispatch_ms, execution_ms = [], [], []
        workers = set()
        attempts = 0
        for node_id, spec in case.expected.items():
            full_id = "DAG-" + node_id
            spans = grouped.get(full_id, [])
            attempts += len(spans)
            good = [span for span in spans if span.get("status") == "COMPLETED"]
            if len(good) != 1:
                errors.append(f"{full_id}: expected one COMPLETED span, found {len(good)}")
                continue
            span = good[0]
            completed[full_id] = span
            actual_parents = tuple(sorted(span.get("parents") or []))
            wanted_parents = tuple(sorted("DAG-" + parent for parent in spec.parents))
            if actual_parents != wanted_parents:
                errors.append(f"{full_id}: parent mismatch")
            workers.add(span.get("worker") or "unknown")
            queued = int(span.get("queued_at") or 0)
            eligible = int(span.get("eligible_at") or queued)
            started = int(span.get("started_at") or 0)
            ended = int(span.get("ended_at") or 0)
            queue_ms.append(max(0, eligible - queued))
            dispatch_ms.append(max(0, started - eligible))
            execution_ms.append(max(0, ended - started))
            if case.statuses.get(full_id) != "COMPLETED":
                errors.append(f"{full_id}: final RPC status {case.statuses.get(full_id)!r}")
        for node_id, spec in case.expected.items():
            child = completed.get("DAG-" + node_id)
            if not child:
                continue
            for parent in spec.parents:
                parent_span = completed.get("DAG-" + parent)
                if parent_span and int(parent_span.get("ended_at") or 0) > int(child.get("started_at") or 0):
                    errors.append(f"DAG-{node_id}: started before DAG-{parent} completed")
        def median(values):
            return round(statistics.median(values), 3) if values else None
        results.append({
            "name": case.name,
            "pass": case.pass_index,
            "passed": not errors,
            "errors": errors,
            "jobs": len(case.expected),
            "edges": sum(len(spec.parents) for spec in case.expected.values()),
            "submissions": len(case.submissions),
            "wall_ms": case.ended_ms - case.started_ms,
            "attempts": attempts,
            "workers": sorted(workers),
            "median_queue_ms": median(queue_ms),
            "median_dispatch_ms": median(dispatch_ms),
            "median_execution_ms": median(execution_ms),
            "decisions": case.decisions,
        })
    return {"passed": all(result["passed"] for result in results), "cases": results}


def render(report: dict) -> str:
    rows = []
    for item in report["verification"]["cases"]:
        rows.append(
            f"<tr><td><b>{html.escape(item['name'])}</b></td><td>{item['pass']}</td>"
            f"<td>{item['jobs']}</td><td>{item['edges']}</td><td>{item['submissions']}</td>"
            f"<td>{item['wall_ms'] / 1000:.2f}s</td><td>{item['median_dispatch_ms']} ms</td>"
            f"<td>{len(item['workers'])}</td><td class={'ok' if item['passed'] else 'bad'}>"
            f"{'PASS' if item['passed'] else 'FAIL'}</td></tr>")
    decisions = []
    for item in report["verification"]["cases"]:
        for decision in item["decisions"]:
            decisions.append(f"<li><b>{html.escape(item['name'])} pass {item['pass']}</b> · "
                             f"{html.escape(str(decision))}</li>")
    passed = report["verification"]["passed"]
    return f"""<!doctype html><html><head><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>
<title>Titan live agent benchmark</title><style>
:root{{--bg:#07100e;--panel:#101c19;--line:#29453b;--text:#ecf8f2;--muted:#9fb8ae;--green:#45e6a8;--red:#ff7188}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 Segoe UI,sans-serif}}main{{max-width:1180px;margin:auto;padding:28px}}h1{{font-size:28px;margin:0}}.muted{{color:var(--muted)}}section,.card{{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px;margin:14px 0}}.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}}.card b{{font-size:24px;display:block}}table{{border-collapse:collapse;width:100%}}th,td{{text-align:left;padding:9px;border-bottom:1px solid var(--line)}}th{{font-size:11px;color:var(--muted);text-transform:uppercase}}.ok{{color:var(--green)}}.bad{{color:var(--red)}}code{{color:#b8f5dc}}@media(max-width:800px){{main{{padding:14px}}.cards{{grid-template-columns:repeat(2,1fr)}}.scroll{{overflow:auto}}}}
</style></head><body><main><p class=muted>TITAN · LIVE ENGINE BENCHMARK</p><h1>Agent-shaped workflow execution</h1>
<p class=muted>Generated {html.escape(report['generated_at'])}. These results came from Titan's master, worker RPC path, scheduler, callbacks, and persisted spans.</p>
<div class=cards><div class=card><span>Verdict</span><b class={'ok' if passed else 'bad'}>{'PASS' if passed else 'FAIL'}</b></div><div class=card><span>Cases</span><b>{len(report['verification']['cases'])}</b></div><div class=card><span>Jobs</span><b>{report['jobs']}</b></div><div class=card><span>Gemini</span><b>{'Used' if report['gemini']['used'] else 'Off'}</b></div></div>
<section><h2>Execution results</h2><div class=scroll><table><thead><tr><th>Pattern</th><th>Pass</th><th>Jobs</th><th>Edges</th><th>DAG submissions</th><th>Wall time</th><th>Median dispatch</th><th>Workers</th><th>Result</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div></section>
<section><h2>Runtime decisions</h2><ul>{''.join(decisions) or '<li>No decision records</li>'}</ul></section>
<section><h2>Interpretation</h2><p>Each stage was submitted through the Python SDK and executed by Titan workers. Verification requires exactly one successful span per expected job, exact parent sets, parent completion before child start, a COMPLETED status from the master, and no missing work.</p><p>The workflows evolve across multiple DAG submissions. This validates Titan's current orchestrator-driven agent model and late-child dependency handling. It does not claim atomic mutation of one admitted DAG.</p></section>
<section><h2>Artifacts</h2><p><code>summary.json</code>, <code>verification.json</code>, the isolated DAG journal, and benchmark logs are stored beside this report. The Gemini key is never stored.</p></section></main></body></html>"""


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--passes", type=int, default=3)
    parser.add_argument("--scenarios", default="all",
                        help="Comma-separated scenario names or 'all'")
    parser.add_argument("--stage-seconds", type=float, default=0.08)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--telemetry-dir", type=Path, default=Path("telemetry"))
    parser.add_argument(
        "--dashboard-visible", action="store_true",
        help="Register accepted DAGs in the dashboard's normal journal")
    parser.add_argument(
        "--dashboard-manifest", type=Path, default=Path(".titan_dag_manifest.json"),
        help="Dashboard manifest used to locate its journal and run-history index")
    parser.add_argument("--gemini", action="store_true")
    parser.add_argument("--gemini-model", default="gemini-2.5-flash")
    args = parser.parse_args(argv)
    if args.passes < 1 or not 0 <= args.stage_seconds <= 5:
        parser.error("passes must be positive and stage seconds must be 0-5")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    persistence = configure_dag_persistence(
        args.output_dir, args.dashboard_visible, args.dashboard_manifest)
    gemini = None
    if args.gemini:
        key = getpass.getpass("Gemini API key: ")
        gemini = GeminiJudge(key, args.gemini_model)
    suite = LiveSuite(args.output_dir, args.stage_seconds, args.timeout, gemini)
    cases = []
    stamp = str(int(time.time() * 1000))[-9:]
    available = {
        "research": suite.research,
        "code-review": suite.code_review,
        "document": suite.document,
        "conditional": suite.conditional,
        "recursive": suite.recursive,
        "concurrent-clients": suite.concurrent_clients,
    }
    selected = list(available) if args.scenarios == "all" else [
        item.strip() for item in args.scenarios.split(",") if item.strip()]
    unknown = sorted(set(selected) - set(available))
    if unknown:
        parser.error("unknown scenarios: " + ", ".join(unknown))
    methods = [available[name] for name in selected]
    for pass_index in range(1, args.passes + 1):
        for scenario_index, method in enumerate(methods):
            token = f"ab{stamp}{pass_index}{scenario_index}"
            try:
                cases.append(method(token, pass_index))
            except Exception as exc:
                failed = Case(method.__name__.replace("_", "-"), pass_index,
                              started_ms=int(time.time() * 1000), ended_ms=int(time.time() * 1000),
                              error=f"{type(exc).__name__}: {exc}")
                cases.append(failed)
                print(f"[FAIL] {method.__name__} pass {pass_index}: {exc}", flush=True)
    verification = verify_cases(cases, args.telemetry_dir)
    for case, result in zip(cases, verification["cases"]):
        if case.error:
            result["errors"].append(case.error)
            result["passed"] = False
    verification["passed"] = all(item["passed"] for item in verification["cases"])
    report = {
        "schema": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "configuration": {"passes": args.passes, "stage_seconds": args.stage_seconds,
                          "timeout": args.timeout, "scenarios": selected,
                          "dashboard_visible": args.dashboard_visible,
                          "dag_manifest": persistence["TITAN_DAG_MANIFEST"],
                          "dag_journal": persistence["TITAN_DAG_JOURNAL_DIR"]},
        "gemini": {"used": bool(gemini), "model": gemini.model if gemini else None},
        "jobs": sum(len(case.expected) for case in cases),
        "verification": verification,
        "submissions": [submission for case in cases for submission in case.submissions],
    }
    (args.output_dir / "summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (args.output_dir / "verification.json").write_text(
        json.dumps(verification, indent=2), encoding="utf-8")
    (args.output_dir / "report.html").write_text(render(report), encoding="utf-8")
    print(json.dumps({"passed": verification["passed"], "cases": len(cases),
                      "jobs": report["jobs"], "report": str(args.output_dir / 'report.html')},
                     indent=2))
    return 0 if verification["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
