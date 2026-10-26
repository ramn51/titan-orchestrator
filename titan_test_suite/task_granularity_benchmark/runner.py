"""Measure Titan's minimum effective task granularity and DAG behavior.

The granularity sweep follows the Task Bench convention of reporting METG(50):
the smallest tested task duration where useful-work efficiency reaches 50%.
The runner also supports graph-pattern, scale, local overlap-proxy, and load-
imbalance suites. It uses Titan's completed span journal as evidence and writes
both machine-readable JSON and a self-contained HTML report.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import html
import json
import math
import os
from pathlib import Path
import random
import socket
import statistics
import struct
import sys
import time
from typing import Iterable

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from titan_sdk.titan_sdk import TitanClient, TitanJob  # noqa: E402

TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}
OP_STATS_JSON = 0x09


def percentile(values: Iterable[float], quantile: float) -> float | None:
    """Return an R-7/NumPy-style linearly interpolated percentile."""
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    if not 0 <= quantile <= 1:
        raise ValueError("quantile must be between 0 and 1")
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def titan_stats_query(payload: str, host: str = "127.0.0.1", port: int = 9090) -> dict:
    """Read one bounded projection through Titan's existing stats opcode."""
    body = payload.encode("utf-8")
    with socket.create_connection((host, port), timeout=5) as connection:
        connection.sendall(struct.pack(">BBBBI", 1, OP_STATS_JSON, 0, 0, len(body)) + body)
        header = b""
        while len(header) < 8:
            chunk = connection.recv(8 - len(header))
            if not chunk: raise OSError("incomplete Titan response header")
            header += chunk
        _, _, _, _, length = struct.unpack(">BBBBI", header)
        response = b""
        while len(response) < length:
            chunk = connection.recv(length - len(response))
            if not chunk: raise OSError("incomplete Titan response payload")
            response += chunk
    return json.loads(response.decode("utf-8"))


def phase_samples(metrics: dict, expected: set[str], started_at: int, ended_at: int) -> dict:
    dispatch = [item for item in metrics.get("dispatch_phases") or []
                if item.get("job_id") in expected and int(item.get("at") or 0) <= ended_at]
    worker = [item for item in metrics.get("worker_phases") or []
              if item.get("job_id") in expected and int(item.get("at") or 0) <= ended_at]
    store = [item for item in metrics.get("store_operations") or []
             if started_at <= int(item.get("at") or 0) <= ended_at]
    result = {name + "_ms_samples": [float(item.get(field) or 0) for item in source]
              for name, field, source in (
                  ("dispatch_route", "route", dispatch), ("worker_selection", "select", dispatch),
                  ("dispatch_record", "record", dispatch), ("dispatch_store", "store", dispatch),
                  ("dispatch_rpc", "send", dispatch), ("worker_queue", "worker_queue", worker),
                  ("preprocess", "preprocess", worker), ("process_startup", "process_startup", worker),
                  ("script_execution", "script", worker), ("callback_transit", "callback_transit", worker),
                  ("callback_handle", "callback_handle", worker),
                  ("terminal_persistence", "terminal_persistence", worker))}
    result["store_read_ms_samples"] = [float(item.get("ms") or 0) for item in store
                                       if item.get("kind") == "read"]
    result["store_write_ms_samples"] = [float(item.get("ms") or 0) for item in store
                                        if item.get("kind") == "write"]
    return result


def bootstrap_median_ci(values: Iterable[float], confidence: float = .95,
                        samples: int = 10_000, seed: int = 20261026) -> list[float] | None:
    """Deterministic percentile-bootstrap confidence interval for a median."""
    observed = [float(value) for value in values]
    if not observed:
        return None
    rng = random.Random(seed)
    estimates = [statistics.median(rng.choices(observed, k=len(observed)))
                 for _ in range(samples)]
    tail = (1 - confidence) / 2
    return [round(percentile(estimates, tail), 6),
            round(percentile(estimates, 1 - tail), 6)]


def variability(values: Iterable[float]) -> dict:
    observed = [float(value) for value in values]
    median = statistics.median(observed)
    mean = statistics.mean(observed)
    standard_deviation = statistics.stdev(observed) if len(observed) > 1 else 0.0
    return {
        "median": round(median, 6),
        "min": round(min(observed), 6),
        "max": round(max(observed), 6),
        "standard_deviation": round(standard_deviation, 6),
        "coefficient_of_variation": round(standard_deviation / mean, 6) if mean else 0.0,
        "median_bootstrap_95_ci": bootstrap_median_ci(observed),
    }


@dataclass(frozen=True)
class TaskSpec:
    node_id: str
    parents: tuple[str, ...]
    duration_ms: float
    mode: str = "sleep"


def parse_csv_numbers(value: str, cast=float) -> list:
    values = [cast(item.strip()) for item in value.split(",") if item.strip()]
    if not values:
        raise argparse.ArgumentTypeError("provide at least one comma-separated value")
    return values


def leaves(specs: list[TaskSpec]) -> list[str]:
    parents = {parent for spec in specs for parent in spec.parents}
    return [spec.node_id for spec in specs if spec.node_id not in parents]


def make_graph(pattern: str, count: int, duration_ms: float, prefix: str,
               durations: list[float] | None = None) -> list[TaskSpec]:
    if count < 2:
        raise ValueError("a graph requires at least two measured tasks")
    ds = durations or [duration_ms] * count
    if len(ds) != count:
        raise ValueError("duration vector length must match task count")
    ids = [f"{prefix}_n{i:04d}" for i in range(count)]
    specs: list[TaskSpec] = []
    if pattern == "bag":
        specs = [TaskSpec(ids[i], (), ds[i]) for i in range(count)]
    elif pattern == "chain":
        specs = [TaskSpec(ids[i], (ids[i - 1],) if i else (), ds[i])
                 for i in range(count)]
    elif pattern == "fanout":
        specs.append(TaskSpec(ids[0], (), ds[0]))
        specs.extend(TaskSpec(ids[i], (ids[0],), ds[i]) for i in range(1, count))
    elif pattern == "fanin":
        specs.extend(TaskSpec(ids[i], (), ds[i]) for i in range(count - 1))
        specs.append(TaskSpec(ids[-1], tuple(ids[:-1]), ds[-1]))
    elif pattern in {"diamond", "layered", "stencil"}:
        width = max(2, int(round(math.sqrt(count))))
        levels = max(2, math.ceil(count / width))
        for i in range(count):
            level, offset = divmod(i, width)
            if level == 0:
                parents = ()
            else:
                previous_start = (level - 1) * width
                previous_end = min(previous_start + width, count)
                if pattern == "layered":
                    parents = tuple(ids[previous_start:previous_end])
                else:
                    candidates = {previous_start + min(offset, previous_end - previous_start - 1)}
                    if offset:
                        candidates.add(previous_start + offset - 1)
                    parents = tuple(ids[p] for p in sorted(candidates) if p < previous_end)
            specs.append(TaskSpec(ids[i], parents, ds[i]))
    else:
        raise ValueError(f"unsupported graph pattern: {pattern}")
    return specs


def add_sink(specs: list[TaskSpec], prefix: str) -> tuple[list[TaskSpec], str]:
    sink = f"{prefix}_sink"
    return [*specs, TaskSpec(sink, tuple(leaves(specs)), 0)], sink


def critical_path_ms(specs: list[TaskSpec]) -> float:
    by_id = {spec.node_id: spec for spec in specs}
    memo: dict[str, float] = {}

    def visit(node_id: str) -> float:
        if node_id not in memo:
            node = by_id[node_id]
            memo[node_id] = node.duration_ms + max((visit(p) for p in node.parents), default=0)
        return memo[node_id]

    return max((visit(spec.node_id) for spec in specs), default=0)


def telemetry_offsets(directory: Path) -> dict[Path, int]:
    return {path: path.stat().st_size for path in directory.glob("spans-*.jsonl")}


def read_new_spans(directory: Path, offsets: dict[Path, int], expected: set[str]) -> dict[str, list[dict]]:
    found: dict[str, list[dict]] = {job_id: [] for job_id in expected}
    for path in directory.glob("spans-*.jsonl"):
        with path.open("rb") as stream:
            stream.seek(offsets.get(path, 0))
            for raw in stream:
                try:
                    span = json.loads(raw)
                except (UnicodeDecodeError, ValueError):
                    continue
                job_id = span.get("job_id")
                if job_id in found:
                    found[job_id].append(span)
    return found


def max_concurrency(spans: Iterable[dict]) -> int:
    events: list[tuple[int, int]] = []
    for span in spans:
        events.append((int(span["started_at"]), 1))
        events.append((int(span["ended_at"]), -1))
    active = peak = 0
    for _, delta in sorted(events, key=lambda item: (item[0], item[1])):
        active += delta
        peak = max(peak, active)
    return peak


def summarize_case(label: str, suite: str, pattern: str, specs: list[TaskSpec], sink_id: str,
                   spans_by_job: dict[str, list[dict]], capacity: int, wall_seconds: float,
                   response: str, configured_workers: int, slots_per_worker: int) -> dict:
    measured = [spec for spec in specs if spec.node_id != sink_id]
    full = lambda node_id: "DAG-" + node_id
    all_spans = [span for spec in measured for span in spans_by_job.get(full(spec.node_id), [])]
    latest = {job_id: spans[-1] for job_id, spans in spans_by_job.items() if spans}
    errors: list[str] = []
    missing = [full(spec.node_id) for spec in measured if not spans_by_job.get(full(spec.node_id))]
    if missing:
        errors.append(f"missing {len(missing)} completed spans")
    bad = [span["job_id"] for span in all_spans if span.get("status") != "COMPLETED"]
    if bad:
        errors.append(f"{len(bad)} non-completed measured spans")
    duplicates = sum(max(0, len(spans_by_job.get(full(spec.node_id), [])) - 1) for spec in measured)
    if duplicates:
        errors.append(f"{duplicates} duplicate attempts")
    ordering_errors = []
    for spec in measured:
        child = latest.get(full(spec.node_id))
        if not child:
            continue
        for parent in spec.parents:
            parent_span = latest.get(full(parent))
            if parent_span and int(child["started_at"]) < int(parent_span["ended_at"]):
                ordering_errors.append(f"{parent}->{spec.node_id}")
    if ordering_errors:
        errors.append(f"{len(ordering_errors)} dependency-order violations")
    worker_ids = sorted({str(span.get("worker", "")) for span in all_spans if span.get("worker")})
    if len(worker_ids) > configured_workers:
        errors.append(f"observed {len(worker_ids)} workers, expected at most {configured_workers}")
    if all_spans:
        window_seconds = (max(int(s["ended_at"]) for s in all_spans)
                          - min(int(s["started_at"]) for s in all_spans)) / 1000
    else:
        window_seconds = 0
    useful_ms = sum(spec.duration_ms for spec in measured)
    lower_bound_ms = max(useful_ms / capacity, critical_path_ms(measured))
    efficiency = lower_bound_ms / (window_seconds * 1000) if window_seconds > 0 else 0
    wall_efficiency = lower_bound_ms / (wall_seconds * 1000) if wall_seconds > 0 else 0
    slot_utilization = useful_ms / (capacity * window_seconds * 1000) if window_seconds > 0 else 0
    queue_ms = [int(s["started_at"]) - int(s.get("eligible_at", s["started_at"])) for s in all_spans]
    execution_ms = [int(s["ended_at"]) - int(s["started_at"]) for s in all_spans]
    return {
        "label": label, "suite": suite, "pattern": pattern,
        "tasks": len(measured), "edges": sum(len(spec.parents) for spec in measured),
        "requested_duration_ms": statistics.median(spec.duration_ms for spec in measured),
        "requested_work_ms": useful_ms, "critical_path_ms": critical_path_ms(measured),
        "ideal_lower_bound_ms": lower_bound_ms, "wall_seconds": round(wall_seconds, 4),
        "execution_window_seconds": round(window_seconds, 4),
        "efficiency": round(min(efficiency, 1.0), 6),
        "end_to_end_efficiency": round(min(wall_efficiency, 1.0), 6),
        "slot_utilization": round(min(slot_utilization, 1.0), 6),
        "throughput_tasks_per_second": round(len(measured) / window_seconds, 3) if window_seconds else 0,
        "queue_ms_p50": round(percentile(queue_ms, .50), 3) if queue_ms else None,
        "queue_ms_p95": round(percentile(queue_ms, .95), 3) if queue_ms else None,
        "queue_ms_p99": round(percentile(queue_ms, .99), 3) if queue_ms else None,
        "execution_ms_p50": round(percentile(execution_ms, .50), 3) if execution_ms else None,
        "execution_ms_p95": round(percentile(execution_ms, .95), 3) if execution_ms else None,
        "execution_ms_p99": round(percentile(execution_ms, .99), 3) if execution_ms else None,
        "queue_ms_samples": queue_ms, "execution_ms_samples": execution_ms,
        "max_concurrency": max_concurrency(all_spans), "workers": worker_ids,
        "configured_workers": configured_workers, "slots_per_worker": slots_per_worker,
        "capacity": capacity, "submission_response": response,
        "spans": len(all_spans), "duplicate_attempts": duplicates,
        "dependency_order_violations": len(ordering_errors), "errors": errors,
        "valid": not errors and response == "DAG_ACCEPTED",
    }


class LiveRunner:
    def __init__(self, output: Path, telemetry: Path, configured_workers: int,
                 slots_per_worker: int, timeout: float):
        self.output = output
        self.telemetry = telemetry
        self.configured_workers = configured_workers
        self.slots_per_worker = slots_per_worker
        self.capacity = configured_workers * slots_per_worker
        self.timeout = timeout
        self.client = TitanClient()
        self.stage = Path(__file__).with_name("stage.py")

    def run(self, suite: str, label: str, pattern: str, specs: list[TaskSpec], pass_index: int) -> dict:
        digest = hashlib.sha1(f"{label}-{pass_index}-{time.time_ns()}".encode()).hexdigest()[:10]
        prefix = f"tb_{digest}"
        remap = {spec.node_id: f"{prefix}_{i:04d}" for i, spec in enumerate(specs)}
        remapped = [TaskSpec(remap[spec.node_id], tuple(remap[p] for p in spec.parents),
                             spec.duration_ms, spec.mode) for spec in specs]
        with_sink, sink = add_sink(remapped, prefix)
        jobs = [TitanJob(spec.node_id, str(self.stage),
                         args=f"{spec.node_id} {spec.mode} {spec.duration_ms / 1000:.6f}",
                         parents=list(spec.parents)) for spec in with_sink]
        expected = {"DAG-" + spec.node_id for spec in with_sink}
        offsets = telemetry_offsets(self.telemetry)
        dag_name = f"TASKBENCH_{suite.upper()}_{label.upper()}_P{pass_index}_{digest}".replace(".", "_")
        started_at_ms = int(time.time() * 1000)
        started = time.monotonic()
        response = self.client.submit_dag(dag_name, jobs)
        if response != "DAG_ACCEPTED":
            raise RuntimeError(f"{dag_name} rejected: {response}")
        deadline = time.monotonic() + self.timeout + sum(s.duration_ms for s in specs) / 1000
        status = ""
        while time.monotonic() < deadline:
            status = self.client.get_job_status("DAG-" + sink).strip()
            if status in TERMINAL:
                break
            time.sleep(.2)
        wall = time.monotonic() - started
        if status != "COMPLETED":
            raise TimeoutError(f"{dag_name} sink ended as {status or 'TIMEOUT'}")
        spans = {}
        for _ in range(80):
            spans = read_new_spans(self.telemetry, offsets, expected)
            if all(spans[job_id] for job_id in expected):
                break
            time.sleep(.1)
        result = summarize_case(label, suite, pattern, with_sink, sink, spans, self.capacity,
                                wall, response, self.configured_workers, self.slots_per_worker)
        ended_at_ms = int(time.time() * 1000)
        metrics = titan_stats_query(f"metrics|{started_at_ms}|5000")
        result["phase_samples"] = phase_samples(metrics, expected, started_at_ms, ended_at_ms)
        result.update({"pass": pass_index, "dag": dag_name,
                       "dag_url": f"http://127.0.0.1:5000/dags/DAG-{dag_name}"})
        return result


def configure_persistence(output: Path, dashboard_visible: bool) -> dict[str, str]:
    if dashboard_visible:
        manifest = REPO / ".titan_dag_manifest.json"
        journal = REPO / ".titan_dag_journal"
        history = REPO / ".titan_dag_run_history.jsonl"
    else:
        manifest = output / "manifest.json"
        journal = output / "dag-journal"
        history = output / "run-history.jsonl"
    values = {"TITAN_DAG_MANIFEST": str(manifest), "TITAN_DAG_JOURNAL_DIR": str(journal),
              "TITAN_DAG_RUN_HISTORY": str(history),
              "TITAN_DAG_WORKSPACE_DIR": str(REPO / "titan_workspace")}
    os.environ.update(values)
    return values


def median_rows(results: list[dict], suite: str, key: str) -> list[dict]:
    groups: dict[float | str, list[dict]] = defaultdict(list)
    for result in results:
        if result["suite"] == suite:
            groups[result[key]].append(result)
    rows = []
    for value, cases in sorted(groups.items(), key=lambda item: float(item[0]) if str(item[0]).replace('.', '', 1).isdigit() else str(item[0])):
        queue_samples = [sample for case in cases for sample in case.get("queue_ms_samples", [])]
        execution_samples = [sample for case in cases for sample in case.get("execution_ms_samples", [])]
        efficiency = variability(c["efficiency"] for c in cases)
        end_to_end = variability(c["end_to_end_efficiency"] for c in cases)
        window = variability(c["execution_window_seconds"] for c in cases)
        throughput = variability(c["throughput_tasks_per_second"] for c in cases)
        rows.append({key: value, "passes": len(cases), "valid_passes": sum(c["valid"] for c in cases),
                     "efficiency": efficiency["median"],
                     "end_to_end_efficiency": end_to_end["median"],
                     "window_seconds": window["median"],
                     "throughput": throughput["median"],
                     "queue_p50_ms": round(percentile(queue_samples, .50), 3) if queue_samples else None,
                     "queue_p95_ms": round(percentile(queue_samples, .95), 3) if queue_samples else None,
                     "queue_p99_ms": round(percentile(queue_samples, .99), 3) if queue_samples else None,
                     "execution_p50_ms": round(percentile(execution_samples, .50), 3) if execution_samples else None,
                     "execution_p95_ms": round(percentile(execution_samples, .95), 3) if execution_samples else None,
                     "execution_p99_ms": round(percentile(execution_samples, .99), 3) if execution_samples else None,
                     "variability": {"efficiency": efficiency, "end_to_end_efficiency": end_to_end,
                                     "window_seconds": window, "throughput": throughput}})
    return rows


def latency_breakdown(results: list[dict]) -> dict:
    phases = ["queue_ms_samples", "dispatch_route_ms_samples", "worker_selection_ms_samples",
              "dispatch_record_ms_samples", "dispatch_store_ms_samples", "dispatch_rpc_ms_samples",
              "worker_queue_ms_samples", "preprocess_ms_samples", "process_startup_ms_samples",
              "script_execution_ms_samples", "callback_transit_ms_samples",
              "callback_handle_ms_samples", "terminal_persistence_ms_samples",
              "store_read_ms_samples", "store_write_ms_samples"]
    aggregated = {}
    for phase in phases:
        if phase == "queue_ms_samples":
            values = [float(sample) for case in results for sample in case.get(phase, [])]
        else:
            values = [float(sample) for case in results
                      for sample in (case.get("phase_samples") or {}).get(phase, [])]
        aggregated[phase.removesuffix("_samples")] = {
            "count": len(values), "p50": round(percentile(values, .50), 6) if values else None,
            "p95": round(percentile(values, .95), 6) if values else None,
            "p99": round(percentile(values, .99), 6) if values else None,
            "mean": round(statistics.fmean(values), 6) if values else None,
        }
    return aggregated


def observed_metg(rows: list[dict], threshold: float = .5) -> dict:
    valid = [row for row in rows if row["valid_passes"] == row["passes"]]
    hit = next((row for row in valid if row["efficiency"] >= threshold), None)
    previous = None
    if hit:
        previous = next((row for row in reversed(valid) if float(row["requested_duration_ms"]) < float(hit["requested_duration_ms"])), None)
    return {"threshold": threshold, "observed_ms": hit["requested_duration_ms"] if hit else None,
            "previous_tested_ms": previous["requested_duration_ms"] if previous else None,
            "censored": hit is None}


def svg_line(rows: list[dict]) -> str:
    if not rows:
        return "<p>No granularity results.</p>"
    width, height, left, right, top, bottom = 760, 300, 72, 24, 24, 54
    xs = [math.log10(max(.1, float(row["requested_duration_ms"]))) for row in rows]
    lo, hi = min(xs), max(xs)
    x = lambda v: left + (v - lo) / max(.001, hi - lo) * (width - left - right)
    y = lambda v: top + (1 - min(1, max(0, v))) * (height - top - bottom)
    points = " ".join(f"{x(v):.1f},{y(row['efficiency']):.1f}" for v, row in zip(xs, rows))
    marks = []
    for xv, row in zip(xs, rows):
        marks.append(f'<circle cx="{x(xv):.1f}" cy="{y(row["efficiency"]):.1f}" r="4"/>'
                     f'<text x="{x(xv):.1f}" y="{height-29}" text-anchor="middle">{row["requested_duration_ms"]:g}ms</text>')
    grid = "".join(f'<line x1="{left}" x2="{width-right}" y1="{y(v):.1f}" y2="{y(v):.1f}"/>'
                   f'<text x="{left-12}" y="{y(v)+4:.1f}" text-anchor="end">{v:.0%}</text>'
                   for v in (0, .25, .5, .75, 1))
    return (f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" '
            'aria-label="Efficiency by requested task duration on a logarithmic x axis">'
            f'<g class="grid">{grid}</g><line class="threshold" x1="{left}" x2="{width-right}" y1="{y(.5):.1f}" y2="{y(.5):.1f}"/>'
            f'<polyline class="series" points="{points}"/><g class="marks">{"".join(marks)}</g>'
            f'<text class="axis-title" x="{(left+width-right)/2:.1f}" y="{height-5}" text-anchor="middle">requested duration · logarithmic spacing</text>'
            f'<text class="axis-title" transform="translate(16 {(top+height-bottom)/2:.1f}) rotate(-90)" text-anchor="middle">useful-work efficiency</text></svg>')


def render_report(summary: dict, destination: Path) -> None:
    h = html.escape
    rows = summary["granularity_summary"]
    metg = summary["metg50"]
    if metg["observed_ms"] is not None:
        threshold_text = f"{metg['observed_ms']:g} ms"
    elif rows:
        threshold_text = f"> {max(float(r['requested_duration_ms']) for r in rows):g} ms"
    else:
        threshold_text = "not measured"
    granularity_table = "".join(
        f"<tr><td>{row['requested_duration_ms']:g} ms</td><td>{row['passes']}</td>"
        f"<td>{row['efficiency']:.1%}</td><td>{row['end_to_end_efficiency']:.1%}</td>"
        f"<td>{row['window_seconds']:.2f} s</td><td>{row['throughput']:.2f}</td>"
        f"<td>{row['queue_p50_ms']:.1f} / {row['queue_p95_ms']:.1f} / {row['queue_p99_ms']:.1f} ms</td>"
        f"<td>{row['execution_p50_ms']:.1f} / {row['execution_p95_ms']:.1f} / {row['execution_p99_ms']:.1f} ms</td>"
        f"<td>{row['variability']['throughput']['min']:.2f}–{row['variability']['throughput']['max']:.2f}<br>"
        f"CV {row['variability']['throughput']['coefficient_of_variation']:.1%}</td></tr>"
        for row in rows)
    latency_rows = "".join(
        f"<tr><td>{h(name.replace('_ms', '').replace('_', ' '))}</td><td>{values['count']}</td>"
        f"<td>{values['mean'] if values['mean'] is not None else '—'}</td>"
        f"<td>{values['p50'] if values['p50'] is not None else '—'}</td>"
        f"<td>{values['p95'] if values['p95'] is not None else '—'}</td>"
        f"<td>{values['p99'] if values['p99'] is not None else '—'}</td></tr>"
        for name, values in summary.get("latency_breakdown", {}).items())
    def suite_table(name: str) -> str:
        cases = [r for r in summary["results"] if r["suite"] == name]
        if not cases:
            return "<p class='muted'>Not run in this invocation.</p>"
        return "<div class='table-wrap'><table><thead><tr><th>Case</th><th>Tasks</th><th>Pattern</th><th>Window</th><th>Bound efficiency</th><th>Slot utilization</th><th>Throughput</th><th>Workers</th><th>Evidence</th></tr></thead><tbody>" + "".join(
            f"<tr><td>{h(c['label'])}</td><td>{c['tasks']}</td><td>{h(c['pattern'])}</td>"
            f"<td>{c['execution_window_seconds']:.2f} s</td><td>{c['efficiency']:.1%}</td>"
            f"<td>{c.get('slot_utilization', min(1, c['requested_work_ms'] / (c['capacity'] * c['execution_window_seconds'] * 1000)) if c['execution_window_seconds'] else 0):.1%}</td>"
            f"<td>{c['throughput_tasks_per_second']:.2f}/s</td><td>{len(c['workers'])}</td>"
            f"<td><span class='pill {'pass' if c['valid'] else 'fail'}'>{'valid' if c['valid'] else 'invalid'}</span></td></tr>" for c in cases) + "</tbody></table></div>"
    validity = sum(result["valid"] for result in summary["results"])
    document = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Titan task granularity benchmark</title><style>
:root{{--bg:#081014;--panel:#101b20;--line:#264048;--text:#e9f2f2;--muted:#94aeb2;--green:#54e6a8;--amber:#f6c85f;--red:#ff7085;--blue:#72c7ff}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 Inter,Segoe UI,Arial,sans-serif}}main{{max-width:1180px;margin:auto;padding:30px 24px 70px}}h1{{font-size:32px;margin:.2rem 0}}h2{{font-size:20px;margin:0 0 14px}}h3{{font-size:15px}}p{{max-width:900px}}.eyebrow{{color:var(--green);text-transform:uppercase;letter-spacing:.14em;font-weight:700;font-size:11px}}.lede,.muted{{color:var(--muted)}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;margin:22px 0}}.card,section{{background:var(--panel);border:1px solid var(--line);border-radius:10px}}.card{{padding:16px}}.card strong{{display:block;font-size:24px;margin-top:5px}}section{{padding:20px;margin:14px 0}}.table-wrap{{overflow:auto}}table{{border-collapse:collapse;width:100%;min-width:760px}}th,td{{padding:9px 11px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}}th{{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.08em}}.chart{{width:100%;max-height:360px;background:#0b1519;border:1px solid var(--line);border-radius:8px}}.grid line{{stroke:#1d3239}}.grid text,.marks text,.axis-title{{fill:var(--muted);font-size:11px}}.threshold{{stroke:var(--amber);stroke-dasharray:6 5}}.series{{fill:none;stroke:var(--green);stroke-width:3}}.marks circle{{fill:var(--bg);stroke:var(--green);stroke-width:3}}code{{color:#b8e6ff}}.pill{{padding:3px 8px;border-radius:20px;font-weight:700;font-size:11px}}.pill.pass{{background:#143f33;color:var(--green)}}.pill.fail{{background:#4a2029;color:var(--red)}}details{{border-top:1px solid var(--line);padding-top:12px;margin-top:12px}}summary{{cursor:pointer;font-weight:700}}a{{color:var(--blue)}}
</style></head><body><main><div class="eyebrow">Titan · controlled scheduler microbenchmark</div><h1>Minimum effective task granularity</h1><p class="lede">A logarithmic duration sweep plus DAG shape, scale, local overlap and load-imbalance probes. Results come from Titan's persisted execution spans; every live case checks terminal status, dependency order, duplicate attempts and observed worker count.</p>
<div class="cards"><div class="card">Observed METG(50)<strong>{h(threshold_text)}</strong><span class="muted">smallest tested duration at ≥50% scheduler-window efficiency</span></div><div class="card">Controlled capacity<strong>{summary['configuration']['configured_workers']} × {summary['configuration']['slots_per_worker']}</strong><span class="muted">workers × slots per worker</span></div><div class="card">Evidence validity<strong>{validity} / {len(summary['results'])}</strong><span class="muted">live cases passing all checks</span></div><div class="card">Source state<strong>{h(summary['source']['branch'])}</strong><span class="muted"><code>{h(summary['source']['commit'][:12])}</code></span></div></div>
<section><h2>Logarithmic task-duration sweep</h2><p>Efficiency = ideal useful-work lower bound ÷ observed span window. One warm-up pass per duration is excluded from every number below. The table aggregates all measured task spans across five passes and reports p50 / p95 / p99 consistently. The amber line marks 50%.</p>{svg_line(rows)}<div class="table-wrap"><table><thead><tr><th>Duration</th><th>Measured passes</th><th>Window efficiency</th><th>End-to-end efficiency</th><th>Window</th><th>Tasks/s</th><th>Queue p50 / p95 / p99</th><th>Execution p50 / p95 / p99</th><th>Throughput range / CV</th></tr></thead><tbody>{granularity_table}</tbody></table></div></section>
<section><h2>Correlated latency breakdown</h2><p class="muted">Worker phases use a monotonic clock on one worker. Callback transit compares worker and master wall clocks and therefore assumes synchronized VM clocks. Store rows include operations observed inside each measured case window.</p><div class="table-wrap"><table><thead><tr><th>Phase</th><th>Samples</th><th>Mean ms</th><th>P50 ms</th><th>P95 ms</th><th>P99 ms</th></tr></thead><tbody>{latency_rows}</tbody></table></div></section>
<section><h2>Graph patterns</h2><p class="muted">Critical path and total work both contribute to the ideal lower bound. Bound efficiency measures proximity to the best possible schedule for that graph; slot utilization shows how much controlled capacity performed requested work.</p>{suite_table('patterns')}</section>
<section><h2>Scale</h2>{suite_table('scale')}</section>
<section><h2>Communication-hiding proxy</h2><p class="muted">This local test compares barriered and pipelined wait/CPU stages. It measures latency overlap on one host; it is not evidence of network communication hiding.</p>{suite_table('overlap')}</section>
<section><h2>Load imbalance</h2>{suite_table('imbalance')}</section>
<section><h2>Interpretation</h2><ul><li><strong>Below METG:</strong> process launch, RPC, queueing and callback overhead dominate useful work.</li><li><strong>At or above METG:</strong> at least half of the ideal controlled-capacity window is useful task time.</li><li><strong>Pattern sensitivity:</strong> compare chain and fan-in against bag tasks to expose dependency-release costs.</li><li><strong>Scale sensitivity:</strong> throughput should stabilize as task count grows if fixed startup costs are being amortized.</li><li><strong>Imbalance:</strong> a falling lower-bound efficiency identifies tail loss from uneven task lengths.</li></ul><div class="muted">METG is configuration specific. It must be remeasured for a different worker count, slot count, executor, hardware or network.</div></section>
<section><h2>Method and evidence</h2><details open><summary>Warm-up and measured-pass protocol</summary><p>Each duration runs {summary['configuration'].get('warmup_passes', 0)} warm-up pass before {summary['configuration']['passes']} measured passes. Warm-up results are retained in <code>summary.json</code> for audit but excluded from percentiles, METG, confidence intervals and all headline values.</p><p>Latency percentiles use linear interpolation over all measured task spans. Each pass-level median reports min–max, standard deviation, coefficient of variation and a deterministic 10,000-resample percentile-bootstrap 95% confidence interval.</p></details><details><summary>Configuration</summary><pre>{h(json.dumps(summary['configuration'], indent=2))}</pre></details><details><summary>Validity and raw-result notes</summary><pre>{h(json.dumps(summary['validity'], indent=2))}</pre></details><details><summary>Definition</summary><p>This report adopts the Task Bench METG(50%) convention and makes Titan's efficiency denominator explicit. The original Task Bench definition is the smallest average task granularity at which an application reaches at least 50% of the machine's peak application performance.</p></details></section>
<p class="muted">Generated {h(summary['generated_at'])}. Self-contained HTML; no external JavaScript or CSS.</p></main></body></html>"""
    destination.write_text(document, encoding="utf-8")


def git_value(*args: str) -> str:
    import subprocess
    try:
        return subprocess.check_output(["git", "-C", str(REPO), *args], text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="submit cases to the live Titan cluster")
    parser.add_argument("--suites", default="granularity,patterns,scale,overlap,imbalance")
    parser.add_argument("--durations-ms", default="10,20,50,100,200,500,1000,2000")
    parser.add_argument("--tasks", type=int, default=32)
    parser.add_argument("--pattern-tasks", type=int, default=24)
    parser.add_argument("--pattern-duration-ms", type=float, default=250)
    parser.add_argument("--passes", type=int, default=5)
    parser.add_argument("--warmup-passes", type=int, default=1,
                        help="unmeasured warm-up passes per granularity duration")
    parser.add_argument("--configured-workers", type=int, default=1)
    parser.add_argument("--slots-per-worker", type=int, default=4)
    parser.add_argument("--scale-tasks", default="32,128,512")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--telemetry", type=Path, default=REPO / "telemetry")
    parser.add_argument("--output", type=Path, default=REPO / "benchmark_results" / "task-granularity")
    parser.add_argument("--dashboard-visible", action="store_true")
    args = parser.parse_args()
    if not args.live:
        parser.error("--live is required; offline estimates are not reported as Titan measurements")
    if not 2 <= args.tasks <= 5000 or not 1 <= args.passes <= 10 or not 0 <= args.warmup_passes <= 3:
        parser.error("tasks must be 2-5000 and passes 1-10")
    suites = {item.strip() for item in args.suites.split(",") if item.strip()}
    unknown = suites - {"granularity", "patterns", "scale", "overlap", "imbalance"}
    if unknown:
        parser.error("unknown suites: " + ", ".join(sorted(unknown)))
    durations = parse_csv_numbers(args.durations_ms)
    scale_tasks = parse_csv_numbers(args.scale_tasks, int)
    if any(value <= 0 or value > 60_000 for value in durations):
        parser.error("durations must be >0 and <=60000 ms")
    stamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    output = args.output / stamp
    output.mkdir(parents=True, exist_ok=True)
    persistence = configure_persistence(output, args.dashboard_visible)
    runner = LiveRunner(output, args.telemetry, args.configured_workers,
                        args.slots_per_worker, args.timeout)
    results: list[dict] = []
    warmup_results: list[dict] = []
    if "granularity" in suites:
        for duration in durations:
            for warmup_index in range(1, args.warmup_passes + 1):
                specs = make_graph("bag", args.tasks, duration, "g")
                warmup = runner.run("granularity", f"{duration:g}ms", "bag", specs, -warmup_index)
                warmup.update({"measurement_phase": "warmup", "warmup_pass": warmup_index})
                warmup_results.append(warmup)
                print(json.dumps({"case": warmup["label"], "warmup": warmup_index,
                                  "efficiency": warmup["efficiency"], "valid": warmup["valid"]}), flush=True)
            for pass_index in range(1, args.passes + 1):
                specs = make_graph("bag", args.tasks, duration, "g")
                result = runner.run("granularity", f"{duration:g}ms", "bag", specs, pass_index)
                result["measurement_phase"] = "measured"
                results.append(result)
                print(json.dumps({"case": result["label"], "pass": pass_index,
                                  "efficiency": result["efficiency"], "valid": result["valid"]}), flush=True)
    if "patterns" in suites:
        for pattern in ("bag", "chain", "fanout", "fanin", "diamond", "layered", "stencil"):
            for pass_index in range(1, args.passes + 1):
                specs = make_graph(pattern, args.pattern_tasks, args.pattern_duration_ms, "p")
                results.append(runner.run("patterns", pattern, pattern, specs, pass_index))
    if "scale" in suites:
        for count in scale_tasks:
            specs = make_graph("bag", count, args.pattern_duration_ms, "s")
            results.append(runner.run("scale", f"{count}-tasks", "bag", specs, 1))
    if "overlap" in suites:
        count = max(4, args.pattern_tasks // 2)
        wait_ids = [f"ow{i:03d}" for i in range(count)]
        cpu_ids = [f"oc{i:03d}" for i in range(count)]
        barrier = [TaskSpec(wait_ids[i], (), args.pattern_duration_ms, "sleep") for i in range(count)]
        barrier += [TaskSpec(cpu_ids[i], tuple(wait_ids), args.pattern_duration_ms, "cpu") for i in range(count)]
        pipelined = [TaskSpec(wait_ids[i], (), args.pattern_duration_ms, "sleep") for i in range(count)]
        pipelined += [TaskSpec(cpu_ids[i], (wait_ids[i],), args.pattern_duration_ms, "cpu") for i in range(count)]
        results.append(runner.run("overlap", "barriered", "wait-then-cpu-barrier", barrier, 1))
        results.append(runner.run("overlap", "pipelined", "wait-cpu-pipeline", pipelined, 1))
    if "imbalance" in suites:
        count, base = args.pattern_tasks, args.pattern_duration_ms
        distributions = {
            "uniform": [base] * count,
            "bimodal": [base * (4 if i % 5 == 0 else .25) for i in range(count)],
            "one-straggler": [base] * (count - 1) + [base * 10],
        }
        for label, vector in distributions.items():
            specs = make_graph("bag", count, base, "i", vector)
            results.append(runner.run("imbalance", label, "bag", specs, 1))
    granularity = median_rows(results, "granularity", "requested_duration_ms")
    summary = {
        "schema": "titan-task-granularity-v1", "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {"branch": git_value("branch", "--show-current"), "commit": git_value("rev-parse", "HEAD"),
                   "dirty": bool(git_value("status", "--porcelain"))},
        "configuration": {"configured_workers": args.configured_workers,
                          "slots_per_worker": args.slots_per_worker,
                          "capacity": runner.capacity, "passes": args.passes,
                          "warmup_passes": args.warmup_passes,
                          "durations_ms": durations, "tasks": args.tasks,
                          "pattern_tasks": args.pattern_tasks,
                          "pattern_duration_ms": args.pattern_duration_ms,
                          "suites": sorted(suites), "telemetry": str(args.telemetry.resolve()),
                          "dashboard_visible": args.dashboard_visible,
                          "persistence": persistence},
        "metg50": observed_metg(granularity), "granularity_summary": granularity,
        "latency_breakdown": latency_breakdown(results),
        "results": results, "warmup_results": warmup_results,
        "validity": {"valid_cases": sum(r["valid"] for r in results), "total_cases": len(results),
                     "invalid_cases": [{"label": r["label"], "errors": r["errors"]} for r in results if not r["valid"]],
                     "limitations": ["worker-host topology must be reported separately; no network communication-hiding claim",
                                     "Python process launch is part of measured Titan execution overhead",
                                     "METG is specific to this worker/slot/hardware configuration"]},
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    render_report(summary, output / "report.html")
    print(output / "report.html")


if __name__ == "__main__":
    main()
