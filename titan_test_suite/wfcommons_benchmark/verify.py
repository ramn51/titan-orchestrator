"""Deterministically verify Titan WfCommons benchmark runs from persisted evidence.

The verifier never queries a live Titan process. It reconstructs the submitted
graph from the official WfFormat input and benchmark result, then checks the
persisted execution spans. Exit status is zero only when every requested case
passes all correctness checks.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Iterable

from benchmark import read_graph, sample_graph


@dataclass
class CaseAudit:
    instance: str
    result: str
    dag: str = ""
    passed: bool = False
    evidence_mode: str = ""
    input_sha256: str = ""
    expected_jobs: int = 0
    observed_jobs: int = 0
    expected_edges: int = 0
    observed_edges: int = 0
    completed_jobs: int = 0
    duplicate_span_records: int = 0
    retry_jobs: int = 0
    unknown_job_spans: int = 0
    missing_jobs: list[str] = field(default_factory=list)
    parent_mismatches: list[dict] = field(default_factory=list)
    ordering_violations: list[dict] = field(default_factory=list)
    non_completed_jobs: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    timing: dict = field(default_factory=dict)
    capacity: dict = field(default_factory=dict)
    dag_shape: dict = field(default_factory=dict)
    latency: dict = field(default_factory=dict)
    telemetry: dict = field(default_factory=dict)


def read_spans(paths: Iterable[Path]) -> list[dict]:
    spans = []
    for path in paths:
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{path}:{line_number}: invalid JSON: {exc}") from exc
                if not isinstance(item, dict) or not item.get("job_id"):
                    raise ValueError(f"{path}:{line_number}: span must be an object with job_id")
                spans.append(item)
    return spans


def _stamp_from_dag(dag: str) -> str:
    stamp = dag.rsplit("_", 1)[-1]
    if not stamp.isdigit():
        raise ValueError(f"DAG name does not end in the benchmark timestamp: {dag}")
    return stamp


def _latest_span(items: list[dict]) -> dict:
    return max(items, key=lambda span: (
        int(span.get("attempt") or 0), int(span.get("ended_at") or 0)
    ))


def _downsample(points: list[dict], limit: int = 240) -> list[dict]:
    if len(points) <= limit:
        return points
    indexes = {round(index * (len(points) - 1) / (limit - 1)) for index in range(limit)}
    return [point for index, point in enumerate(points) if index in indexes]


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    value = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    return round(value, 3)


def _distribution(values: list[float]) -> dict:
    return {"count": len(values), "min": round(min(values), 3) if values else None,
            "p50": _percentile(values, .50), "p95": _percentile(values, .95),
            "max": round(max(values), 3) if values else None,
            "mean": round(sum(values) / len(values), 3) if values else None}


def analyze_dag_shape(expected_parents: dict[str, set[str]]) -> dict:
    children = defaultdict(set)
    for child, parents in expected_parents.items():
        for parent in parents:
            children[parent].add(child)
    remaining = {job: len(parents) for job, parents in expected_parents.items()}
    depth = {job: 1 for job, count in remaining.items() if count == 0}
    ready = sorted(depth)
    while ready:
        parent = ready.pop(0)
        for child in children[parent]:
            remaining[child] -= 1
            depth[child] = max(depth.get(child, 1), depth[parent] + 1)
            if remaining[child] == 0:
                ready.append(child)
    layer_width = Counter(depth.values())
    return {
        "roots": sum(not parents for parents in expected_parents.values()),
        "leaves": sum(not children[job] for job in expected_parents),
        "max_fan_in": max(map(len, expected_parents.values()), default=0),
        "max_fan_out": max(map(len, children.values()), default=0),
        "depth": max(depth.values(), default=0),
        "maximum_layer_width": max(layer_width.values(), default=0),
    }


def analyze_latency(latest: dict[str, dict]) -> dict:
    ready_wait, execution = [], []
    for span in latest.values():
        eligible = int(span.get("eligible_at") or span.get("queued_at") or 0)
        started = int(span.get("started_at") or 0)
        ended = int(span.get("ended_at") or 0)
        if eligible and started >= eligible:
            ready_wait.append(float(started - eligible))
        if started and ended >= started:
            execution.append(float(ended - started))
    return {"ready_queue_ms": _distribution(ready_wait),
            "scheduler_to_callback_ms": _distribution(execution)}


def analyze_telemetry(telemetry: dict | None, start: int, end: int) -> dict:
    metrics = (telemetry or {}).get("metrics") or {}
    samples = sorted((sample for sample in metrics.get("samples") or []
                      if start <= int(sample.get("at") or 0) <= end),
                     key=lambda sample: int(sample["at"]))
    phases = [phase for phase in metrics.get("dispatch_phases") or []
              if start <= int(phase.get("at") or 0) <= end]
    stores = [operation for operation in metrics.get("store_operations") or []
              if start <= int(operation.get("at") or 0) <= end]
    events = [event for event in metrics.get("events") or []
              if start <= int(event.get("at") or 0) <= end]
    if not samples:
        return {"available": False, "samples": 0, "dispatch_samples": len(phases),
                "store_samples": len(stores), "events": events}
    workers = [worker for sample in samples for worker in sample.get("workers") or []]
    capacities = [sum(int(worker.get("capacity") or 0) for worker in sample.get("workers") or [])
                  for sample in samples]
    used = [sum(int(worker.get("load") or 0) for worker in sample.get("workers") or [])
            for sample in samples]
    phase_summary = {name + "_ms": _distribution([float(item.get(name) or 0) for item in phases])
                     for name in ("route", "select", "record", "store", "send")}
    store_summary = {}
    for kind in ("read", "write"):
        store_summary[kind + "_ms"] = _distribution(
            [float(item.get("ms") or 0) for item in stores if item.get("kind") == kind]
        )
    return {
        "available": True,
        "samples": len(samples),
        "resolution_seconds": metrics.get("resolution_seconds"),
        "max_ready_queue": max(int(item.get("queue_depth") or 0) for item in samples),
        "max_dependency_wait": max(int(item.get("dependency_wait") or 0) for item in samples),
        "max_capability_wait": max(int(item.get("capability_wait") or 0) for item in samples),
        "max_running": max(int(item.get("running") or 0) for item in samples),
        "max_oldest_ready_ms": max(int(item.get("oldest_ready_ms") or 0) for item in samples),
        "worker_count_min": min(len(item.get("workers") or []) for item in samples),
        "worker_count_max": max(len(item.get("workers") or []) for item in samples),
        "capacity_min": min(capacities), "capacity_max": max(capacities),
        "slots_used_mean": round(sum(used) / len(used), 3), "slots_used_max": max(used),
        "slot_utilization_percent": round(
            100 * sum(used) / sum(capacities), 2
        ) if sum(capacities) else None,
        "worker_cpu_peak_percent": round(max((float(worker.get("cpu_percent") or 0)
                                               for worker in workers), default=0), 2),
        "worker_memory_peak_percent": round(max((100 * int(worker.get("memory_used_bytes") or 0)
                                                  / int(worker.get("memory_total_bytes") or 1)
                                                  for worker in workers), default=0), 2),
        "heartbeat_rtt_p95_ms": _percentile(
            [float(worker.get("heartbeat_rtt_ms") or 0) for worker in workers], .95
        ),
        "missed_heartbeats_max": max(int(item.get("missed_heartbeats") or 0) for item in samples),
        "dropped_span_writes_max": max(int(item.get("dropped_span_writes") or 0) for item in samples),
        "pending_span_writes_max": max(int(item.get("pending_span_writes") or 0) for item in samples),
        "store_write_failures_max": max(int(item.get("store_dropped_writes") or 0) for item in samples),
        "store_unhealthy_samples": sum(not bool(item.get("store_healthy")) for item in samples),
        "dispatch_samples": len(phases), "dispatch_phases": phase_summary,
        "store_samples": len(stores), "store_operations": store_summary,
        "events": events,
    }


def analyze_capacity(latest: dict[str, dict], telemetry: dict | None) -> tuple[dict, list[str]]:
    """Measure execution occupancy and, when available, actual fleet samples."""
    warnings = []
    valid = [span for span in latest.values()
             if int(span.get("started_at") or 0) and int(span.get("ended_at") or 0)]
    if not valid:
        return {"span_window_seconds": None, "observed_workers": 0,
                "peak_slots_used": 0, "workers": [], "slot_usage_series": [],
                "fleet_series": [], "telemetry_available": False}, warnings
    start = min(int(span["started_at"]) for span in valid)
    end = max(int(span["ended_at"]) for span in valid)
    events = []
    jobs_per_worker = Counter()
    for span in valid:
        worker = str(span.get("worker") or "unknown")
        jobs_per_worker[worker] += 1
        events.append((int(span["started_at"]), 1, worker))
        events.append((int(span["ended_at"]), -1, worker))
    active = Counter()
    peak_by_worker = Counter()
    peak_total = 0
    slot_series = []
    for at, delta, worker in sorted(events, key=lambda item: (item[0], item[1])):
        active[worker] += delta
        peak_by_worker[worker] = max(peak_by_worker[worker], active[worker])
        total = sum(active.values())
        peak_total = max(peak_total, total)
        slot_series.append({"at": at, "used": total})
    workers = [{"id": worker, "jobs": jobs_per_worker[worker],
                "peak_slots_used": peak_by_worker[worker]}
               for worker in sorted(jobs_per_worker)]
    window_ms = max(1, end - start)
    busy_ms = sum(int(span["ended_at"]) - int(span["started_at"]) for span in valid)

    fleet_series = []
    samples = ((telemetry or {}).get("metrics") or {}).get("samples") or []
    for sample in samples:
        at = int(sample.get("at") or 0)
        if start <= at <= end:
            fleet = sample.get("workers") or []
            fleet_series.append({
                "at": at,
                "workers": len(fleet),
                "capacity": sum(int(worker.get("capacity") or 0) for worker in fleet),
                "used": sum(int(worker.get("load") or 0) for worker in fleet),
            })
    fleet_series.sort(key=lambda point: point["at"])
    if telemetry is not None and not fleet_series:
        resolution = (((telemetry or {}).get("metrics") or {}).get("resolution_seconds"))
        duration_seconds = (end - start) / 1000
        resolution_text = f" at {resolution}s resolution" if resolution else ""
        warnings.append(
            f"No periodic cluster snapshot fell inside this {duration_seconds:.1f}s run"
            f"{resolution_text}. Job spans and any timestamped dispatch, store, and event records "
            "remain available; queue, CPU, and memory snapshot statistics are unavailable for this run."
        )
    return {
        "span_window_seconds": round((end - start) / 1000, 3),
        "observed_workers": len(workers),
        "peak_slots_used": peak_total,
        "average_slots_used": round(busy_ms / window_ms, 3),
        "completed_jobs_per_second": round(len(valid) * 1000 / window_ms, 3),
        "workers": workers,
        "slot_usage_series": _downsample(slot_series),
        "fleet_series": _downsample(fleet_series),
        "telemetry_available": bool(fleet_series),
    }, warnings


def verify_case(instance_path: Path, result_path: Path, spans: list[dict],
                allow_retries: bool = False, timing_valid: bool = True,
                telemetry: dict | None = None, manifest: dict | None = None) -> CaseAudit:
    audit = CaseAudit(instance=str(instance_path), result=str(result_path))
    try:
        raw = instance_path.read_bytes()
        actual_sha = hashlib.sha256(raw).hexdigest()
        graph, graph_sha = read_graph(instance_path)
        result = json.loads(result_path.read_text(encoding="utf-8"))
        live = result["live"]
        sample = result["sample"]
        dag = str(live["dag"])
        stamp = _stamp_from_dag(dag)
        submission = result.get("submission") or {}
        submitted_jobs = submission.get("jobs") or []
        if submitted_jobs:
            expected_parents = {str(item["job_id"]): set(map(str, item.get("parents") or []))
                                for item in submitted_jobs}
            source_by_job = {str(item["job_id"]): str(item["source_task"])
                             for item in submitted_jobs}
            if len(source_by_job) != len(submitted_jobs):
                audit.errors.append("Submission plan contains duplicate job IDs")
            for job_id, source_task in source_by_job.items():
                if source_task not in graph:
                    audit.errors.append(f"Submission plan references unknown source task {source_task}")
                    continue
                expected_source_parents = {source_by_job[parent]
                                           for parent in expected_parents[job_id]
                                           if parent in source_by_job}
                actual_source_parents = set(graph[source_task]) & set(source_by_job.values())
                if expected_source_parents != actual_source_parents:
                    audit.errors.append(f"Submission plan differs from WfFormat parents for {source_task}")
            audit.evidence_mode = "recorded_submission_plan"
        else:
            manifest_jobs = {
                str(job_id): set(map(str, value.get("deps") or []))
                for job_id, value in (manifest or {}).items()
                if isinstance(value, dict) and value.get("dag") == dag
                and str(job_id).startswith("DAG-") and str(job_id).endswith("_" + stamp)
            }
            if manifest_jobs:
                expected_parents = manifest_jobs
                audit.evidence_mode = "legacy_manifest"
                audit.warnings.append(
                    "Legacy run has no recorded source-task mapping; deployed manifest shape was verified, "
                    "but the selected subset cannot be mapped back to individual WfFormat task IDs"
                )
            elif int(sample["tasks"]) == len(graph):
                selected = sample_graph(graph, int(sample["tasks"]))
                job_ids = {task: f"DAG-wfc_{index:04d}_{stamp}"
                           for index, task in enumerate(selected)}
                expected_parents = {
                    job_ids[task]: {job_ids[parent] for parent in graph[task] if parent in job_ids}
                    for task in selected
                }
                audit.evidence_mode = "reconstructed_full_graph"
            else:
                raise ValueError(
                    "Legacy sampled run cannot be reconstructed safely; supply --manifest or rerun with a recorded submission plan"
                )
        expected_ids = set(expected_parents)

        audit.dag = dag
        audit.input_sha256 = actual_sha
        audit.expected_jobs = len(expected_ids)
        audit.expected_edges = sum(map(len, expected_parents.values()))
        audit.timing = {
            "valid": timing_valid,
            "wall_seconds": live.get("wall_seconds"),
            "jobs_per_second": live.get("jobs_per_second"),
        }

        recorded_sha = str(result.get("instance", {}).get("sha256", ""))
        if graph_sha != actual_sha or recorded_sha != actual_sha:
            audit.errors.append(
                f"Input hash mismatch: actual={actual_sha}, result={recorded_sha or 'missing'}"
            )
        if int(sample.get("edges", -1)) != audit.expected_edges:
            audit.errors.append(
                f"Result edge count is {sample.get('edges')}; reconstructed graph has {audit.expected_edges}"
            )
        if int(live.get("tasks", -1)) != audit.expected_jobs:
            audit.errors.append(
                f"Result task count is {live.get('tasks')}; reconstructed graph has {audit.expected_jobs}"
            )

        by_job: dict[str, list[dict]] = defaultdict(list)
        unknown = 0
        for span in spans:
            job_id = str(span.get("job_id", ""))
            if job_id in expected_ids:
                by_job[job_id].append(span)
            elif job_id.endswith("_" + stamp):
                unknown += 1
        audit.unknown_job_spans = unknown
        audit.observed_jobs = len(by_job)
        audit.missing_jobs = sorted(expected_ids - set(by_job))
        audit.duplicate_span_records = sum(max(0, len(items) - 1) for items in by_job.values())
        audit.retry_jobs = sum(
            max((int(item.get("attempt") or 0) for item in items), default=0) > 1
            for items in by_job.values()
        )

        latest = {job_id: _latest_span(items) for job_id, items in by_job.items()}
        audit.capacity, capacity_warnings = analyze_capacity(latest, telemetry)
        audit.warnings.extend(capacity_warnings)
        audit.dag_shape = analyze_dag_shape(expected_parents)
        audit.latency = analyze_latency(latest)
        valid_times = [(int(span.get("started_at") or 0), int(span.get("ended_at") or 0))
                       for span in latest.values()
                       if int(span.get("started_at") or 0) and int(span.get("ended_at") or 0)]
        if valid_times:
            audit.telemetry = analyze_telemetry(
                telemetry, min(start for start, _ in valid_times), max(end for _, end in valid_times)
            )
            if 0 < audit.telemetry.get("samples", 0) < 3:
                audit.warnings.append(
                    f"Only {audit.telemetry['samples']} scheduler telemetry sample was retained; "
                    "queue, utilization, CPU, and memory values are snapshots rather than distributions"
                )
        else:
            audit.telemetry = {"available": False, "samples": 0}
        for job_id, span in sorted(latest.items()):
            observed = set(map(str, span.get("parents") or []))
            expected = expected_parents[job_id]
            audit.observed_edges += len(observed)
            if observed != expected:
                audit.parent_mismatches.append({
                    "job_id": job_id,
                    "missing": sorted(expected - observed),
                    "unexpected": sorted(observed - expected),
                })
            status = str(span.get("status", "UNKNOWN"))
            if status == "COMPLETED":
                audit.completed_jobs += 1
            else:
                audit.non_completed_jobs.append({
                    "job_id": job_id,
                    "status": status,
                    "attempt": span.get("attempt"),
                    "reason": span.get("reason", ""),
                })
            child_start = int(span.get("started_at") or 0)
            if not child_start:
                audit.ordering_violations.append({
                    "parent": None, "child": job_id, "reason": "missing child start time"
                })
            for parent_id in expected:
                parent = latest.get(parent_id)
                if parent is None:
                    continue
                parent_end = int(parent.get("ended_at") or 0)
                if not parent_end or parent_end > child_start:
                    audit.ordering_violations.append({
                        "parent": parent_id,
                        "child": job_id,
                        "parent_ended_at": parent_end,
                        "child_started_at": child_start,
                    })

        recorded_status = str(live.get("status"))
        recorded_completed = int(live.get("completed") or 0)
        if recorded_status in ("FAILED", "CANCELLED"):
            audit.errors.append(f"Benchmark result reports terminal status {recorded_status}")
        elif recorded_status != "COMPLETED" or recorded_completed != audit.expected_jobs:
            audit.warnings.append(
                f"Benchmark polling snapshot was {recorded_status} with {recorded_completed} completed; "
                "persisted final spans are used for final correctness"
            )
        if audit.unknown_job_spans:
            audit.errors.append(f"Found {audit.unknown_job_spans} unexpected span records with run timestamp {stamp}")
        if audit.missing_jobs:
            audit.errors.append(f"Missing {len(audit.missing_jobs)} expected jobs")
        if audit.parent_mismatches:
            audit.errors.append(f"Found {len(audit.parent_mismatches)} parent-set mismatches")
        if audit.ordering_violations:
            audit.errors.append(f"Found {len(audit.ordering_violations)} dependency ordering violations")
        if audit.non_completed_jobs:
            counts = Counter(item["status"] for item in audit.non_completed_jobs)
            audit.errors.append(f"Non-completed final spans: {dict(counts)}")
        if (audit.duplicate_span_records or audit.retry_jobs) and not allow_retries:
            audit.errors.append(
                f"Retries/duplicates are disallowed: {audit.duplicate_span_records} extra spans, "
                f"{audit.retry_jobs} jobs with attempt > 1"
            )
        elif audit.duplicate_span_records or audit.retry_jobs:
            audit.warnings.append(
                f"Retries allowed: {audit.duplicate_span_records} extra spans, {audit.retry_jobs} retried jobs"
            )
        if not timing_valid:
            audit.warnings.append("Timing excluded from performance analysis by caller")
        audit.passed = not audit.errors
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        audit.errors.append(str(exc))
    return audit


def _line_chart(points: list[dict], fields: list[tuple[str, str]], empty_text: str) -> str:
    if not points:
        return f'<div class="empty">{escape(empty_text)}</div>'
    width, height = 720, 210
    left, right, top, bottom = 52, 18, 24, 36
    times = [int(point["at"]) for point in points]
    start, end = min(times), max(times)
    duration = max(1, end - start)
    max_value = max(1, max(int(point.get(field) or 0) for point in points for field, _ in fields))
    plot_w, plot_h = width - left - right, height - top - bottom
    parts = [f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="Time series chart">']
    for index in range(3):
        value = max_value * (2 - index) / 2
        y = top + index * plot_h / 2
        parts.append(f'<line class="chart-grid" x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}"/>')
        parts.append(f'<text class="chart-axis-label" text-anchor="end" x="{left-8}" y="{y+5:.1f}">{value:.0f}</text>')
    for index in range(5):
        x = left + index * plot_w / 4
        elapsed = duration * index / 4000
        parts.append(f'<line class="chart-tick" x1="{x:.1f}" y1="{top+plot_h}" x2="{x:.1f}" y2="{top+plot_h+5}"/>')
        parts.append(f'<text class="chart-axis-label" text-anchor="middle" x="{x:.1f}" y="{height-9}">{elapsed:.0f}s</text>')
    for field, color in fields:
        coords = []
        for point in points:
            x = left + (int(point["at"]) - start) / duration * plot_w
            y = top + plot_h - int(point.get(field) or 0) / max_value * plot_h
            coords.append(f"{x:.1f},{y:.1f}")
        parts.append(f'<polyline points="{" ".join(coords)}" fill="none" stroke="{color}" stroke-width="3"/>')
    parts.append('</svg>')
    return "".join(parts)


def _summary_case_label(case: dict, index: int) -> tuple[str, str]:
    """Return a compact visible label and a complete tooltip for summary charts."""
    result = Path(case["result"])
    cell = result.parent.name
    suffix = re.search(r"(\d+)$", result.stem)
    pass_number = suffix.group(1) if suffix else result.stem
    full_label = f"{cell} #{pass_number}"
    dag = str(case.get("dag") or "").strip()
    if dag:
        label = re.sub(r"^WFC_", "", dag, flags=re.IGNORECASE)
        label = re.sub(r"_\d{9,}$", "", label)
        label = label.replace("_", " ")
    elif len(cell) <= 18:
        label = f"{cell} #{pass_number}"
    else:
        label = result.stem
    label = re.sub(r"\b(\d{4,})\b", lambda match: f"{int(match.group(1)):,}", label)
    if len(label) > 26:
        label = f"{label[:23].rstrip()}…"
    return label or f"Run {index + 1}", full_label


def _summary_bar_chart(cases: list[dict], title: str, value_getter, formatter) -> str:
    order = {"1x4": 0, "1x8": 1, "2x4": 2, "1x20": 3, "5x4": 4}
    colors = ["#45e0a8", "#68b7ff", "#c89cff", "#ffc857", "#ff7f8d"]
    def result_order(case):
        result = Path(case["result"])
        suffix = re.search(r"(\d+)$", result.stem)
        return order.get(result.parent.name, 99), int(suffix.group(1)) if suffix else 0

    ordered = sorted(cases, key=result_order)
    values = [float(value_getter(case) or 0) for case in ordered]
    width, height, left, top, bottom = 900, 285, 64, 50, 54
    plot_w, plot_h = width - left - 18, height - top - bottom
    maximum = max(values) * 1.18 or 1
    step = plot_w / max(1, len(values))
    bar_w = min(120, max(12, step * .64))
    parts = [f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">',
             f'<text class="summary-chart-title" x="{left}" y="23">{escape(title)}</text>',
             f'<line class="summary-axis" x1="{left}" y1="{top+plot_h}" x2="{left+plot_w}" y2="{top+plot_h}"/>']
    for index, (case, value) in enumerate(zip(ordered, values)):
        cell = Path(case["result"]).parent.name
        label, full_label = _summary_case_label(case, index)
        x = left + index * step + (step - bar_w) / 2
        bar_h = value / maximum * plot_h
        y = top + plot_h - bar_h
        color = colors[order.get(cell, 0)]
        if title.startswith("Rejected") and value:
            color = "#ff6b7d"
        parts.extend([
            f'<g><title>{escape(full_label)}</title><rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" rx="4" fill="{color}"/></g>',
            f'<text class="summary-value" text-anchor="middle" x="{x+bar_w/2:.1f}" y="{max(43,y-9):.1f}">{escape(formatter(value))}</text>',
            f'<text class="summary-label" text-anchor="middle" x="{x+bar_w/2:.1f}" y="{top+plot_h+25}">{escape(label)}</text>',
        ])
    return "".join(parts) + "</svg>"


def render_html(report: dict) -> str:
    workflow_correct_cases = sum(
        not case.get("missing_jobs")
        and not case.get("parent_mismatches")
        and not case.get("ordering_violations")
        and not case.get("non_completed_jobs")
        and case.get("completed_jobs") == case.get("expected_jobs")
        and case.get("observed_edges") == case.get("expected_edges")
        for case in report["cases"]
    )
    extra_dispatch_records = sum(case.get("duplicate_span_records", 0) for case in report["cases"])
    affected_dispatch_runs = sum(bool(case.get("duplicate_span_records", 0)) for case in report["cases"])
    executive_graphs = "".join([
        _summary_bar_chart(report["cases"], "Runtime by pass · seconds", lambda case: case["timing"].get("wall_seconds"), lambda value: f"{value:.1f}s"),
        _summary_bar_chart(report["cases"], "Throughput by pass · jobs/second", lambda case: case["timing"].get("jobs_per_second"), lambda value: f"{value:.1f}"),
        _summary_bar_chart(report["cases"], "Average occupied slots by pass", lambda case: case["capacity"].get("average_slots_used"), lambda value: f"{value:.1f}"),
        _summary_bar_chart(report["cases"], "Rejected dispatch records by pass", lambda case: case.get("duplicate_span_records"), lambda value: f"{int(value)}"),
    ])
    rows = []
    for case in report["cases"]:
        state = "PASS" if case["passed"] else "FAIL"
        errors = "<br>".join(escape(message) for message in case["errors"]) or "None"
        timing = "excluded" if not case["timing"]["valid"] else (
            f'{case["timing"].get("wall_seconds")} s / '
            f'{case["timing"].get("jobs_per_second")} jobs/s'
        )
        rows.append(
            "<tr>"
            f"<td><strong>{escape(case['dag'])}</strong><small>{escape(case['instance'])}</small></td>"
            f"<td>{case['observed_jobs']} / {case['expected_jobs']}</td>"
            f"<td>{case['observed_edges']} / {case['expected_edges']}</td>"
            f"<td>{case['ordering_violations'].__len__()}</td>"
            f"<td>{case['duplicate_span_records']} / {case['retry_jobs']}</td>"
            f"<td>{escape(timing)}</td><td>{errors}</td>"
            f'<td><span class="pill {"ok" if case["passed"] else "bad"}">{state}</span></td>'
            "</tr>"
        )
    details = []
    for case in report["cases"]:
        capacity = case["capacity"]
        fleet_note = ("Actual registered fleet samples from cluster telemetry."
                      if capacity["telemetry_available"] else
                      "No in-window fleet telemetry; worker count below is derived from execution spans.")
        worker_chips = "".join(
            f'<span class="worker">{escape(worker["id"])} · {worker["jobs"]} jobs · peak {worker["peak_slots_used"]} slots</span>'
            for worker in capacity["workers"]
        ) or '<span class="muted">No workers observed</span>'
        warnings_html = ("".join(f'<div class="warning">{escape(message)}</div>'
                                 for message in case.get("warnings") or []))
        slot_chart = _line_chart(capacity["slot_usage_series"], [("used", "#45e0a8")],
                                 "No valid span timestamps")
        fleet_chart = _line_chart(capacity["fleet_series"],
                                  [("workers", "#68b7ff"), ("capacity", "#ffc857"), ("used", "#45e0a8")],
                                  "Fleet scaling history unavailable")
        telemetry = case["telemetry"]
        shape = case["dag_shape"]
        queue = (f'<div class="telemetry-grid"><div><b>{telemetry.get("samples")}</b><small>Retained samples</small></div>'
                 f'<div><b>{telemetry.get("resolution_seconds")}s</b><small>Sample resolution</small></div>'
                 f'<div><b>{telemetry.get("max_ready_queue")}</b><small>Max ready queue</small></div>'
                 f'<div><b>{telemetry.get("max_dependency_wait")}</b><small>Max dependency wait</small></div>'
                 f'<div><b>{telemetry.get("max_oldest_ready_ms")} ms</b><small>Oldest ready job</small></div>'
                 f'<div><b>{telemetry.get("slot_utilization_percent")}%</b><small>Sampled slot utilization</small></div>'
                 f'<div><b>{telemetry.get("worker_cpu_peak_percent")}%</b><small>Peak worker CPU</small></div>'
                 f'<div><b>{telemetry.get("worker_memory_peak_percent")}%</b><small>Peak worker memory</small></div></div>'
                 if telemetry.get("available") else
                 '<div class="empty compact">No scheduler telemetry samples were retained inside this run window.</div>')
        dispatch = telemetry.get("dispatch_phases") or {}
        dispatch_rows = "".join(
            f'<tr><td>{name.removesuffix("_ms")}</td><td>{values.get("mean")}</td><td>{values.get("p50")}</td><td>{values.get("p95")}</td><td>{values.get("max")}</td></tr>'
            for name, values in dispatch.items()
        )
        dispatch_table = (f'<p>{telemetry.get("dispatch_samples", 0)} retained dispatch samples.</p><table class="mini"><thead><tr><th>Phase</th><th>Mean ms</th><th>P50 ms</th><th>P95 ms</th><th>Max ms</th></tr></thead><tbody>{dispatch_rows}</tbody></table>'
                          if telemetry.get("dispatch_samples", 0) else '<div class="empty compact">No dispatch phase samples retained for this run.</div>')
        stores = telemetry.get("store_operations") or {}
        store_rows = "".join(
            f'<tr><td>{name.removesuffix("_ms")}</td><td>{values.get("count")}</td><td>{values.get("mean")}</td><td>{values.get("p50")}</td><td>{values.get("p95")}</td><td>{values.get("max")}</td></tr>'
            for name, values in stores.items() if values.get("count")
        )
        store_table = (f'<table class="mini"><thead><tr><th>Operation</th><th>Count</th><th>Mean ms</th><th>P50 ms</th><th>P95 ms</th><th>Max ms</th></tr></thead><tbody>{store_rows}</tbody></table>'
                       if store_rows else '<div class="empty compact">No store latency samples retained for this run.</div>')
        latency = case["latency"]
        latency_table = (f'<table class="mini"><thead><tr><th>Measured interval</th><th>Mean</th><th>P50</th><th>P95</th><th>Max</th></tr></thead><tbody>'
                         f'<tr><td>Eligible → scheduler dispatch</td><td>{latency["ready_queue_ms"]["mean"]} ms</td><td>{latency["ready_queue_ms"]["p50"]} ms</td><td>{latency["ready_queue_ms"]["p95"]} ms</td><td>{latency["ready_queue_ms"]["max"]} ms</td></tr>'
                         f'<tr><td>Scheduler dispatch → callback recorded</td><td>{latency["scheduler_to_callback_ms"]["mean"]} ms</td><td>{latency["scheduler_to_callback_ms"]["p50"]} ms</td><td>{latency["scheduler_to_callback_ms"]["p95"]} ms</td><td>{latency["scheduler_to_callback_ms"]["max"]} ms</td></tr>'
                         '</tbody></table>')
        details.append(f"""
        <details class="run"><summary class="run-summary"><span><b>{escape(case['dag'])}</b>
        <small>{case['timing'].get('wall_seconds')}s · {capacity.get('observed_workers')} workers · peak {capacity.get('peak_slots_used')} slots · {case['completed_jobs']}/{case['expected_jobs']} jobs</small></span>
        <span class="pill {'ok' if case['passed'] else 'bad'}">{'PASS' if case['passed'] else 'FAIL'}</span></summary><div class="run-content">
        <p>{escape(case['instance'])}</p>
        {warnings_html}
        <div class="metrics"><div><b>{case['timing'].get('wall_seconds')}s</b><small>Total benchmark runtime</small></div>
        <div><b>{capacity.get('span_window_seconds')}s</b><small>Span execution window</small></div>
        <div><b>{capacity.get('observed_workers')}</b><small>Workers executing jobs</small></div>
        <div><b>{capacity.get('peak_slots_used')}</b><small>Peak slots used</small></div>
        <div><b>{capacity.get('average_slots_used')}</b><small>Average slots used</small></div>
        <div><b>{capacity.get('completed_jobs_per_second')}</b><small>Span throughput / second</small></div></div>
        <div class="workers">{worker_chips}</div>
        <div class="charts"><details class="mini-card"><summary><b>Slots used during this run</b><span>View graph</span></summary><div class="mini-content">{slot_chart}</div></details>
        <details class="mini-card"><summary><b>Fleet count, capacity, and used slots</b><span>View graph</span></summary><div class="mini-content"><p>{fleet_note}</p>{fleet_chart}</div></details></div></div></details>""")
        details[-1] = details[-1].rsplit('</div></details>', 1)[0] + f'''<details class="subsection"><summary><b>DAG shape</b><span>View</span></summary><div class="sub-content"><div class="telemetry-grid">
        <div><b>{shape['roots']}</b><small>Roots</small></div><div><b>{shape['leaves']}</b><small>Leaves</small></div>
        <div><b>{shape['max_fan_in']}</b><small>Maximum fan-in</small></div><div><b>{shape['max_fan_out']}</b><small>Maximum fan-out</small></div>
        <div><b>{shape['depth']}</b><small>Depth</small></div><div><b>{shape['maximum_layer_width']}</b><small>Widest layer</small></div></div></div></details>
        <details class="subsection"><summary><b>Job latency distribution</b><span>View table</span></summary><div class="sub-content">{latency_table}</div></details>
        <details class="subsection"><summary><b>Scheduler and host telemetry</b><span>View metrics</span></summary><div class="sub-content">{queue}</div></details>
        <div class="charts"><details class="subsection"><summary><b>Dispatch phase latency</b><span>View table</span></summary><div class="sub-content">{dispatch_table}</div></details>
        <details class="subsection"><summary><b>Store operation latency</b><span>View table</span></summary><div class="sub-content">{store_table}</div></details></div></div></details>'''
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Titan benchmark verification</title>
<style>
:root{{--bg:#070b0d;--panel:#10171a;--line:#29393e;--text:#e9f5f1;--muted:#8ea49f;--green:#45e0a8;--red:#ff6b7d}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.55 Segoe UI,sans-serif}}
main{{max-width:1180px;margin:auto;padding:32px 24px 60px}}h1{{font-size:30px;line-height:1.2;margin:5px 0}}h2{{font-size:20px;margin:20px 0 8px}}p,small{{color:var(--muted)}}.headline-cards{{display:grid;grid-template-columns:repeat(3,1fr);gap:9px;margin:16px 0}}.headline-cards>div{{background:var(--panel);border:1px solid var(--line);border-radius:9px;padding:12px}}.headline-cards b{{display:block;font-size:20px;color:var(--green)}}.headline-cards .attention{{color:#ffc857}}
.executive-charts{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}}.executive-charts svg{{height:240px;min-width:0;background:#0a1012;border:1px solid var(--line);border-radius:8px}}.summary-chart-title{{fill:var(--text);font-size:20px;font-weight:700}}.summary-axis{{stroke:#6d8580;stroke-width:1.5}}.summary-value{{fill:var(--text);font-size:20px;font-weight:800}}.summary-label{{fill:#b9cbc7;font-size:17px;font-weight:650}}.chart-grid{{stroke:#26373b;stroke-width:1}}.chart-tick{{stroke:#6d8580;stroke-width:1.5}}.chart-axis-label{{fill:#b9cbc7;font-size:13px;font-weight:600}}
.panel,.run{{margin-top:12px;background:var(--panel);border:1px solid var(--line);border-radius:10px;overflow:hidden}}details>summary{{cursor:pointer;list-style:none}}details>summary::-webkit-details-marker{{display:none}}.panel>summary,.run-summary{{display:flex;justify-content:space-between;align-items:center;gap:14px;padding:13px 15px}}.panel>summary:hover,.run-summary:hover,.mini-card>summary:hover,.subsection>summary:hover{{background:#152024}}.table-wrap{{overflow:auto;max-height:660px}}
table{{border-collapse:collapse;width:max-content;min-width:1280px}}th,td{{padding:14px 16px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top}}
th{{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted)}}small{{display:block;margin-top:5px;max-width:440px;overflow-wrap:anywhere}}
.pill{{padding:4px 9px;border-radius:999px;font-weight:800;font-size:11px}}.ok{{background:#123a2e;color:var(--green)}}.bad{{background:#421923;color:var(--red)}}
.run-summary>b,.run-summary span>b{{font-size:15px}}.run-summary>.pill{{flex:0 0 auto}}.run-content{{padding:0 15px 15px}}.run p{{margin:5px 0;color:var(--muted);overflow-wrap:anywhere}}
.metrics{{display:grid;grid-template-columns:repeat(3,1fr);gap:9px;margin:15px 0}}.metrics div{{background:#0a1012;border:1px solid var(--line);border-radius:8px;padding:11px}}.metrics b{{font-size:19px}}
.workers{{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}}.worker{{background:#10252d;color:#a9dcff;border:1px solid #24424f;border-radius:999px;padding:5px 10px;font-size:12px}}
.warning{{margin:10px 0;padding:9px 11px;background:#27220f;border-left:3px solid #ffc857;color:#ffe4a0;border-radius:5px}}
.charts{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-top:14px}}.mini-card,.subsection{{background:#0a1012;border:1px solid var(--line);border-radius:9px;min-width:0;overflow:hidden}}.mini-card>summary,.subsection>summary{{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:14px 16px}}.mini-card>summary span,.subsection>summary span{{color:var(--green);font-size:12px}}.mini-content,.sub-content{{padding:0 14px 14px;overflow:auto}}h3{{font-size:17px;margin:0 0 7px}}svg{{display:block;width:100%;height:220px;min-width:520px}}.empty{{height:140px;display:grid;place-items:center;color:var(--muted)}}.muted{{color:var(--muted)}}
.subsection{{margin-top:12px}}.charts>.subsection{{margin-top:0}}.telemetry-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}}.telemetry-grid>div{{background:#0d1518;border:1px solid #203036;border-radius:8px;padding:14px}}.telemetry-grid b{{font-size:20px}}.mini{{min-width:900px}}.compact{{height:100px}}
thead th{{position:sticky;top:0;background:#152024;z-index:3}}.panel th:first-child,.panel td:first-child{{position:sticky;left:0;background:#111b1e;z-index:2}}.panel thead th:first-child{{z-index:4;background:#182529}}
@media(max-width:850px){{main{{padding:28px 16px 60px}}.metrics,.telemetry-grid,.charts,.headline-cards,.executive-charts{{grid-template-columns:1fr 1fr}}}}@media(max-width:650px){{.metrics,.telemetry-grid,.charts,.headline-cards,.executive-charts{{grid-template-columns:1fr}}.run-summary{{align-items:flex-start}}svg{{height:200px}}}}
</style></head><body><main><p>Titan · persisted evidence verifier</p><h1>{'Verification passed' if report['passed'] else 'Correct DAG results with dispatch churn'}</h1>
<p>Generated {escape(report['generated_at'])}. Final workflow correctness and strict dispatch-history cleanliness are reported separately.</p>
<div class="headline-cards"><div><b>{workflow_correct_cases} / {report['total_cases']}</b><small>DAGs completed every job and dependency correctly</small></div><div><b class="attention">{report['passed_cases']} / {report['total_cases']}</b><small>Strict exactly-once history checks passed</small></div><div><b class="attention">{extra_dispatch_records}</b><small>Rejected dispatch records across {affected_dispatch_runs} affected {'run' if affected_dispatch_runs == 1 else 'runs'}</small></div></div>
<h2>Executive trends <small>{report['total_cases']} {'run' if report['total_cases'] == 1 else 'runs'} · raw values shown</small></h2><section class="executive-charts">{executive_graphs}</section>
<details class="panel"><summary><span><b>All verification cases</b><small>Compact comparison table for {report['total_cases']} runs</small></span><span>Open table</span></summary><div class="table-wrap"><table><thead><tr><th>DAG</th><th>Jobs</th><th>Edges</th><th>Ordering errors</th><th>Extra spans / retries</th><th>Timing</th><th>Errors</th><th>Result</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div></details>
{''.join(details)}
<details class="run"><summary class="run-summary"><span><b>Instrumentation coverage</b><small>What the engine captured and what is still missing</small></span><span>Open</span></summary><div class="run-content"><p>The report only calculates metrics supported by persisted evidence.</p>
<table class="mini"><thead><tr><th>Metric family</th><th>Status</th><th>Source or required change</th></tr></thead><tbody>
<tr><td>Dependencies, attempts, status, ready wait, scheduler-to-callback interval</td><td><span class="pill ok">AVAILABLE</span></td><td>Execution span journal</td></tr>
<tr><td>Queue composition, worker load/capacity, CPU, memory, heartbeat RTT</td><td><span class="pill ok">AVAILABLE</span></td><td>Scheduler telemetry samples; subject to bounded retention</td></tr>
<tr><td>Dispatch route/select/record/store/send and store read/write latency</td><td><span class="pill ok">AVAILABLE</span></td><td>Scheduler telemetry event buffers</td></tr>
<tr><td>RPC transit, worker queue, process startup, script execution, callback transit</td><td><span class="pill bad">NOT SEPARATED</span></td><td>Add worker-side timestamps and preserve them in completion callbacks</td></tr>
<tr><td>Master CPU/memory, network bytes, process/thread counts</td><td><span class="pill bad">NOT CAPTURED</span></td><td>Add master process and RPC transport instrumentation</td></tr>
<tr><td>Hardware, power state, Git/config identity</td><td><span class="pill bad">RUNNER DATA</span></td><td>Capture in the benchmark harness, not the engine</td></tr>
</tbody></table></div></details>
</main></body></html>"""


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", nargs=2, metavar=("INSTANCE", "RESULT"), required=True,
                        help="WfFormat instance and benchmark result JSON; repeat for a suite")
    parser.add_argument("--spans", nargs="+", type=Path, required=True,
                        help="One or more Titan day-partitioned span JSONL files")
    parser.add_argument("--telemetry", type=Path,
                        help="Optional saved /api/cluster JSON with fleet and slot samples")
    parser.add_argument("--manifest", type=Path,
                        help="Optional saved DAG manifest for legacy results without a submission plan")
    parser.add_argument("--allow-retries", action="store_true",
                        help="Report retry attempts as warnings instead of failures")
    parser.add_argument("--exclude-timing", action="append", default=[], metavar="DAG",
                        help="Mark a DAG's performance timing invalid while still checking correctness")
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--html-out", type=Path)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        spans = read_spans(args.spans)
        telemetry = (json.loads(args.telemetry.read_text(encoding="utf-8"))
                     if args.telemetry else None)
        manifest = (json.loads(args.manifest.read_text(encoding="utf-8"))
                    if args.manifest else None)
    except (OSError, ValueError) as exc:
        print(f"verification input error: {exc}", file=sys.stderr)
        return 2
    cases = []
    for instance, result in args.case:
        result_path = Path(result)
        dag = ""
        try:
            dag = str(json.loads(result_path.read_text(encoding="utf-8"))["live"]["dag"])
        except (OSError, KeyError, TypeError, json.JSONDecodeError):
            pass
        cases.append(verify_case(Path(instance), result_path, spans, args.allow_retries,
                                 dag not in set(args.exclude_timing), telemetry, manifest))
    report = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).astimezone().isoformat(),
        "passed": all(case.passed for case in cases),
        "total_cases": len(cases),
        "passed_cases": sum(case.passed for case in cases),
        "span_files": [str(path) for path in args.spans],
        "telemetry_file": str(args.telemetry) if args.telemetry else None,
        "manifest_file": str(args.manifest) if args.manifest else None,
        "cases": [asdict(case) for case in cases],
    }
    rendered = json.dumps(report, indent=2)
    print(rendered)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(rendered + "\n", encoding="utf-8")
    if args.html_out:
        args.html_out.parent.mkdir(parents=True, exist_ok=True)
        args.html_out.write_text(render_html(report), encoding="utf-8")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
