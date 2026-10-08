"""Cluster telemetry and bounded, predefined demonstration DAGs."""

import json
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Blueprint, jsonify, render_template, request
from dag_viewer.submission import GraphError, normalize, payload_for, submit
from .demos import PRESETS, build, recover_latest

OP_STATS_JSON = 0x09
MAX_HISTORY_DAYS = 14
MAX_SPANS = 5000
MAX_RUNS = 500
LIVE_HISTORY_THRESHOLD_MS = 15 * 60 * 1000


def _decode(raw, fallback):
    if not raw or raw.startswith(("ERROR", "UNKNOWN_OPCODE")):
        return fallback
    try:
        return json.loads(raw[raw.index("{" if isinstance(fallback, dict) else "["):])
    except (TypeError, ValueError):
        return fallback


def _history_days(since, now=None):
    """Return the bounded set of UTC journal dates intersecting the query."""
    today = (now or datetime.now(timezone.utc)).date()
    earliest = today - timedelta(days=MAX_HISTORY_DAYS - 1)
    if since > 0:
        requested = datetime.fromtimestamp(since / 1000, timezone.utc).date()
        earliest = max(earliest, requested)
    return [(earliest + timedelta(days=offset)).isoformat()
            for offset in range((today - earliest).days + 1)]


def _compact_runs(manifest_path, since):
    """Read only the bounded tail of the append-only dashboard run index."""
    path = Path(manifest_path).with_name(".titan_dag_run_history.jsonl")
    records = deque(maxlen=MAX_RUNS)
    try:
        with path.open(encoding="utf-8") as file:
            for line in file:
                try:
                    record = json.loads(line)
                    if (isinstance(record, dict) and isinstance(record.get("run_ts"), int)
                            and record["run_ts"] >= since):
                        records.append(record)
                except ValueError:
                    continue
    except OSError:
        pass
    return list(records)


_legacy_cache_lock = threading.Lock()
_legacy_cache = (None, ())


def _legacy_runs(path_text, modified_ns, size):
    """Parse an old monolithic manifest once per immutable file revision."""
    global _legacy_cache
    revision = (path_text, modified_ns, size)
    with _legacy_cache_lock:
        if _legacy_cache[0] == revision:
            return _legacy_cache[1]
        runs = []
        try:
            manifest = json.loads(Path(path_text).read_text(encoding="utf-8"))
            for key, value in manifest.items():
                if not key.startswith("__runs__") or not isinstance(value, list):
                    continue
                name = key.removeprefix("__runs__")
                for run in value:
                    if isinstance(run, dict) and isinstance(run.get("run_ts"), int):
                        jobs = run.get("jobs", {})
                        runs.append({"name": name, "run_ts": run["run_ts"],
                                     "jobs": list(jobs) if isinstance(jobs, dict) else list(jobs or [])})
        except (OSError, ValueError, TypeError):
            pass
        _legacy_cache = (revision, tuple(runs))
        return _legacy_cache[1]


def _cached_legacy_runs(manifest_path, since):
    path = Path(manifest_path)
    try:
        stat = path.stat()
    except OSError:
        return []
    return [run for run in _legacy_runs(str(path), stat.st_mtime_ns, stat.st_size)
            if run["run_ts"] >= since]


def create_blueprint(communicate, manifest_path):
    cluster = Blueprint("cluster_monitor", __name__)
    scripts = Path(__file__).resolve().parents[1] / "dag_samples"
    demo_lock = threading.Lock()
    latest_demo = recover_latest(manifest_path)

    def rpc(opcode, payload=""):
        return communicate(opcode, payload, retries=1)

    @cluster.get("/cluster")
    def page():
        return render_template("cluster_monitor.html", active_section="cluster",
                               nav_status="Cluster monitor", demo_presets=PRESETS)

    def demo_progress(demo):
        if not demo:
            return None
        ids = demo["job_ids"]
        statuses = _decode(rpc(OP_STATS_JSON, "status|" + ",".join(ids)), {})
        counts = {}
        for job_id in ids:
            status = statuses.get(job_id, {}).get("status", "UNKNOWN")
            counts[status] = counts.get(status, 0) + 1
        return {**demo, "counts": counts,
                "finished": sum(counts.get(s, 0) for s in
                                ("COMPLETED", "FAILED", "DEAD", "CANCELLED", "UNKNOWN")) == len(ids)}

    @cluster.get("/api/cluster/demo")
    def demo_status():
        return jsonify({"demo": demo_progress(latest_demo)})

    @cluster.post("/api/cluster/demo")
    def launch_demo():
        nonlocal latest_demo
        body = request.get_json(silent=True) or {}
        preset = body.get("preset")
        if preset not in PRESETS:
            return jsonify({"error": "Choose a listed demo preset."}), 400
        with demo_lock:
            current = demo_progress(latest_demo)
            if current and not current["finished"]:
                return jsonify({"error": "A demo is already running.", "demo": current}), 409
            metrics = _decode(rpc(OP_STATS_JSON, "metrics|0|1"), {})
            if not isinstance(metrics.get("samples"), list):
                return jsonify({"error": "Restart the master with the updated observability engine before running demos."}), 503
            try:
                name, graph = build(preset)
                name, jobs = normalize({"name": name, "jobs": graph}, scripts)
                submit(communicate, 0x04, manifest_path, name, jobs, payload_for(jobs))
            except (GraphError, OSError, ValueError) as error:
                return jsonify({"error": str(error)}), 400
            latest_demo = {"preset": preset, "label": PRESETS[preset][0],
                           "dag_id": f"DAG-{name}", "job_ids": [f"DAG-{job['id']}" for job in jobs]}
            return jsonify({"demo": demo_progress(latest_demo)}), 201

    @cluster.get("/api/cluster")
    def snapshot():
        try:
            since = max(0, int(request.args.get("since", "0")))
        except ValueError:
            since = 0
        mode = request.args.get("mode", "live")
        stats_raw = rpc(OP_STATS_JSON)
        stats = _decode(stats_raw, {})
        board = _decode(rpc(OP_STATS_JSON, "queue|500"), {"counts": {}, "jobs": [], "services": [], "dead_letter": []})
        metrics = _decode(rpc(OP_STATS_JSON, f"metrics|{since}|900"), {"samples": [], "events": []})
        spans = _decode(rpc(OP_STATS_JSON, f"timeline|{since}|5000"), [])
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        include_history = (mode == "analytics" or
                           (since > 0 and now_ms - since > LIVE_HISTORY_THRESHOLD_MS))
        history_days = []
        if include_history:
            seen = {(s.get("job_id"), s.get("started_at"), s.get("attempt")) for s in spans}
            history_days = _history_days(since)
            for day in history_days:
                historical = _decode(rpc(OP_STATS_JSON, f"history|{day}|{MAX_SPANS}"), [])
                for span in historical:
                    key = (span.get("job_id"), span.get("started_at"), span.get("attempt"))
                    if key not in seen and span.get("started_at", 0) >= since:
                        spans.append(span)
                        seen.add(key)
            spans.sort(key=lambda span: span.get("started_at", 0))
        runs = []
        if mode == "analytics":
            # Preserve old runs during migration, but cache the large legacy parse.
            runs = _cached_legacy_runs(manifest_path, since)
            runs.extend(_compact_runs(manifest_path, since))
            unique_runs = {}
            for run in runs:
                if isinstance(run.get("name"), str) and isinstance(run.get("run_ts"), int):
                    unique_runs[(run["name"], run["run_ts"])] = run
            runs = sorted(unique_runs.values(), key=lambda run: run["run_ts"])
        return jsonify({"online": bool(stats), "stats": stats, "board": board,
                        "metrics": metrics, "spans": spans[-MAX_SPANS:], "runs": runs[-MAX_RUNS:],
                        "history": {"included": include_history, "days": history_days,
                                    "span_limit": MAX_SPANS}})

    return cluster
