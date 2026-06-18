#  Copyright 2026 Ram Narayanan
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0

import socket
import struct
import json
import time
import base64
import os
import glob as _glob
import subprocess
import re
from flask import Flask, render_template, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates'))

# --- CONFIGURATION ---
SCHEDULER_HOST = "127.0.0.1"
SCHEDULER_PORT = 9090

# --- TITAN PROTOCOL CONSTANTS ---
CURRENT_VERSION = 1
OP_STATS_JSON      = 0x09
OP_GET_LOGS        = 0x16
OP_GET_JOB_STATUS  = 0x55
OP_CANCEL_JOB      = 0x56
OP_KV_SET          = 0x60
OP_KV_GET          = 0x61
OP_KV_SADD         = 0x62
OP_KV_SMEMBERS     = 0x63

# --- HITL KV key schema ---
HITL_QUEUE_KEY     = "titan:hitl:queue"
HITL_STATUS_PREFIX = "titan:hitl:status:"
HITL_MSG_PREFIX    = "titan:hitl:message:"
HITL_TS_PREFIX     = "titan:hitl:ts:"

# -------------------------------------------------------
# DAG Registry — tracks DAGs seen from OP_STATS_JSON
# -------------------------------------------------------
dag_registry = {}   # dag_key -> { name, jobs, submitted, job_meta }

# YAML-derived maps (populated once at first request)
_yaml_job_to_dag = {}   # "DAG-step-1-init"  -> "titan-comprehensive-test"
_yaml_job_deps   = {}   # "DAG-step-1-init"  -> ["DAG-...dep"]
_yaml_scanned    = False

def recv_all(sock, n):
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

def titan_communicate(op_code, payload_str="", retries=3):
    attempt = 0
    while attempt < retries:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((SCHEDULER_HOST, SCHEDULER_PORT))

            body_bytes  = payload_str.encode('utf-8')
            length      = len(body_bytes)
            header      = struct.pack('>BBBBI', CURRENT_VERSION, op_code, 0, 0, length)
            s.sendall(header + body_bytes)

            raw_header = recv_all(s, 8)
            if not raw_header:
                s.close()
                return None

            ver, resp_op, flags, spare, resp_len = struct.unpack('>BBBBI', raw_header)
            response_payload = ""
            if resp_len > 0:
                raw_body = recv_all(s, resp_len)
                if raw_body:
                    response_payload = raw_body.decode('utf-8')

            s.close()
            return response_payload

        except Exception as e:
            attempt += 1
            time.sleep(0.2)
            if attempt == retries:
                return None
    return None

def get_job_status(job_id):
    """Query Master for a single job status string."""
    raw = titan_communicate(OP_GET_JOB_STATUS, f"DAG-{job_id}")
    if not raw or raw.startswith("CONNECTION_ERROR"):
        raw = titan_communicate(OP_GET_JOB_STATUS, job_id)
    raw = (raw or "").strip().upper()
    if "COMPLETED" in raw: return "COMPLETED"
    if "RUNNING"   in raw: return "RUNNING"
    if "FAILED"    in raw: return "FAILED"
    if "DEAD"      in raw: return "FAILED"
    if "CANCELLED" in raw: return "CANCELLED"
    return "WAITING"

def kv_set(key, value):
    titan_communicate(OP_KV_SET, f"{key}|{value}")

def kv_get(key):
    return (titan_communicate(OP_KV_GET, key) or "").strip()

def kv_smembers(key):
    raw = titan_communicate(OP_KV_SMEMBERS, key) or ""
    return [m for m in raw.split(",") if m.strip()] if raw.strip() else []


_TERMINAL_STATUSES = {"COMPLETED", "FAILED", "CANCELLED", "DEAD"}

def _persist_terminal_statuses_to_manifest(registry):
    """Write terminal job statuses back to manifest so they survive cluster restarts."""
    manifest_path = ".titan_dag_manifest.json"
    try:
        existing = {}
        if os.path.exists(manifest_path):
            with open(manifest_path) as f:
                existing = json.load(f)

        dirty = False
        for dag_key, dag_meta in registry.items():
            for job_id, jmeta in dag_meta.get("job_meta", {}).items():
                status = jmeta.get("status", "")
                if status not in _TERMINAL_STATUSES:
                    continue
                entry = existing.get(job_id, {})
                if entry.get("final_status") != status:
                    existing.setdefault(job_id, {})["final_status"] = status
                    dirty = True

        if dirty:
            with open(manifest_path, "w") as f:
                json.dump(existing, f)
    except Exception:
        pass  # never crash the dashboard over a manifest write


def discover_dags_from_stats(stats):
    """
    Auto-discover DAGs from OP_STATS_JSON worker history.
    Uses YAML-derived job→DAG mapping so that all jobs from
    the same pipeline (e.g. step-1-init … step-4-server) are
    grouped under one DAG entry instead of appearing separately.
    """
    global dag_registry
    scan_yaml_dags()  # re-scan each time to pick up new SDK submissions
    if not stats or "workers" not in stats:
        return

    seen_jobs = {}  # job_id -> { worker, status, time }

    for w in stats.get("workers", []):
        worker_addr = f"127.0.0.1:{w.get('port','?')}"

        active = w.get("active_job")
        if active:
            seen_jobs[active] = {"worker": worker_addr, "status": "RUNNING", "time": ""}

        for svc in w.get("services", []):
            seen_jobs[svc] = {"worker": worker_addr, "status": "RUNNING", "time": ""}

        # Status severity: higher = more authoritative for display.
        # When the same job appears multiple times in history (e.g. old COMPLETED
        # from a previous run AND a new DEAD from the current rejection), prefer
        # the higher-severity entry so a rejection always surfaces as FAILED.
        _SEV = {"RUNNING": 5, "DEAD": 4, "FAILED": 4, "CANCELLED": 4, "COMPLETED": 3, "WAITING": 2}
        for h in w.get("history", []):
            jid = h.get("id", "")
            if not jid:
                continue
            new_status = h.get("status", "UNKNOWN")
            existing   = seen_jobs.get(jid)
            if existing is None or _SEV.get(new_status, 0) > _SEV.get(existing["status"], 0):
                seen_jobs[jid] = {
                    "worker":       worker_addr,
                    "status":       new_status,
                    "time":         h.get("time", ""),
                    "completed_at": h.get("completed_at", 0),
                }

    for job_id, meta in seen_jobs.items():
        if not job_id.startswith("DAG-"):
            continue

        # Primary: look up which DAG this job belongs to from YAML scan
        yaml_dag_name = _yaml_job_to_dag.get(job_id)
        if yaml_dag_name:
            dag_name = yaml_dag_name
        else:
            # Fallback: DAG-<dagname>_<stepname> naming convention
            parts = job_id[4:]
            dag_name = parts.split("_")[0] if "_" in parts else parts

        dag_key = f"DAG-{dag_name}"

        if dag_key not in dag_registry:
            dag_registry[dag_key] = {
                "name":      dag_name,
                "jobs":      [],
                "submitted": int(time.time() * 1000),
                "job_meta":  {}
            }

        if job_id not in dag_registry[dag_key]["jobs"]:
            dag_registry[dag_key]["jobs"].append(job_id)

        incoming_status = meta["status"]
        if incoming_status == "DEAD":
            incoming_status = "FAILED"

        # Filter stale COMPLETED entries from previous runs.
        # Two cases mark a COMPLETED entry as stale:
        #   1. completed_at is set and predates the current run (normal case)
        #   2. completed_at is 0 (worker didn't set it) AND the DAG was explicitly
        #      reset by scan_yaml_dags when a newer run_ts was detected — tracked
        #      via the "_reset_ts" field set at reset time.
        if incoming_status == "COMPLETED":
            run_ts       = dag_registry[dag_key].get("submitted", 0)
            reset_ts     = dag_registry[dag_key].get("_reset_ts", 0)
            completed_at = meta.get("completed_at", 0)
            if run_ts > 0 and completed_at > 0 and completed_at < run_ts:
                incoming_status = "WAITING"  # stale with timestamp
            elif reset_ts > 0 and completed_at == 0:
                incoming_status = "WAITING"  # stale after explicit reset

        existing_req = dag_registry[dag_key]["job_meta"].get(job_id, {}).get("requirement", "GENERAL")
        dag_registry[dag_key]["job_meta"][job_id] = {
            "worker":      meta["worker"],
            "status":      incoming_status,
            "time":        meta["time"],
            "requirement": existing_req,
        }
        # Once a job genuinely completes in the current run (has a real timestamp),
        # clear the reset guard so it doesn't keep getting wiped on future polls.
        if incoming_status == "COMPLETED" and meta.get("completed_at", 0) > 0:
            dag_registry[dag_key].pop("_reset_ts", None)

    # Persist terminal statuses to manifest so they survive cluster restarts.
    _persist_terminal_statuses_to_manifest(dag_registry)

    # Dependency-graph validation pass.
    # The worker history in Redis can contain COMPLETED entries from old runs
    # that have no completed_at timestamp, so the timestamp-based stale filter
    # above cannot catch them. As a second pass, walk the dependency graph: if
    # any parent of a job is not yet COMPLETED, that job cannot be COMPLETED in
    # the current run — reset it to WAITING.
    for dag_key, dag_meta in dag_registry.items():
        job_meta = dag_meta.get("job_meta", {})
        changed = True
        while changed:          # iterate until no more cascading changes
            changed = False
            for job_id, jmeta in job_meta.items():
                if jmeta["status"] in ("RUNNING", "FAILED"):
                    continue
                for dep_id in _yaml_job_deps.get(job_id, []):
                    dep_status = job_meta.get(dep_id, {}).get("status", "WAITING")
                    if dep_status != "COMPLETED":
                        if jmeta["status"] != "WAITING":
                            jmeta["status"] = "WAITING"
                            changed = True
                        break


# ================================================================
# HTML templates live in templates/ — loaded by render_template()
# ================================================================

# ================================================================
# YAML DEPENDENCY RESOLVER
# Finds the matching YAML file by DAG name and maps depends_on
# back to full registry job IDs — no changes to Java Master needed.
# ================================================================

def _parse_yaml_lite(path):
    """Extract name + jobs[{id, depends_on}] from a titan YAML file.
    Handles both block form (depends_on:\\n  - x) and inline form (depends_on: ["x","y"]).
    """
    name = None
    jobs = []
    cur  = None
    in_dep = False

    with open(path) as f:
        for raw in f:
            line = raw.rstrip()
            s      = line.lstrip()
            indent = len(line) - len(s)

            if indent == 0 and s.startswith('name:'):
                name = s.split(':', 1)[1].strip().strip('"\'')

            if indent == 2 and s.startswith('- id:'):
                if cur: jobs.append(cur)
                cur    = {'id': s.split(':', 1)[1].strip().strip('"\''), 'depends_on': []}
                in_dep = False
            elif cur and s.startswith('depends_on:'):
                rest = s[len('depends_on:'):].strip()
                if rest.startswith('['):
                    # Inline list: depends_on: ["a", "b"]
                    items = re.findall(r'[\"\']([^\"\']+)[\"\']', rest)
                    cur['depends_on'].extend(items)
                    in_dep = False
                else:
                    in_dep = True
            elif cur and in_dep and s.startswith('- '):
                cur['depends_on'].append(s[2:].strip().strip('"\''))
            elif s and not s.startswith('- ') and ':' in s:
                in_dep = False

    if cur: jobs.append(cur)
    return name, jobs


def scan_yaml_dags():
    """
    Populates _yaml_job_to_dag and _yaml_job_deps from two sources:
    1. YAML files (YAML-defined pipelines)
    2. .titan_dag_manifest.json (Python SDK-defined pipelines)
    """
    global _yaml_job_to_dag, _yaml_job_deps, _yaml_scanned

    # Source 1: YAML files
    yaml_files = (_glob.glob('**/*.yaml', recursive=True) +
                  _glob.glob('**/*.yml',  recursive=True))
    for yf in yaml_files:
        try:
            name, jobs = _parse_yaml_lite(yf)
            if not name or not jobs:
                continue
            for job in jobs:
                full_id   = f"DAG-{job['id']}"
                full_deps = [f"DAG-{d}" for d in job.get('depends_on', [])]
                _yaml_job_to_dag[full_id] = name
                _yaml_job_deps[full_id]   = full_deps
        except Exception:
            continue

    # Source 2: Python SDK manifest
    manifest_path = ".titan_dag_manifest.json"
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)

            # Find latest run_ts per DAG (skip metadata keys like __payload__)
            dag_run_ts = {}
            for full_id, info in manifest.items():
                if full_id.startswith("__") or "dag" not in info:
                    continue
                dag_name = info["dag"]
                ts = info.get("run_ts", 0)
                if ts > dag_run_ts.get(dag_name, 0):
                    dag_run_ts[dag_name] = ts

            for dag_name, latest_ts in dag_run_ts.items():
                dag_key = f"DAG-{dag_name}"
                old_ts  = dag_registry.get(dag_key, {}).get("submitted", 0)
                if old_ts > 0 and latest_ts > old_ts:
                    for jid in dag_registry[dag_key].get("job_meta", {}):
                        dag_registry[dag_key]["job_meta"][jid]["status"] = "WAITING"
                        dag_registry[dag_key]["job_meta"][jid]["worker"] = None
                    dag_registry[dag_key]["submitted"] = latest_ts
                    # Mark reset time so the stale filter can catch completed_at=0 entries
                    dag_registry[dag_key]["_reset_ts"] = latest_ts

            for full_id, info in manifest.items():
                if full_id.startswith("__") or "dag" not in info:
                    continue
                _yaml_job_to_dag[full_id] = info["dag"]
                _yaml_job_deps[full_id]   = info.get("deps", [])
                dag_name = info["dag"]
                dag_key  = f"DAG-{dag_name}"
                if dag_key not in dag_registry:
                    dag_registry[dag_key] = {
                        "name": dag_name, "jobs": [],
                        "submitted": info.get("run_ts", 0), "job_meta": {}
                    }
                if full_id not in dag_registry[dag_key]["jobs"]:
                    dag_registry[dag_key]["jobs"].append(full_id)
                if full_id not in dag_registry[dag_key]["job_meta"]:
                    dag_registry[dag_key]["job_meta"][full_id] = {
                        "worker": None, "status": "WAITING", "time": "",
                        "requirement": info.get("requirement", "GENERAL"),
                    }

            # Clean up stale fallback DAG entries.
            # When the dashboard starts without a manifest, discover_dags_from_stats
            # creates individual entries (e.g. DAG-INGEST, DAG-TRANSFORM) via the
            # fallback naming heuristic. Once the manifest is available and maps those
            # jobs to their real DAG (e.g. ETL_PIPELINE), remove the stale entries.
            for full_id, correct_dag_name in list(_yaml_job_to_dag.items()):
                correct_dag_key = f"DAG-{correct_dag_name}"
                for dag_key in list(dag_registry.keys()):
                    if dag_key == correct_dag_key:
                        continue
                    entry = dag_registry[dag_key]
                    if full_id in entry.get("jobs", []):
                        entry["jobs"].remove(full_id)
                        entry.get("job_meta", {}).pop(full_id, None)
                        if not entry["jobs"]:
                            del dag_registry[dag_key]

        except Exception:
            pass

    _yaml_scanned = True


# ================================================================
# SVG GRAPH BUILDER  (pure Python, no JS libraries needed)
# ================================================================

STATUS_DOT = {
    "COMPLETED": "#4caf6e",
    "RUNNING":   "#ffb74d",
    "WAITING":   "#9090b0",
    "FAILED":    "#ff5252",
    "CANCELLED": "#ffa000",
    "PENDING":   "#9090b0",
}

def build_dag_svg(jobs_data):
    """
    Build an SVG string for the DAG graph.
    jobs_data: list of dicts { id, status, requirement, depends_on, worker, is_service }
    """
    NODE_W, NODE_H = 170, 66
    COL_GAP, ROW_GAP = 88, 18

    by_id = {j["id"]: j for j in jobs_data}

    # Compute column levels via longest-path from roots
    levels = {}
    visited = set()
    def get_level(jid):
        if jid in levels:    return levels[jid]
        if jid in visited:   return 0
        visited.add(jid)
        job = by_id.get(jid)
        if not job or not job.get("depends_on"):
            levels[jid] = 0
            return 0
        levels[jid] = max(get_level(p) + 1 for p in job["depends_on"])
        return levels[jid]
    for j in jobs_data:
        get_level(j["id"])

    # Assign positions
    cols = {}
    for j in jobs_data:
        l = levels.get(j["id"], 0)
        cols.setdefault(l, []).append(j)

    positions = {}
    for col_idx in sorted(cols):
        col_jobs = cols[col_idx]
        x = col_idx * (NODE_W + COL_GAP) + 16
        for row_idx, j in enumerate(col_jobs):
            positions[j["id"]] = (x, 16 + row_idx * (NODE_H + ROW_GAP))

    max_col = max(cols.keys()) if cols else 0
    max_rows = max(len(v) for v in cols.values()) if cols else 1
    svg_w = (max_col + 1) * (NODE_W + COL_GAP) + 32
    svg_h = max_rows * (NODE_H + ROW_GAP) + 32

    lines = [
        f'<svg width="{svg_w}" height="{svg_h}" xmlns="http://www.w3.org/2000/svg" style="display:block;min-width:{svg_w}px;">',
        '<defs><marker id="arr" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">'
        '<path d="M0,0 L0,6 L8,3 z" fill="#444466"/></marker></defs>',
    ]

    # Edges
    for j in jobs_data:
        for pid in (j.get("depends_on") or []):
            if pid not in positions or j["id"] not in positions:
                continue
            px, py = positions[pid]
            cx, cy = positions[j["id"]]
            x1, y1 = px + NODE_W, py + NODE_H // 2
            x2, y2 = cx, cy + NODE_H // 2
            mx = (x1 + x2) // 2
            parent_status = by_id.get(pid, {}).get("status", "WAITING")
            color = STATUS_DOT.get(parent_status, "#666")
            dash = 'stroke-dasharray="4 3"' if parent_status == "WAITING" else ""
            lines.append(
                f'<path data-edge-from="{pid}" '
                f'd="M{x1},{y1} C{mx},{y1} {mx},{y2} {x2},{y2}" '
                f'fill="none" stroke="{color}" stroke-width="1.5" opacity="0.6" marker-end="url(#arr)" {dash}/>'
            )

    # Nodes — each links to /dags/<dag_id>?job=<job_id>
    for j in jobs_data:
        if j["id"] not in positions:
            continue
        x, y = positions[j["id"]]
        dot   = STATUS_DOT.get(j["status"], "#666")
        label = j["id"]
        if len(label) > 19:
            label = label[:18] + "…"
        req   = j.get("requirement", "GENERAL")
        extra_parts = []
        if req != "GENERAL":
            extra_parts.append(req)
        if j.get("is_service"):
            extra_parts.append("svc")
        extra = " · ".join(extra_parts) or "GENERAL"
        worker_short = (j.get("worker") or "unassigned").replace("127.0.0.1:", ":")

        # Fill colour based on status
        fill_map = {
            "COMPLETED": "#132213", "RUNNING": "#1e2010",
            "FAILED":    "#221313", "WAITING": "#141420",
            "CANCELLED": "#221a10", "PENDING": "#141420",
        }
        fill = fill_map.get(j["status"], "#141420")

        lines.append(
            f'<a href="?job={j["id"]}">'
            f'<g class="node-g">'
            f'<rect data-node-rect="{j["id"]}" x="{x}" y="{y}" width="{NODE_W}" height="{NODE_H}" rx="8" '
            f'fill="{fill}" stroke="{dot}" stroke-width="1.2"/>'
            f'<circle data-node-dot="{j["id"]}" cx="{x+13}" cy="{y+15}" r="5" fill="{dot}"/>'
            f'<text x="{x+26}" y="{y+19}" font-size="12" font-weight="500" fill="#e0e0e0" font-family="Segoe UI,sans-serif">{label}</text>'
            f'<text x="{x+10}" y="{y+36}" font-size="10" fill="#8888aa" font-family="Segoe UI,sans-serif">{extra}</text>'
            f'<text x="{x+10}" y="{y+52}" font-size="10" fill="#555577" font-family="Segoe UI,sans-serif">{worker_short}</text>'
            f'</g></a>'
        )

    lines.append("</svg>")
    return "\n".join(lines)


# ================================================================
# ROUTES — original ones preserved exactly
# ================================================================

@app.route('/')
def index():
    raw_json = titan_communicate(OP_STATS_JSON, "")
    stats = None
    if raw_json:
        try:
            json_start = raw_json.find('{')
            if json_start != -1:
                stats = json.loads(raw_json[json_start:])
        except json.JSONDecodeError:
            pass

    if not stats:
        stats = {"active_workers": 0, "queue_size": 0, "workers": []}
        status_color, status_text = "#f44336", "OFFLINE"
    else:
        status_color, status_text = "#00e676", "ONLINE"
        discover_dags_from_stats(stats)

    return render_template('dashboard.html', stats=stats,
                           status_color=status_color, status_text=status_text)


@app.route('/logs/<job_id>')
def view_logs(job_id):
    return render_template('log_viewer.html', job_id=job_id)


@app.route('/api/logs_raw/<job_id>')
def get_raw_logs(job_id):
    logs = titan_communicate(OP_GET_LOGS, job_id)
    if logs is None:
        return "Error fetching logs", 500
    return logs if logs else "[Titan] No logs found for this ID yet."


# ================================================================
# NEW ROUTES — DAG visualizer
# ================================================================

@app.route('/dags')
@app.route('/dags/<dag_id>')
def dag_dashboard(dag_id=None):
    # Refresh stats so dag_registry stays current
    raw_json = titan_communicate(OP_STATS_JSON, "")
    status_color, status_text = "#f44336", "OFFLINE"
    if raw_json:
        try:
            json_start = raw_json.find('{')
            if json_start != -1:
                stats = json.loads(raw_json[json_start:])
                discover_dags_from_stats(stats)
                status_color, status_text = "#00e676", "ONLINE"
        except Exception:
            pass

    # Build sidebar list
    dag_list = []
    for did, meta in dag_registry.items():
        jobs_meta = meta.get("job_meta", {})
        total = len(meta["jobs"])
        done  = sum(1 for jid in meta["jobs"] if jobs_meta.get(jid, {}).get("status") == "COMPLETED")
        dag_status = _resolve_dag_status_from_meta(meta)
        dag_list.append({
            "id":     did,
            "name":   meta["name"],
            "status": dag_status,
            "done":   done,
            "total":  total,
        })

    selected_dag  = None
    selected_job  = None

    if dag_id and dag_id in dag_registry:
        meta      = dag_registry[dag_id]
        jobs_meta = meta.get("job_meta", {})

        # deps are already populated by scan_yaml_dags()
        dep_map = _yaml_job_deps

        # Build jobs_data — status comes from worker history (job_meta),
        # which is reliably populated by discover_dags_from_stats.
        jobs_data = []
        for jid in meta["jobs"]:
            stored = jobs_meta.get(jid, {})
            status = stored.get("status", "WAITING")
            # Normalise terminal states for display
            if status in ("DEAD", "UNKNOWN", ""):
                status = "FAILED"
            # CANCELLED stays as CANCELLED (distinct grey colour)
            req = stored.get("requirement", "GENERAL")
            jobs_data.append({
                "id":          jid,
                "status":      status,
                "requirement": req,
                "depends_on":  dep_map.get(jid, []),
                "worker":      stored.get("worker"),
                "time":        stored.get("time", ""),
                "is_service":  "svc" in jid.lower() or "service" in jid.lower(),
            })

        total     = len(jobs_data)
        done      = sum(1 for j in jobs_data if j["status"] == "COMPLETED")
        running   = sum(1 for j in jobs_data if j["status"] == "RUNNING")
        failed    = sum(1 for j in jobs_data if j["status"] == "FAILED")
        waiting   = sum(1 for j in jobs_data if j["status"] == "WAITING")
        done_pct  = round((done   / total) * 100) if total else 0
        run_pct   = round((running / total) * 100) if total else 0
        fail_pct  = round((failed  / total) * 100) if total else 0

        dag_status    = _resolve_dag_status_from_jobs(jobs_data)
        requirements  = list({j["requirement"] for j in jobs_data})
        graph_svg     = build_dag_svg(jobs_data)

        # Check if this DAG has a stored payload (i.e. was submitted via Constructor)
        manifest_path = ".titan_dag_manifest.json"
        can_redeploy  = False
        try:
            if os.path.exists(manifest_path):
                with open(manifest_path) as _mf:
                    _man = json.load(_mf)
                can_redeploy = f"__payload__{meta['name']}" in _man
        except Exception:
            pass

        selected_dag = {
            "id":           dag_id,
            "name":         meta["name"],
            "status":       dag_status,
            "jobs":         jobs_data,
            "done":         done,
            "total":        total,
            "done_pct":     done_pct,
            "run_pct":      run_pct,
            "fail_pct":     fail_pct,
            "requirements": requirements,
            "graph_svg":    graph_svg,
            "can_redeploy": can_redeploy,
            "stat_pills": [
                ("Completed", done,    "#4caf6e"),
                ("Running",   running, "#ffb74d"),
                ("Failed",    failed,  "#ff5252"),
                ("Waiting",   waiting, "#9090b0"),
            ],
        }

        # Check if a job node was clicked (via ?job=)
        clicked_job_id = request.args.get("job")
        if clicked_job_id:
            job_info = next((j for j in jobs_data if j["id"] == clicked_job_id), None)
            if job_info:
                logs = titan_communicate(OP_GET_LOGS, clicked_job_id) or ""
                selected_job = {**job_info, "logs": logs or "No logs available yet."}

    return render_template(
        'dag_visualizer.html',
        dags        = dag_list,
        selected_dag = selected_dag,
        selected_job = selected_job,
        dag_count   = len(dag_registry),
        status_color = status_color,
        status_text  = status_text,
        dot_colors  = STATUS_DOT,
    )


@app.route('/dags/new')
def dag_constructor():
    return send_from_directory(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates'),
        'dag_constructor.html'
    )


@app.route('/api/dag/submit', methods=['POST'])
def api_dag_submit():
    """Receives a DAG payload from the constructor UI and forwards it to TitanMaster."""
    body = request.get_json(force=True)
    if not body or not body.get('jobs'):
        return jsonify({"error": "Missing jobs"}), 400

    dag_name = body.get('name', 'my-pipeline')
    jobs_raw = body['jobs']
    perm_dir = os.path.dirname(os.path.abspath(__file__))

    # ── HITL gate injection ────────────────────────────────────────
    # Build remap: source_job_id -> gate_job_id for all jobs with hitl_message
    gate_file  = os.path.join(perm_dir, 'hitl_gate.py')
    hitl_remap = {}
    for j in jobs_raw:
        if j.get('hitl_message'):
            if not os.path.exists(gate_file):
                return jsonify({"error": "hitl_gate.py not found in perm_files — required for HITL gates"}), 400
            hitl_remap[j['id']] = f"hitl-gate-{j['id']}"

    # Pre-load gate script b64 once and clear any stale KV decisions
    gate_b64 = None
    if hitl_remap:
        with open(gate_file, 'rb') as f:
            gate_b64 = base64.b64encode(f.read()).decode('utf-8')
        for gate_id in hitl_remap.values():
            titan_communicate(OP_KV_SET, f"titan:hitl:status:{gate_id}|CLEARED")

    # ── Build job strings ──────────────────────────────────────────
    job_strings = []
    for j in jobs_raw:
        job_id   = j.get('id', '')
        filename = j.get('filename', '')
        req      = (j.get('requirement') or 'GENERAL').replace('|', '')
        priority = j.get('priority', 1)
        delay    = j.get('delay', 0)
        affinity = j.get('affinity', False)
        args     = (j.get('args') or '').replace('|', ' ')
        parents  = j.get('depends_on') or []
        job_type = (j.get('job_type') or 'run').lower()

        # Re-wire parents: if a parent has a gate, point to the gate instead
        parents = [hitl_remap.get(p, p) for p in parents]

        parents_str     = '[' + ','.join(parents) + ']'
        affinity_suffix = '|AFFINITY' if affinity else ''
        simple_name     = os.path.basename(filename)

        # Read and base64-encode the script from perm_files
        file_path = os.path.join(perm_dir, simple_name)
        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found in perm_files: {simple_name}"}), 400
        with open(file_path, 'rb') as f:
            payload_b64 = base64.b64encode(f.read()).decode('utf-8')

        if job_type == 'service':
            port = j.get('port', 0)
            header = 'DEPLOY_PAYLOAD'
            payload_content = f"{simple_name}|{payload_b64}|{port}|{req}"
        else:
            header = 'RUN_PAYLOAD'
            payload_content = f"{simple_name}|{args}|{payload_b64}|{req}"

        line = f"{job_id}|{header}|{payload_content}|{priority}|{delay}|{parents_str}{affinity_suffix}"
        job_strings.append(line)

        # Inject HITL gate immediately after this job if it has hitl_message
        if j.get('hitl_message'):
            gate_id  = hitl_remap[job_id]
            safe_msg = j['hitl_message'].replace('|', ' ')
            max_wait = int(j.get('max_wait_seconds') or 172800)
            gate_args  = f"{gate_id} {max_wait} {safe_msg}"
            gate_line  = (f"{gate_id}|RUN_PAYLOAD|hitl_gate.py|{gate_args}|{gate_b64}"
                          f"|GENERAL|{priority}|0|[{job_id}]")
            job_strings.append(gate_line)

    dag_payload = ' ; '.join(job_strings)
    resp = titan_communicate(0x04, dag_payload)  # 0x04 = OP_SUBMIT_DAG

    if resp and 'ERROR' not in (resp or '').upper():
        # Build manifest list: original jobs (parents re-wired) + gate jobs
        manifest_jobs = []
        for j in jobs_raw:
            rewired = dict(j)
            rewired['depends_on'] = [hitl_remap.get(p, p) for p in (j.get('depends_on') or [])]
            manifest_jobs.append(rewired)
            if j.get('hitl_message'):
                manifest_jobs.append({
                    'id':         hitl_remap[j['id']],
                    'filename':   'hitl_gate.py',
                    'depends_on': [j['id']],
                    'requirement': 'GENERAL',
                })
        _write_constructor_manifest(dag_name, manifest_jobs, dag_payload)
        # Save canvas state so the DAG can be loaded back for editing
        _nodes = body.get('_nodes', [])
        _edges = body.get('_edges', [])
        if _nodes:
            _save_constructor_state(dag_name, _nodes, _edges)
        return jsonify({"status": "ok", "response": resp})
    return jsonify({"status": "error", "response": resp}), 502


def _write_constructor_manifest(dag_name, jobs_raw, dag_payload=""):
    """Mirrors what the Python SDK does — writes .titan_dag_manifest.json for dashboard grouping."""
    manifest_path = ".titan_dag_manifest.json"
    try:
        existing = {}
        if os.path.exists(manifest_path):
            with open(manifest_path) as f:
                existing = json.load(f)
        run_ts = int(time.time() * 1000)
        # Remove stale job entries from previous deploys of this DAG
        stale_keys = [k for k, v in existing.items()
                      if not k.startswith('__') and isinstance(v, dict) and v.get('dag') == dag_name]
        for k in stale_keys:
            del existing[k]
        for j in jobs_raw:
            full_id   = f"DAG-{j['id']}"
            full_deps = [f"DAG-{p}" for p in (j.get('depends_on') or [])]
            existing[full_id] = {
                "dag":      dag_name,
                "deps":     full_deps,
                "run_ts":   run_ts,
                "filename": os.path.basename(j.get('filename', '')),
                "requirement": j.get('requirement', 'GENERAL'),
            }
        # Store full payload for redeploy
        existing[f"__payload__{dag_name}"] = {"dag_payload": dag_payload, "run_ts": run_ts}
        # Store individual job payloads (parents stripped to []) for single-job replay
        if dag_payload:
            for job_str in dag_payload.split(" ; "):
                job_str = job_str.strip()
                if not job_str:
                    continue
                job_key = job_str.split("|")[0]
                import re as _re
                replay_str = _re.sub(r'\[[^\]]*\]', '[]', job_str)
                existing[f"__job_payload__DAG-{job_key}"] = replay_str
        with open(manifest_path, 'w') as f:
            json.dump(existing, f, indent=2)
    except Exception:
        pass


_CONSTRUCTOR_STATES_FILE = ".dag_constructor_states.json"


def _save_constructor_state(dag_name, nodes, edges):
    """Persists canvas nodes+edges so the DAG can be reloaded for editing."""
    try:
        existing = {}
        if os.path.exists(_CONSTRUCTOR_STATES_FILE):
            with open(_CONSTRUCTOR_STATES_FILE) as f:
                existing = json.load(f)
        existing[dag_name] = {
            "nodes":    nodes,
            "edges":    edges,
            "saved_at": int(time.time() * 1000),
        }
        with open(_CONSTRUCTOR_STATES_FILE, 'w') as f:
            json.dump(existing, f)
    except Exception:
        pass


@app.route('/api/dag/save_draft', methods=['POST'])
def api_save_draft():
    """Saves canvas state without submitting the DAG to the master."""
    body = request.get_json(force=True, silent=True) or {}
    dag_name = (body.get('name') or 'my-pipeline').strip()
    nodes = body.get('nodes', [])
    edges = body.get('edges', [])
    if not dag_name:
        return jsonify({"error": "name required"}), 400
    _save_constructor_state(dag_name, nodes, edges)
    return jsonify({"status": "ok", "name": dag_name})


@app.route('/api/dag/constructor_states')
def api_constructor_states():
    """Returns a list of DAGs that have saved constructor canvas states."""
    if not os.path.exists(_CONSTRUCTOR_STATES_FILE):
        return jsonify({"states": []})
    with open(_CONSTRUCTOR_STATES_FILE) as f:
        data = json.load(f)
    states = [{"name": k, "saved_at": v.get("saved_at", 0)} for k, v in data.items()]
    states.sort(key=lambda x: x["saved_at"], reverse=True)
    return jsonify({"states": states})


@app.route('/api/dag/constructor_state/<dag_name>')
def api_constructor_state(dag_name):
    """Returns the saved canvas state (nodes + edges) for a specific DAG."""
    if not os.path.exists(_CONSTRUCTOR_STATES_FILE):
        return jsonify({"error": "No saved states"}), 404
    with open(_CONSTRUCTOR_STATES_FILE) as f:
        data = json.load(f)
    if dag_name not in data:
        return jsonify({"error": "Not found"}), 404
    return jsonify(data[dag_name])


_WORKSPACE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'titan_workspace', 'shared'
)


@app.route('/api/workspace/files')
def api_workspace_files():
    """Returns workspace files sorted by recency, optionally filtered by job name fragments."""
    q       = request.args.get('q', '').lower()
    filters = [f.strip() for f in q.split(',') if f.strip()] if q else []

    if not os.path.exists(_WORKSPACE_DIR):
        return jsonify({"files": []})

    files = []
    for fname in os.listdir(_WORKSPACE_DIR):
        fpath = os.path.join(_WORKSPACE_DIR, fname)
        if not os.path.isfile(fpath) or fname.startswith('.'):
            continue
        if filters:
            # Also match on individual tokens (split on -) so "analyst-airflow"
            # matches files containing just "airflow" or "analyst"
            tokens = set(filters)
            for f in filters:
                tokens.update(t for t in f.split('-') if len(t) >= 3)
            if not any(t in fname.lower() for t in tokens):
                continue
        stat = os.stat(fpath)
        _, ext = os.path.splitext(fname)
        files.append({
            "name":     fname,
            "size":     stat.st_size,
            "modified": int(stat.st_mtime * 1000),
            "ext":      ext.lower(),
        })

    files.sort(key=lambda x: x["modified"], reverse=True)
    return jsonify({"files": files})


@app.route('/api/workspace/file/<path:filename>')
def api_workspace_file(filename):
    """Serves a single workspace file as a download."""
    safe = os.path.basename(filename)
    if not os.path.exists(os.path.join(_WORKSPACE_DIR, safe)):
        return jsonify({"error": "File not found"}), 404
    return send_from_directory(_WORKSPACE_DIR, safe, as_attachment=True)


@app.route('/api/perm_files')
def api_perm_files():
    """Returns a sorted list of .py files currently in the perm_files directory."""
    perm_dir = os.path.dirname(os.path.abspath(__file__))
    files = sorted(
        os.path.basename(p)
        for p in _glob.glob(os.path.join(perm_dir, '*.py'))
    )
    return jsonify({"files": files})


@app.route('/api/upload_script', methods=['POST'])
def api_upload_script():
    """Uploads a .py file into the perm_files directory."""
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    f = request.files['file']
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400
    name = secure_filename(f.filename)
    if not name.endswith('.py'):
        return jsonify({"error": "Only .py files are accepted"}), 400
    perm_dir = os.path.dirname(os.path.abspath(__file__))
    f.save(os.path.join(perm_dir, name))
    return jsonify({"status": "ok", "filename": name})


@app.route('/api/manifest/sync', methods=['POST'])
def api_manifest_sync():
    """Accepts a manifest payload from a remote SDK client and merges it into the local manifest."""
    manifest_path = ".titan_dag_manifest.json"
    try:
        incoming = request.get_json(force=True)
        if not incoming:
            return jsonify({"error": "Empty payload"}), 400
        existing = {}
        if os.path.exists(manifest_path):
            with open(manifest_path) as f:
                existing = json.load(f)
        existing.update(incoming)
        with open(manifest_path, 'w') as f:
            json.dump(existing, f, indent=2)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/dag/redeploy/<dag_id>', methods=['POST'])
def api_dag_redeploy(dag_id):
    """Re-submits a DAG by replaying the stored payload from the manifest."""
    manifest_path = ".titan_dag_manifest.json"
    if not os.path.exists(manifest_path):
        return jsonify({"error": "No manifest — submit this DAG at least once first"}), 404

    with open(manifest_path) as f:
        manifest = json.load(f)

    # Resolve the DAG name from dag_id (strip the DAG- prefix)
    dag_name = dag_id[4:] if dag_id.startswith("DAG-") else dag_id

    payload_key = f"__payload__{dag_name}"
    if payload_key not in manifest:
        return jsonify({"error": f"No stored payload for '{dag_name}'. Re-submit via the SDK or Constructor to enable redeploy."}), 404

    dag_payload = manifest[payload_key].get("dag_payload", "")
    if not dag_payload:
        return jsonify({"error": "Stored payload is empty"}), 400

    # Clear stale HITL decisions so every redeployed gate waits for fresh approval.
    # Gate job IDs follow the pattern hitl-gate-<source_job_id>.
    for job_str in dag_payload.split(" ; "):
        raw_id = job_str.strip().split("|")[0]          # e.g. "hitl-gate-preprocess"
        if raw_id.startswith("hitl-gate-"):
            titan_communicate(OP_KV_SET, f"{HITL_STATUS_PREFIX}{raw_id}|CLEARED")

    resp = titan_communicate(0x04, dag_payload)

    if resp and 'ERROR' not in (resp or '').upper():
        # Update run_ts on all jobs and the payload entry so stale detection resets
        run_ts = int(time.time() * 1000)
        for key, val in manifest.items():
            if isinstance(val, dict) and val.get("dag") == dag_name:
                val["run_ts"] = run_ts
        manifest[payload_key]["run_ts"] = run_ts
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        return jsonify({"status": "ok", "response": resp})

    return jsonify({"status": "error", "response": resp}), 502


@app.route('/api/dag/replay/<job_id>', methods=['POST'])
def api_dag_replay(job_id):
    """Re-submits a single job from a DAG, with its dependencies stripped so it runs immediately."""
    manifest_path = ".titan_dag_manifest.json"
    if not os.path.exists(manifest_path):
        return jsonify({"error": "No manifest found"}), 404

    with open(manifest_path) as f:
        manifest = json.load(f)

    payload_key = f"__job_payload__{job_id}"
    job_payload = manifest.get(payload_key, "")

    # Fallback: parse from the full DAG payload if individual entry missing (older runs)
    if not job_payload:
        job_info = manifest.get(job_id, {})
        dag_name = job_info.get("dag", "")
        full_payload = manifest.get(f"__payload__{dag_name}", {}).get("dag_payload", "")
        if full_payload:
            import re as _re
            for part in full_payload.split(" ; "):
                part = part.strip()
                if part.startswith(job_id.replace("DAG-", "") + "|") or part.split("|")[0] == job_id.replace("DAG-", ""):
                    job_payload = _re.sub(r'\[[^\]]*\]', '[]', part)
                    break

    if not job_payload:
        return jsonify({"error": f"No stored payload for job '{job_id}'. Re-submit the DAG to enable replay."}), 404

    resp = titan_communicate(0x04, job_payload)

    if resp and 'ERROR' not in (resp or '').upper():
        run_ts = int(time.time() * 1000)
        if job_id in manifest and isinstance(manifest[job_id], dict):
            manifest[job_id]["run_ts"] = run_ts
        manifest[payload_key] = job_payload
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        return jsonify({"status": "ok", "response": resp})

    return jsonify({"status": "error", "response": resp}), 502


@app.route('/api/dag_status')
def api_dag_status():
    """JSON endpoint polled every 3s by the dashboard JS."""
    # Refresh registry from master on every poll so statuses are live
    scan_yaml_dags()
    raw_json = titan_communicate(OP_STATS_JSON, "")
    if raw_json:
        try:
            json_start = raw_json.find('{')
            if json_start != -1:
                discover_dags_from_stats(json.loads(raw_json[json_start:]))
        except Exception:
            pass

    result = []
    for did, meta in dag_registry.items():
        jobs_meta = meta.get("job_meta", {})
        jobs_out  = []
        for jid in meta["jobs"]:
            jobs_out.append({
                "id":     jid,
                "status": jobs_meta.get(jid, {}).get("status", "WAITING"),
                "worker": jobs_meta.get(jid, {}).get("worker"),
            })
        result.append({
            "id":     did,
            "name":   meta["name"],
            "status": _resolve_dag_status_from_meta(meta),
            "jobs":   jobs_out,
        })
    return jsonify(result)


# ================================================================
# Helpers
# ================================================================

def _resolve_dag_status_from_jobs(jobs):
    statuses = [j["status"] for j in jobs]
    if "FAILED"    in statuses: return "FAILED"
    if "RUNNING"   in statuses: return "RUNNING"
    if "CANCELLED" in statuses: return "CANCELLED"
    if all(s == "COMPLETED" for s in statuses): return "COMPLETED"
    return "PENDING"

def _resolve_dag_status_from_meta(meta):
    jobs_meta = meta.get("job_meta", {})
    # Use persisted final_status when live status is unavailable (e.g. after restart)
    statuses = [
        v.get("status") or v.get("final_status", "WAITING")
        for v in jobs_meta.values()
    ]
    if not statuses:            return "PENDING"
    if "FAILED"    in statuses: return "FAILED"
    if "RUNNING"   in statuses: return "RUNNING"
    if "CANCELLED" in statuses: return "CANCELLED"
    if all(s == "COMPLETED" for s in statuses): return "COMPLETED"
    return "PENDING"


# ================================================================
# HITL — Human-in-the-Loop Endpoints
# ================================================================

@app.route('/api/hitl/pending')
def api_hitl_pending():
    """Returns all jobs currently waiting for human approval."""
    members = kv_smembers(HITL_QUEUE_KEY)
    pending = []
    for job_id in members:
        status = kv_get(HITL_STATUS_PREFIX + job_id)
        if status == "WAITING":
            message = kv_get(HITL_MSG_PREFIX + job_id)
            ts_raw  = kv_get(HITL_TS_PREFIX  + job_id)
            try:
                ts = int(ts_raw)
            except Exception:
                ts = 0
            pending.append({"job_id": job_id, "message": message or "Awaiting approval", "ts": ts})
    return jsonify(pending)


@app.route('/api/hitl/approve/<job_id>', methods=['POST'])
def api_hitl_approve(job_id):
    result = titan_communicate(OP_KV_SET, f"{HITL_STATUS_PREFIX}{job_id}|APPROVED")
    if result is None:
        return jsonify({"status": "error", "message": "TitanStore unreachable"}), 502
    return jsonify({"status": "ok", "job_id": job_id, "decision": "APPROVED"})


@app.route('/api/hitl/reject/<job_id>', methods=['POST'])
def api_hitl_reject(job_id):
    result = titan_communicate(OP_KV_SET, f"{HITL_STATUS_PREFIX}{job_id}|REJECTED")
    if result is None:
        return jsonify({"status": "error", "message": "TitanStore unreachable"}), 502
    return jsonify({"status": "ok", "job_id": job_id, "decision": "REJECTED"})


@app.route('/api/job/<job_id>/cancel', methods=['POST'])
def api_cancel_job(job_id):
    """Cancel a single running or queued job by its full ID (e.g. DAG-hitl-train)."""
    result = titan_communicate(OP_CANCEL_JOB, job_id)
    if result is None:
        return jsonify({"status": "error", "message": "Master unreachable"}), 502
    if result == "NOT_FOUND":
        return jsonify({"status": "error", "message": f"Job '{job_id}' not found or already finished"}), 404
    return jsonify({"status": "ok", "job_id": job_id, "result": result})


@app.route('/api/dag/<dag_id>/cancel', methods=['POST'])
def api_cancel_dag(dag_id):
    """Cancel all active (running/waiting/pending) jobs in a DAG."""
    if dag_id not in dag_registry:
        return jsonify({"status": "error", "message": f"DAG '{dag_id}' not found"}), 404

    jobs      = dag_registry[dag_id].get("jobs", [])
    jobs_meta = dag_registry[dag_id].get("job_meta", {})
    cancelled = []
    errors    = []

    for jid in jobs:
        stored_status = jobs_meta.get(jid, {}).get("status", "WAITING")
        if stored_status in ("COMPLETED", "FAILED", "CANCELLED", "DEAD"):
            continue  # already terminal — skip
        result = titan_communicate(OP_CANCEL_JOB, jid)
        if result in (None, "NOT_FOUND"):
            errors.append(jid)
        else:
            cancelled.append(jid)

    if not cancelled and errors:
        return jsonify({"status": "error", "message": "No jobs cancelled", "errors": errors}), 502

    return jsonify({"status": "ok", "dag_id": dag_id, "cancelled": cancelled, "skipped": errors})


# ================================================================
# AGENT RUNS VIEW  — groups DAGs that share an agent_run_id
# Passive: only appears when agent_run_id entries exist in manifest
# ================================================================



def _dag_run_status(dag_name):
    """Returns the overall status of a DAG by name from dag_registry."""
    dag_key = f"DAG-{dag_name}"
    meta = dag_registry.get(dag_key)
    if not meta:
        return "WAITING"
    return _resolve_dag_status_from_meta(meta)


def _short_stage_label(dag_name, run_id_prefix):
    """Strips the run_id prefix to give a short readable stage label."""
    label = dag_name
    if run_id_prefix and label.startswith(run_id_prefix):
        label = label[len(run_id_prefix):].lstrip("_")
    # Further shorten common suffixes: RESEARCH_cb5e13_PLAN → PLAN
    parts = label.split("_")
    return parts[-1] if parts else label


@app.route('/agents')
def agent_runs_view():
    # Refresh dag_registry
    raw_json = titan_communicate(OP_STATS_JSON, "")
    status_color, status_text = "#f44336", "OFFLINE"
    if raw_json:
        try:
            json_start = raw_json.find('{')
            if json_start != -1:
                stats = json.loads(raw_json[json_start:])
                discover_dags_from_stats(stats)
                status_color, status_text = "#00e676", "ONLINE"
        except Exception:
            pass

    scan_yaml_dags()

    agent_runs = []
    manifest_path = ".titan_dag_manifest.json"
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)

            import datetime
            for key, entry in manifest.items():
                if not key.startswith("__agent_run__"):
                    continue
                run_id   = entry.get("agent_run_id", key.replace("__agent_run__", ""))
                stages   = entry.get("stages", [])
                run_ts   = entry.get("run_ts", 0)

                # Find common prefix to strip for labels (e.g. "RESEARCH_cb5e13_")
                prefix = ""
                if stages:
                    # All stage names share a prefix up to the last underscore before the stage label
                    parts = stages[0].split("_")
                    if len(parts) >= 2:
                        prefix = "_".join(parts[:-1]) + "_"

                stage_list = []
                for dag_name in stages:
                    status = _dag_run_status(dag_name)
                    label  = _short_stage_label(dag_name, prefix)
                    stage_list.append({"dag_name": dag_name, "label": label, "status": status})

                ts_str = ""
                if run_ts:
                    try:
                        ts_str = datetime.datetime.fromtimestamp(run_ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        pass

                agent_runs.append({
                    "id":     run_id,
                    "stages": stage_list,
                    "ts":     ts_str,
                    "run_ts": run_ts,
                })

            # Most recent first
            agent_runs.sort(key=lambda r: r["run_ts"], reverse=True)
        except Exception:
            pass

    return render_template(
        'agent_runs.html',
        agent_runs   = agent_runs,
        status_color = status_color,
        status_text  = status_text,
    )


@app.route('/api/worker/launch', methods=['POST'])
def launch_worker():
    try:
        body = request.get_json(force=True)
        port       = int(body.get('port', 8082))
        capability = body.get('capability', 'GENERAL').upper()
        permanent  = bool(body.get('permanent', False))

        if capability not in ('GENERAL', 'GPU', 'HIGH_MEM', 'PYTHON'):
            return jsonify({'status': 'error', 'error': f'Unknown capability: {capability}'}), 400

        # Resolve JAR path relative to this file (perm_files/ → ../target/)
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        jar_path = os.path.join(base_dir, 'target', 'titan-orchestrator-1.0-SNAPSHOT.jar')
        if not os.path.exists(jar_path):
            return jsonify({'status': 'error', 'error': f'JAR not found at {jar_path}'}), 500

        cmd = ['java', '-cp', jar_path, 'titan.TitanWorker',
               str(port), '127.0.0.1', '9090', capability, str(permanent).lower()]
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return jsonify({'status': 'ok', 'pid': proc.pid, 'port': port, 'capability': capability})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/logo.png')
def serve_logo():
    from flask import send_from_directory
    logo_dir = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(logo_dir, 'Titan_logo.png')


if __name__ == '__main__':
    print("=" * 50)
    print("  Titan Dashboard")
    print("  http://127.0.0.1:5000          ← Orchestrator view")
    print("  http://127.0.0.1:5000/dags     ← DAG pipeline view")
    print("  http://127.0.0.1:5000/agents   ← Agent runs view")
    print("=" * 50)
    app.run(host='0.0.0.0', port=5000, debug=False)
