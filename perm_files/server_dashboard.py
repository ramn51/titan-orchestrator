#  Copyright 2026 Ram Narayanan
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0

import socket
import struct
import threading
import json
import time
import base64
import os
import glob as _glob
import subprocess
import re
from flask import Flask, render_template, request, jsonify, send_from_directory, Response
from werkzeug.utils import secure_filename

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates'))

# --- CONFIGURATION ---
# The dashboard is a client of the Master, not a part of it — it can run on a laptop while the
# Master runs elsewhere. These were hardcoded to loopback, which silently limited it to the
# Master's own host. Defaults are unchanged, so an existing single-host setup behaves identically.
SCHEDULER_HOST = os.environ.get("TITAN_MASTER_HOST", "127.0.0.1")
SCHEDULER_PORT = int(os.environ.get("TITAN_MASTER_PORT", "9090"))

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

        _register_job(dag_registry[dag_key], job_id)

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
                _register_job(dag_registry[dag_key], full_id)
                if full_id not in dag_registry[dag_key]["job_meta"]:
                    dag_registry[dag_key]["job_meta"][full_id] = {
                        "worker": None, "status": "WAITING", "time": "",
                        "requirement": info.get("requirement", "GENERAL"),
                        # Which run this job belongs to. A pipeline name is reused across runs and
                        # each run has its own job IDs, so without this the registry accumulates
                        # every run ever submitted and the graph draws them superimposed.
                        "run_ts": info.get("run_ts", 0),
                    }
                else:
                    dag_registry[dag_key]["job_meta"][full_id].setdefault(
                        "run_ts", info.get("run_ts", 0))

            # Clean up stale fallback DAG entries.
            # When the dashboard starts without a manifest, discover_dags_from_stats
            # creates individual entries (e.g. DAG-INGEST, DAG-TRANSFORM) via the
            # fallback naming heuristic. Once the manifest is available and maps those
            # jobs to their real DAG (e.g. ETL_PIPELINE), remove the stale entries.
            # Was: for every known job, walk every registered pipeline and linear-scan its job
            # list. At ~37,000 jobs and ~1,100 pipelines that is ~41 million iterations per call,
            # and this runs on every page load. It was the whole 13 seconds of /dags.
            #
            # Inverted: index once which pipelines actually contain each job, then visit only
            # those. Same outcome, proportional to the data instead of its square.
            owners = {}
            for dag_key, entry in dag_registry.items():
                for jid in entry.get("jobs", ()):
                    owners.setdefault(jid, set()).add(dag_key)

            for full_id, correct_dag_name in _yaml_job_to_dag.items():
                correct_dag_key = f"DAG-{correct_dag_name}"
                for dag_key in list(owners.get(full_id, ())):
                    if dag_key == correct_dag_key:
                        continue
                    entry = dag_registry.get(dag_key)
                    if entry is None:
                        continue
                    _unregister_job(entry, full_id)
                    if not entry.get("jobs"):
                        dag_registry.pop(dag_key, None)

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

# The Master keeps this many spans in memory (titan.metrics.spans.max). Asking for more than
# this cannot return more, and asking for less silently drops older pipelines from a filtered view.
MASTER_SPAN_WINDOW = int(os.environ.get("TITAN_MASTER_SPAN_WINDOW", "5000"))

# Sidebar page size. Status resolution is the expensive part of this page and it scales with the
# number of pipelines shown, so this is the knob that bounds it.
DAGS_PER_PAGE = int(os.environ.get("TITAN_DAGS_PER_PAGE", "25"))

# Above this, a graph stops being readable and starts being a rendering cost: every node is laid
# out and serialised into one SVG string server-side.
MAX_RENDER_NODES = int(os.environ.get("TITAN_DAG_MAX_NODES", "300"))


def _register_job(meta, job_id):
    """Add a job to a registry entry once, in O(1).

    `meta["jobs"]` is a list because the views depend on insertion order, but membership was being
    tested against that list once per manifest entry. With pipelines up to 1,000 jobs and 75,000
    manifest entries that is quadratic, and it was costing about 13 seconds on every page load of
    /dags. The set is an index over the same data, not a second source of truth.
    """
    seen = meta.get("_job_set")
    if seen is None:
        seen = meta["_job_set"] = set(meta.get("jobs", ()))
    if job_id in seen:
        return False
    meta.setdefault("jobs", []).append(job_id)
    seen.add(job_id)
    return True


def _run_stamps(meta):
    """Distinct run timestamps recorded for a pipeline, newest first."""
    jm = meta.get("job_meta", {})
    return sorted({(jm.get(j, {}) or {}).get("run_ts", 0) for j in meta.get("jobs", ())} - {0},
                  reverse=True)


def _jobs_of_run(meta, run_ts):
    """The jobs belonging to one run of a pipeline.

    A pipeline name is reused across runs and each run brings its own job IDs, so the registry
    holds every run ever submitted under that name. The sidebar count and the graph must agree on
    which run they are describing, so both go through here.
    """
    jobs = meta.get("jobs", [])
    stamps = _run_stamps(meta)
    if not stamps or run_ts is None:
        return list(jobs)
    jm = meta.get("job_meta", {})
    newest = (run_ts == stamps[0])
    return [j for j in jobs
            if (jm.get(j, {}) or {}).get("run_ts", 0) == run_ts
            or (newest and not (jm.get(j, {}) or {}).get("run_ts", 0))]


def _unregister_job(meta, job_id):
    """Remove a job from a registry entry, keeping the membership index in step."""
    seen = meta.get("_job_set")
    if seen is None:
        seen = meta["_job_set"] = set(meta.get("jobs", ()))
    if job_id not in seen:
        return False
    try:
        meta["jobs"].remove(job_id)
    except ValueError:
        pass
    seen.discard(job_id)
    meta.get("job_meta", {}).pop(job_id, None)
    return True


def build_dag_svg(jobs_data, max_nodes=None, info=None):
    """
    Build an SVG string for the DAG graph.
    jobs_data: list of dicts { id, status, requirement, depends_on, worker, is_service }

    max_nodes: cap on nodes drawn. Defaults to MAX_RENDER_NODES; pass 0 to draw everything,
               which is what the downloadable full-graph route does.
    info:      optional dict, filled in with {total, rendered, truncated} so the caller can tell
               the user what was left out. The in-canvas note alone is not enough: it scrolls out
               of view on a wide graph, and a truncated graph that looks complete is a lie.

    Very large graphs are capped rather than drawn in full. A thousand 170px nodes is neither
    readable nor cheap: every node is laid out and serialised into a single string here, and the
    browser then has to parse all of it. The cap keeps whole dependency levels so the shape of
    what is shown stays honest.
    """
    NODE_W, NODE_H = 170, 66
    COL_GAP, ROW_GAP = 88, 18

    cap = MAX_RENDER_NODES if max_nodes is None else max_nodes
    total_nodes = len(jobs_data)
    truncated_from = 0
    if cap and len(jobs_data) > cap:
        truncated_from = len(jobs_data)
        keep, kept_ids = [], set()
        # Walk levels from the roots so the kept subgraph stays connected and readable.
        remaining = list(jobs_data)
        while remaining and len(keep) < cap:
            layer = [j for j in remaining
                     if all(p in kept_ids for p in (j.get("depends_on") or []))]
            if not layer:                      # dependencies point outside this set; take the rest
                layer = remaining
            for j in layer:
                if len(keep) >= cap:
                    break
                keep.append(j)
                kept_ids.add(j["id"])
            remaining = [j for j in remaining if j["id"] not in kept_ids]
        for j in keep:                          # drop edges to nodes that did not survive
            j = j
        jobs_data = [dict(j, depends_on=[p for p in (j.get("depends_on") or []) if p in kept_ids])
                     for j in keep]

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

    if info is not None:
        info.update({"total": total_nodes, "rendered": len(jobs_data),
                     "truncated": bool(truncated_from)})
    if truncated_from:
        # Say what was dropped on the canvas too, for anyone who saves or shares the SVG alone.
        lines.append(
            f'<text x="12" y="20" fill="#ffb74d" font-size="13" font-family="monospace">'
            f'Showing {len(jobs_data)} of {truncated_from} nodes (dependency order). '
            f'Download the full graph for all {truncated_from}.</text>')
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


def _load_manifest():
    try:
        with open('.titan_dag_manifest.json') as mf:
            return json.load(mf)
    except (OSError, ValueError):
        return {}


def _jobs_of_dag(dag):
    """The job IDs belonging to a pipeline name.

    The Master only substring-matches on job ID, and a DAG's name is usually nothing like its
    job IDs, so the name has to be resolved here, where the manifest lives.
    """
    return {k for k, v in _load_manifest().items()
            if isinstance(v, dict) and v.get('dag') == dag}


# =======================================================================
# Demo runner — preset workflows for showing the dashboard to someone live
# =======================================================================
# This is the one endpoint that MUTATES the cluster, and the dashboard has no authentication,
# so the blast radius is deliberately fenced in:
#   * only the presets below can run — the request names a preset, it never carries a script,
#     a job spec, or a command, so there is nothing to inject
#   * the scripts are short sleeps and deliberate failures written by this file
#   * one run at a time, so it cannot be used to pile on load
#   * set TITAN_DASHBOARD_DEMO=0 to remove the endpoint's ability to run at all
DEMO_ENABLED = os.environ.get('TITAN_DASHBOARD_DEMO', '1') not in ('0', 'false', 'False')
DEMO_DIR = '/tmp/titan_demo_presets'

# Each preset is (pipeline name, builder). A builder returns a list of (job_id, script, parents,
# priority, requirement) tuples — plain data, so nothing from the request reaches a job spec.
DEMO_PRESETS = {
    'fanout': {
        'label': 'Fan-out / fan-in',
        'shows': 'parallelism, the blocked lane, a real critical path',
        'seconds': 25,
    },
    'saturate': {
        'label': 'Saturation burst',
        'shows': 'queue wait, wave-shaped starvation, scaling pressure',
        'seconds': 45,
    },
    'failures': {
        'label': 'Failures and retries',
        'shows': 'retry spans, the dead-letter queue, red failure reasons',
        'seconds': 60,
    },
    'deadend': {
        'label': 'Capability dead end',
        'shows': 'the parked lane, pending reasons, a red capability gap',
        'seconds': 10,
    },
    'priority': {
        'label': 'Priority test',
        'shows': 'whether a high-priority job really jumps a saturated queue',
        'seconds': 30,
    },
    'deep': {
        'label': 'Deep chain',
        'shows': 'genuine graph depth and dependency-release latency',
        'seconds': 30,
    },
    'service': {
        'label': 'Deploy a service',
        'shows': 'the services panel, readiness gating, address discovery',
        'seconds': 30,
    },
}

_demo_state = {'running': False, 'preset': None, 'started': 0, 'jobs': [],
               'pipeline': None, 'error': None, 'finished': 0}


def _demo_script(client, name, body):
    if not os.path.isdir(DEMO_DIR):
        os.makedirs(DEMO_DIR, exist_ok=True)
    path = os.path.join(DEMO_DIR, name + '.py')
    with open(path, 'w') as f:
        f.write(body)
    client.deploy_script(path)
    return path


def _demo_jobs(client, preset, tag):
    """Build the job list for a preset. Returns (pipeline_name, [TitanJob])."""
    from titan_sdk import TitanJob

    quick = _demo_script(client, 'demo_quick', "import time\ntime.sleep(0.4)\nprint('ok')\n")
    med = _demo_script(client, 'demo_med', "import time\ntime.sleep(1.5)\nprint('ok')\n")
    heavy = _demo_script(client, 'demo_heavy', "import time\ntime.sleep(3.0)\nprint('ok')\n")

    if preset == 'fanout':
        jobs = [TitanJob(job_id='demo%s-seed' % tag, filename=quick)]
        leaves = []
        for i in range(8):
            jid = 'demo%s-map%d' % (tag, i)
            leaves.append(jid)
            jobs.append(TitanJob(job_id=jid, filename=med, parents=['demo%s-seed' % tag]))
        jobs.append(TitanJob(job_id='demo%s-reduce' % tag, filename=heavy, parents=leaves))
        return 'demo-fanout', jobs

    if preset == 'saturate':
        return 'demo-saturation', [
            TitanJob(job_id='demo%s-burst%d' % (tag, i), filename=heavy) for i in range(24)]

    if preset == 'failures':
        conn = _demo_script(client, 'demo_err_conn',
                            "raise ConnectionError('postgres refused connection on 5432')\n")
        code = _demo_script(client, 'demo_err_code', "import sys\nsys.exit(137)\n")
        imp = _demo_script(client, 'demo_err_import', "import a_module_that_is_not_installed\n")
        return 'demo-failures', [
            TitanJob(job_id='demo%s-ok1' % tag, filename=quick),
            TitanJob(job_id='demo%s-ok2' % tag, filename=quick),
            TitanJob(job_id='demo%s-db' % tag, filename=conn),
            TitanJob(job_id='demo%s-oom' % tag, filename=code),
            TitanJob(job_id='demo%s-import' % tag, filename=imp),
        ]

    if preset == 'deadend':
        return 'demo-dead-end', [
            TitanJob(job_id='demo%s-needs-tpu' % tag, filename=quick, requirement='TPU')]

    if preset == 'priority':
        jobs = [TitanJob(job_id='demo%s-bulk%d' % (tag, i), filename=heavy, priority=1)
                for i in range(16)]
        # Submitted last but at the top priority: it should overtake the bulk jobs still queued.
        jobs.append(TitanJob(job_id='demo%s-urgent' % tag, filename=quick, priority=9))
        return 'demo-priority', jobs

    if preset == 'service':
        # A real HTTP server, so readiness probing has something to actually connect to. The port
        # is derived from the tag, not from the request, so it cannot be chosen by a caller.
        port = 9700 + (int(tag) % 80)
        folder = os.path.join(DEMO_DIR, 'demo_svc_%s' % tag)
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, 'server.py'), 'w') as f:
            f.write("from http.server import BaseHTTPRequestHandler, HTTPServer\n"
                    "class H(BaseHTTPRequestHandler):\n"
                    "    def do_GET(s):\n"
                    "        s.send_response(200); s.end_headers(); s.wfile.write(b'titan demo service')\n"
                    "    def log_message(s, *a): pass\n"
                    "httpd = HTTPServer(('0.0.0.0', %d), H)\n"
                    "print('bound', flush=True)\n"
                    "httpd.serve_forever()\n" % port)
        client.upload_project_folder(folder)
        zip_name = os.path.basename(folder) + '.zip'
        return 'demo-service', [TitanJob(job_id='demo%s-svc' % tag,
                                         filename='%s/server.py' % zip_name,
                                         job_type='SERVICE', port=port, is_archive=True)]

    if preset == 'deep':
        jobs, prev = [], None
        for lvl in range(8):
            jid = 'demo%s-lvl%d' % (tag, lvl)
            jobs.append(TitanJob(job_id=jid, filename=med, parents=[prev] if prev else []))
            prev = jid
        return 'demo-deep-chain', jobs

    return None, []


def _demo_worker(preset, tag):
    """Submit the preset off the request thread so the HTTP call returns straight away."""
    try:
        from titan_sdk import TitanClient
        client = TitanClient()
        pipeline, jobs = _demo_jobs(client, preset, tag)
        if not jobs:
            _demo_state['error'] = 'unknown preset'
            return
        _demo_state['pipeline'] = pipeline
        _demo_state['jobs'] = ['DAG-' + j.id for j in jobs]
        client.submit_dag(pipeline, jobs)
    except Exception as exc:                       # a demo must never take the dashboard down
        _demo_state['error'] = '{}: {}'.format(type(exc).__name__, exc)
    finally:
        _demo_state['running'] = False
        _demo_state['finished'] = int(time.time() * 1000)


@app.route('/api/demo/presets')
def api_demo_presets():
    """What the Demo runner can run, for the picker."""
    return jsonify({
        "enabled": DEMO_ENABLED,
        "presets": [dict(id=k, **v) for k, v in DEMO_PRESETS.items()],
        "state": {k: _demo_state[k] for k in ('running', 'preset', 'started', 'pipeline', 'error')},
    })


@app.route('/api/demo/run', methods=['POST'])
def api_demo_run():
    """Submit one preset workflow. The body names a preset and nothing else."""
    if not DEMO_ENABLED:
        return jsonify({"error": "Demo runner is disabled (TITAN_DASHBOARD_DEMO=0)"}), 403
    preset = (request.get_json(silent=True) or {}).get('preset') or request.args.get('preset') or ''
    preset = preset.strip()
    if preset not in DEMO_PRESETS:
        return jsonify({"error": "Unknown preset"}), 400
    if _demo_state['running']:
        return jsonify({"error": "A demo is already being submitted — wait for it to finish"}), 409

    _demo_state.update(running=True, preset=preset, started=int(time.time() * 1000),
                       jobs=[], pipeline=None, error=None)
    tag = str(int(time.time()) % 100000)
    threading.Thread(target=_demo_worker, args=(preset, tag), daemon=True).start()
    return jsonify({"ok": True, "preset": preset, "label": DEMO_PRESETS[preset]['label'],
                    "expect_seconds": DEMO_PRESETS[preset]['seconds']})


@app.route('/api/demo/status')
def api_demo_status():
    """Live progress of the last demo submitted, by asking the store for each job's status."""
    jobs = list(_demo_state['jobs'])
    counts = {"COMPLETED": 0, "RUNNING": 0, "PENDING": 0, "FAILED": 0, "DEAD": 0, "UNKNOWN": 0}
    if jobs:
        # The bulk payload is a flat {id: status} map; fetch_bulk_status already chunks it to stay
        # under the frame cap, so reuse it rather than re-implementing the call.
        statuses = fetch_bulk_status(jobs)
        for j in jobs:
            st = (statuses.get(j) or 'UNKNOWN').upper()
            counts[st if st in counts else 'UNKNOWN'] += 1
    return jsonify({
        "running": _demo_state['running'], "preset": _demo_state['preset'],
        "pipeline": _demo_state['pipeline'], "error": _demo_state['error'],
        "started": _demo_state['started'], "total": len(jobs), "counts": counts,
        "done": counts['COMPLETED'] + counts['FAILED'] + counts['DEAD'],
    })


@app.route('/api/dag_names')
def api_dag_names():
    """DAG names known to the manifest, newest first, for the timeline picker."""
    manifest = _load_manifest()
    if not manifest:
        return jsonify({"dags": []})
    seen = {}
    for v in manifest.values():
        if not (isinstance(v, dict) and v.get('dag')):
            continue
        name, ts = v['dag'], v.get('run_ts', 0)
        e = seen.setdefault(name, {"name": name, "run_ts": 0, "jobs": 0, "stamps": set()})
        e['jobs'] += 1
        e['run_ts'] = max(e['run_ts'], ts)
        if ts:
            e['stamps'].add(ts)
    out = []
    for e in seen.values():
        # Jobs submitted together share a run_ts, so distinct stamps ≈ distinct runs. This is the
        # count a reader expects; the job total spans every run the manifest has ever seen.
        out.append({"name": e['name'], "run_ts": e['run_ts'],
                    "jobs": e['jobs'], "runs": max(len(e['stamps']), 1)})
    out.sort(key=lambda d: -d['run_ts'])
    out = out[:80]

    # The manifest is a submission log that lives forever on this side; spans live in the
    # Master's ring (cleared on restart) and on disk (retained a few days). So a pipeline can be
    # in the picker with nothing left to draw. Say which, instead of rendering a blank chart.
    job_to_dag = {k: v['dag'] for k, v in manifest.items()
                  if isinstance(v, dict) and v.get('dag')}
    mem_counts, disk_counts = {}, {}
    now = int(time.time() * 1000)
    for target, payload in ((mem_counts, "timeline::5000"),
                            (disk_counts, "history:{}:{}::20000".format(
                                now - 7 * 24 * 3600 * 1000, now))):
        raw = titan_communicate(OP_STATS_JSON, payload)
        if not raw:
            continue
        try:
            start = raw.find('{')
            spans = json.loads(raw[start:]).get('spans', []) if start != -1 else []
        except json.JSONDecodeError:
            continue
        for sp in spans:
            name = job_to_dag.get(sp.get('id'))
            if name:
                target[name] = target.get(name, 0) + 1
    for e in out:
        e['spans_memory'] = mem_counts.get(e['name'], 0)
        e['spans_disk'] = disk_counts.get(e['name'], 0)
        e['spans'] = max(e['spans_memory'], e['spans_disk'])
    return jsonify({"dags": out})


@app.route('/api/history')
def api_history():
    """Persisted spans from disk — survives Master restarts, unlike the in-memory ring."""
    now = int(time.time() * 1000)
    try:
        hours = max(1, min(int(request.args.get('hours', 24)), 24 * 30))
    except (TypeError, ValueError):
        hours = 24
    try:
        limit = max(10, min(int(request.args.get('limit', 2000)), 20000))
    except (TypeError, ValueError):
        limit = 2000
    f = (request.args.get('filter') or '').strip()

    # Same resolution as /api/timeline: a pipeline NAME is not a job-id substring, so passing it
    # through as a filter matches nothing. Resolve it here and filter on the returned ids.
    dag = (request.args.get('dag') or '').strip()
    wanted = None
    if dag:
        wanted = _jobs_of_dag(dag)
        if not wanted:
            wanted = None
            f = f or dag

    raw = titan_communicate(OP_STATS_JSON, "history:{}:{}:{}:{}".format(
        now - hours * 3600 * 1000, now, f, limit))
    if not raw:
        return jsonify({"error": "Master unreachable on :9090", "spans": [], "count": 0})
    try:
        start = raw.find('{')
        data = json.loads(raw[start:]) if start != -1 else {"spans": [], "count": 0}
        if wanted is not None:
            data['spans'] = [sp for sp in data.get('spans', []) if sp.get('id') in wanted]
            data['count'] = len(data['spans'])
            data['dag'] = dag
        return jsonify(data)
    except json.JSONDecodeError as e:
        return jsonify({"error": "Malformed history payload: {}".format(e), "spans": [], "count": 0})


@app.route('/api/history_stats')
def api_history_stats():
    """What is on disk: day files, bytes, retention window, write errors."""
    raw = titan_communicate(OP_STATS_JSON, "history_stats")
    if not raw:
        return jsonify({"error": "Master unreachable on :9090"})
    try:
        start = raw.find('{')
        return jsonify(json.loads(raw[start:]) if start != -1 else {})
    except json.JSONDecodeError as e:
        return jsonify({"error": str(e)})


@app.route('/api/job_dag_map')
def api_job_dag_map():
    """job id -> pipeline name and submission stamp, so Analytics can group spans into runs."""
    manifest = _load_manifest()
    if not manifest:
        return jsonify({"map": {}, "runs": {}})
    ok = {k: v for k, v in manifest.items() if isinstance(v, dict) and v.get('dag')}
    # run_ts is stamped per submission, so it separates two runs of the SAME pipeline exactly.
    # Grouping by a time gap instead merges back-to-back runs into one, which understates the
    # run count and inflates the wall clock.
    return jsonify({"map": {k: v['dag'] for k, v in ok.items()},
                    "runs": {k: v.get('run_ts', 0) for k, v in ok.items()}})


@app.route('/api/board')
def api_board():
    """Contents of the four pre-dispatch holding areas: delayed, blocked, ready, parked."""
    try:
        limit = max(5, min(int(request.args.get('limit', 40)), 200))
    except (TypeError, ValueError):
        limit = 40
    raw = titan_communicate(OP_STATS_JSON, "board:{}".format(limit))
    if not raw:
        return jsonify({"error": "Master unreachable on :9090"})
    try:
        start = raw.find('{')
        return jsonify(json.loads(raw[start:]) if start != -1 else {})
    except json.JSONDecodeError as e:
        return jsonify({"error": "Malformed board payload: {}".format(e)})


@app.route('/api/export')
def api_export():
    """A shareable snapshot of the cluster's important state.

    fmt=md  -> a human-readable report you can paste into an issue or a message
    fmt=json -> the raw stats + metrics, for tooling
    """
    fmt = (request.args.get('fmt') or 'md').strip().lower()
    stats_raw = titan_communicate(OP_STATS_JSON, "")
    metrics_raw = titan_communicate(OP_STATS_JSON, "metrics")
    if not stats_raw or not metrics_raw:
        return "Master unreachable on :9090", 503, {'Content-Type': 'text/plain'}
    try:
        stats = json.loads(stats_raw[stats_raw.find('{'):])
        m = json.loads(metrics_raw[metrics_raw.find('{'):])
    except json.JSONDecodeError as e:
        return "Malformed payload: {}".format(e), 502, {'Content-Type': 'text/plain'}

    stamp = time.strftime('%Y-%m-%d %H:%M:%S')
    fname = "titan-cluster-{}".format(time.strftime('%Y%m%d-%H%M%S'))

    if fmt == 'json':
        body = json.dumps({"generated_at": stamp, "stats": stats, "metrics": m}, indent=2)
        return body, 200, {'Content-Type': 'application/json',
                           'Content-Disposition': 'attachment; filename="{}.json"'.format(fname)}

    st = m.get('store', {})
    qw = m.get('queue_wait', {})
    rt = m.get('retries', {})
    pk = m.get('parked', {})
    dlq = m.get('dlq', {})
    disp = sorted(p[1] for p in m.get('dispatch', []))

    def pctl(arr, q):
        return arr[min(len(arr) - 1, max(0, int(q * len(arr)) - 1))] if arr else 0

    L = []
    L.append("# Titan cluster report")
    L.append("")
    L.append("Generated {}".format(stamp))
    L.append("")
    L.append("## Fleet")
    L.append("")
    L.append("| Node | Capability | Slots | Kind | Active job | Services |")
    L.append("|---|---|---|---|---|---|")
    for w in sorted(stats.get('workers', []), key=lambda x: x['port']):
        L.append("| `{}:{}` | {} | {} | {} | {} | {} |".format(
            w.get('host', '?'), w['port'], w.get('capabilities', ''), w.get('load', ''),
            'permanent' if w.get('permanent') else 'ephemeral',
            w.get('active_job') or '—', len(w.get('services') or [])))
    L.append("")
    L.append("## Performance")
    L.append("")
    L.append("| Layer | p50 | p99 | max | samples |")
    L.append("|---|---|---|---|---|")
    L.append("| Queue wait (submit → dispatch) | {}ms | {}ms | {}ms | {} |".format(
        qw.get('p50', 0), qw.get('p99', 0), qw.get('max', 0), qw.get('n', 0)))
    L.append("| Dispatch placement | {}ms | {}ms | {}ms | {} |".format(
        pctl(disp, .5), pctl(disp, .99), disp[-1] if disp else 0, len(disp)))
    L.append("| Store write | {}ms | {}ms | {}ms | {} |".format(
        st.get('latency_p50', 0), st.get('latency_p99', 0), st.get('latency_max', 0), st.get('ops', 0)))
    L.append("| Store read | {}ms | {}ms | — | {} |".format(
        st.get('read_p50', 0), st.get('read_p99', 0), st.get('reads', 0)))
    for k, v in (m.get('heartbeat') or {}).items():
        rt_vals = sorted(p[1] for p in v)
        L.append("| Heartbeat `{}` | {}ms | {}ms | {}ms | {} |".format(
            k, pctl(rt_vals, .5), pctl(rt_vals, .99), rt_vals[-1] if rt_vals else 0, len(rt_vals)))
    L.append("")
    L.append("## TitanStore")
    L.append("")
    L.append("- **{}** at `{}`".format('Connected' if st.get('connected') else 'DISCONNECTED',
                                       st.get('endpoint', '?')))
    L.append("- {} writes, {} reads, **{} dropped writes**, {} reconnects".format(
        st.get('ops', 0), st.get('reads', 0), st.get('dropped_writes', 0), st.get('reconnects', 0)))
    L.append("- Last successful write {}ms ago".format(st.get('last_write_age_ms', -1)))
    L.append("- Store lists {} workers / {} services; Master has {} workers{}".format(
        st.get('live_workers', 0), st.get('live_services', 0), st.get('master_workers', 0),
        "  ⚠ **state drift**" if st.get('live_workers') != st.get('master_workers') else ""))
    L.append("- {} jobs would be recovered if the Master restarted now".format(st.get('recoverable_jobs', 0)))
    if st.get('last_error'):
        L.append("- Last error: `{}`".format(st['last_error']))
    L.append("")
    L.append("## Scheduling health")
    L.append("")
    L.append("- Ready queue **{}**, parked **{}**, dead-letter **{}**".format(
        stats.get('queue_size', 0), pk.get('total', 0), dlq.get('depth', 0)))
    L.append("- Retry rate **{:.1%}** over {} dispatches".format(rt.get('rate', 0), rt.get('dispatches', 0)))
    if m.get('capability'):
        L.append("")
        L.append("| Capability | Waiting | Parked | Workers |")
        L.append("|---|---|---|---|")
        for cp in m['capability']:
            flag = "  ⚠ dead end" if cp['waiting'] > 0 and cp['workers'] == 0 else ""
            L.append("| {} | {} | {} | {}{} |".format(cp['cap'], cp['waiting'], cp.get('parked', 0),
                                                      cp['workers'], flag))
    if m.get('pending_reasons'):
        L.append("")
        L.append("### Why queued work is not running")
        L.append("")
        for x in m['pending_reasons']:
            L.append("- `{}` — {}".format(x['id'], x['reason']))
    if dlq.get('jobs'):
        L.append("")
        L.append("### Dead-letter queue")
        L.append("")
        for x in dlq['jobs']:
            L.append("- `{}` after {} attempts — {}".format(x['id'], x['attempts'], x.get('reason') or 'no reason'))
    if m.get('scaler_events'):
        L.append("")
        L.append("### Recent cluster events")
        L.append("")
        for e in m['scaler_events'][-12:]:
            L.append("- **{}** — {}".format(e['type'], e['detail']))
    L.append("")
    L.append("---")
    L.append("")
    L.append("_Series are in-memory ring buffers (~10 min at 1s resolution) and reset on Master restart._")

    return "\n".join(L), 200, {'Content-Type': 'text/markdown; charset=utf-8',
                               'Content-Disposition': 'attachment; filename="{}.md"'.format(fname)}


@app.route('/cluster')
def cluster_view():
    """Topology + control-plane vitals. All data comes from two JSON endpoints below."""
    return render_template('cluster.html')


@app.route('/api/cluster_stats')
def api_cluster_stats():
    """The existing cluster snapshot, unchanged — now including host and permanence per worker."""
    raw = titan_communicate(OP_STATS_JSON, "")
    if not raw:
        return jsonify({"error": "Master unreachable on :9090"})
    try:
        start = raw.find('{')
        return jsonify(json.loads(raw[start:]) if start != -1 else {})
    except json.JSONDecodeError as e:
        return jsonify({"error": "Malformed stats payload: {}".format(e)})


@app.route('/api/metrics')
def api_metrics():
    """Sampled control-plane series: queue composition, dispatch duration, throughput, heartbeat RTT."""
    res = (request.args.get('res') or 'fine').strip()
    if res not in ('fine', 'mid', 'coarse'):
        res = 'fine'
    raw = titan_communicate(OP_STATS_JSON, "metrics:{}".format(res))
    if not raw:
        return jsonify({"error": "Master unreachable on :9090"})
    try:
        start = raw.find('{')
        return jsonify(json.loads(raw[start:]) if start != -1 else {})
    except json.JSONDecodeError as e:
        return jsonify({"error": "Malformed metrics payload: {}".format(e)})


@app.route('/timeline')
@app.route('/timeline/<path:dag_filter>')
def timeline_view(dag_filter=None):
    """Gantt view of recorded executions. Data is fetched client-side from /api/timeline."""
    return render_template('timeline.html', dag_filter=dag_filter or '')


@app.route('/api/timeline')
def api_timeline():
    """Proxy the Master's timeline payload (OP_STATS_JSON with a 'timeline' discriminator)."""
    f = (request.args.get('filter') or '').strip()
    try:
        limit = max(10, min(int(request.args.get('limit', 2000)), 5000))
    except (TypeError, ValueError):
        limit = 2000

    # A DAG's name and its job IDs are usually different strings — the Master can only substring
    # match on job ID, so resolve the name to its jobs here, where the manifest lives.
    dag = (request.args.get('dag') or '').strip()
    wanted = None
    if dag:
        wanted = _jobs_of_dag(dag) or None
        if not wanted:
            # Unknown DAG name: fall back to treating it as a plain substring.
            f = f or dag

    # The Master applies the limit BEFORE we can filter by pipeline: it returns the most recent N
    # spans across every pipeline, and only then do we keep the ones belonging to this DAG. So a
    # pipeline whose spans are older than that window silently comes back short, or empty, with
    # nothing saying why. When a pipeline is selected, ask for the largest window instead, since
    # the filter is what actually bounds the result.
    fetch = MASTER_SPAN_WINDOW if wanted is not None else limit
    raw = titan_communicate(OP_STATS_JSON, "timeline:{}:{}".format(f, fetch))
    if not raw:
        return jsonify({"error": "Master unreachable on :9090", "spans": [], "count": 0})
    try:
        start = raw.find('{')
        data = json.loads(raw[start:]) if start != -1 else {"spans": [], "count": 0}
        if wanted is not None:
            scanned = len(data.get('spans', []))
            data['spans'] = [sp for sp in data.get('spans', []) if sp.get('id') in wanted]
            data['count'] = len(data['spans'])
            data['dag'] = dag
            data['scanned'] = scanned
            data['known_jobs'] = len(wanted)
            # The Master's in-memory ring is capped. If it handed back a full window, spans older
            # than it exist but were never offered to the filter, so this view may be partial.
            data['window_saturated'] = scanned >= MASTER_SPAN_WINDOW
            # Short of the job count means either the pipeline has not finished running, or its
            # older spans have already aged out of the live ring. From here those look identical,
            # so report the shortfall and point at the disk view rather than guessing.
            data['partial'] = data['count'] < len(wanted)
        return jsonify(data)
    except json.JSONDecodeError as e:
        return jsonify({"error": "Malformed timeline payload: {}".format(e), "spans": [], "count": 0})


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

    # Sidebar list, newest first, searchable, one page at a time.
    #
    # This used to resolve status for EVERY job in EVERY pipeline on every page load. At 1,105
    # pipelines and 37,171 jobs that is 310 sequential round trips to the Master, about 13 seconds,
    # to render a sidebar that only shows a badge and an x/y count. Status is now fetched for the
    # pipelines actually on screen, which is a fixed cost regardless of how much history exists.
    q = (request.args.get("q") or "").strip().lower()
    try:
        page = max(1, int(request.args.get("page", 1)))
    except (TypeError, ValueError):
        page = 1

    entries = list(dag_registry.items())
    if q:
        entries = [(d, m) for d, m in entries
                   if q in (m.get("name") or "").lower() or q in d.lower()]
    # `submitted` is the latest run timestamp, kept current by scan_yaml_dags from the manifest.
    entries.sort(key=lambda dm: dm[1].get("submitted", 0), reverse=True)

    matched = len(entries)
    pages = max(1, (matched + DAGS_PER_PAGE - 1) // DAGS_PER_PAGE)
    page = min(page, pages)
    start = (page - 1) * DAGS_PER_PAGE
    window = entries[start:start + DAGS_PER_PAGE]

    # A pipeline opened directly may sit on another page or be filtered out; keep it resolvable
    # so the detail view and its sidebar badge never disagree.
    if dag_id and dag_id in dag_registry and all(d != dag_id for d, _ in window):
        window = window + [(dag_id, dag_registry[dag_id])]

    # Count the LATEST run, not every run ever submitted under this name, so the sidebar agrees
    # with the graph the detail view draws.
    latest_jobs = {did: _jobs_of_run(m, (_run_stamps(m) or [None])[0]) for did, m in window}
    visible_jobs = [jid for jl in latest_jobs.values() for jid in jl]
    authoritative = fetch_bulk_status(visible_jobs)

    dag_list = []
    for did, meta in window:
        jobs_meta = meta.get("job_meta", {})
        run_jobs_side = latest_jobs.get(did, meta["jobs"])
        total = len(run_jobs_side)
        done = sum(1 for jid in run_jobs_side
                   if (authoritative.get(jid) or jobs_meta.get(jid, {}).get("status")) == "COMPLETED")
        dag_status = _resolve_dag_status_from_meta(meta, authoritative)
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

        # Build jobs_data. Status comes from the Master's store, which keeps every job; the
        # worker-history window (job_meta) is only a fallback because it holds 10 entries per
        # worker and silently drops the rest of a larger graph.
        # Show ONE run, latest first.
        #
        # A pipeline name is reused across runs and each run carries its own job IDs, so the
        # registry accumulates every run ever submitted under that name. Rendering all of them
        # drew N disconnected copies of the same graph and called it one pipeline: 'nightly-etl'
        # came out as 34 nodes across 8 runs rather than the 5 nodes it actually has.
        run_of = {jid: (jobs_meta.get(jid, {}) or {}).get("run_ts", 0) for jid in meta["jobs"]}
        run_stamps = sorted({t for t in run_of.values() if t}, reverse=True)

        selected_run = None
        req_run = request.args.get("run")
        if req_run:
            try:
                rr = int(req_run)
                if rr in run_stamps:
                    selected_run = rr
            except (TypeError, ValueError):
                selected_run = None
        if selected_run is None and run_stamps:
            selected_run = run_stamps[0]

        # Jobs with no recorded run (discovered from live stats rather than the manifest) cannot
        # be attributed, so _jobs_of_run lets them ride along with the newest run rather than vanish.
        run_jobs = _jobs_of_run(meta, selected_run)

        run_options = []
        for idx, ts in enumerate(run_stamps):
            run_options.append({
                "ts": ts,
                "label": time.strftime("%d %b %H:%M:%S", time.localtime(ts / 1000)) if ts else "unknown",
                "jobs": sum(1 for v in run_of.values() if v == ts),
                "is_latest": idx == 0,
            })

        jobs_data = []
        # The sidebar pass already resolved this pipeline's jobs (it is force-included in the
        # window), so only ask the Master for anything it missed.
        unresolved = [j for j in run_jobs if j not in authoritative]
        if unresolved:
            authoritative.update(fetch_bulk_status(unresolved))
        for jid in run_jobs:
            stored = jobs_meta.get(jid, {})
            status = authoritative.get(jid) or stored.get("status", "WAITING")
            if status == "UNKNOWN":
                status = stored.get("status") or stored.get("final_status") or "WAITING"
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
        graph_info    = {}
        graph_svg     = build_dag_svg(jobs_data, info=graph_info)

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
            "runs":            run_options,
            "selected_run":    selected_run,
            "run_count":       len(run_options),
            "showing_latest":  bool(run_options) and selected_run == run_stamps[0],
            "graph_total":     graph_info.get("total", 0),
            "graph_rendered":  graph_info.get("rendered", 0),
            "graph_truncated": graph_info.get("truncated", False),
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
        page        = page,
        pages       = pages,
        matched     = matched,
        per_page    = DAGS_PER_PAGE,
        showing_from = (start + 1) if matched else 0,
        showing_to  = min(start + DAGS_PER_PAGE, matched),
        query       = request.args.get("q") or "",
        status_color = status_color,
        status_text  = status_text,
        dot_colors  = STATUS_DOT,
    )


def _graph_jobs(dag_id, run_ts=None):
    """Jobs of one run of a pipeline, with statuses resolved. Shared by every export format."""
    meta = dag_registry[dag_id]
    jobs_meta = meta.get("job_meta", {})
    stamps = _run_stamps(meta)
    if run_ts is None and stamps:
        run_ts = stamps[0]
    jobs = _jobs_of_run(meta, run_ts)
    authoritative = fetch_bulk_status(jobs)
    out = []
    for jid in jobs:
        stored = jobs_meta.get(jid, {})
        status = authoritative.get(jid) or stored.get("status", "WAITING")
        if status in ("DEAD", "UNKNOWN", ""):
            status = "FAILED"
        out.append({
            "id": jid, "status": status,
            "requirement": stored.get("requirement", "GENERAL"),
            "depends_on": _yaml_job_deps.get(jid, []),
            "worker": stored.get("worker"), "time": stored.get("time", ""),
            "is_service": "svc" in jid.lower() or "service" in jid.lower(),
        })
    return out


_MERMAID_CLASS = {
    "COMPLETED": "done", "RUNNING": "run", "FAILED": "fail",
    "CANCELLED": "cancel", "WAITING": "wait", "PENDING": "wait",
}


def _mermaid_id(jid, idx):
    """Mermaid node ids must be alphanumeric-ish, so map to a short stable handle."""
    return "n%d" % idx


def build_dag_mermaid(jobs_data):
    """Mermaid flowchart source.

    Best format below a few hundred nodes: it is small, pasteable into mermaid.live, GitHub or
    Notion, and the renderer does its own layout, which beats a fixed server-side one for wide
    graphs. Above roughly 300 nodes Mermaid's layout gets slow and often gives up, which is what
    the DOT export is for.
    """
    ids = {j["id"]: _mermaid_id(j["id"], i) for i, j in enumerate(jobs_data)}
    known = set(ids)
    lines = ["flowchart LR"]
    for j in jobs_data:
        label = j["id"][4:] if j["id"].startswith("DAG-") else j["id"]
        label = label.replace('"', "'")
        lines.append(f'  {ids[j["id"]]}["{label}"]:::{_MERMAID_CLASS.get(j["status"], "wait")}')
    for j in jobs_data:
        for p in j.get("depends_on", []):
            if p in known:
                lines.append(f'  {ids[p]} --> {ids[j["id"]]}')
    lines += [
        "  classDef done fill:#1b3d2a,stroke:#4caf6e,color:#d7f0e0;",
        "  classDef run fill:#3d3218,stroke:#ffb74d,color:#f7e7cd;",
        "  classDef fail fill:#3d1b1b,stroke:#ff5252,color:#f7d7d7;",
        "  classDef wait fill:#22262e,stroke:#9090b0,color:#cfd6e0;",
        "  classDef cancel fill:#2b3238,stroke:#78909c,color:#cfd6e0;",
    ]
    return "\n".join(lines)


_DOT_COLOR = {
    "COMPLETED": ("#1b3d2a", "#4caf6e"), "RUNNING": ("#3d3218", "#ffb74d"),
    "FAILED": ("#3d1b1b", "#ff5252"), "CANCELLED": ("#2b3238", "#78909c"),
}


def build_dag_dot(jobs_data, name="pipeline"):
    """Graphviz DOT source.

    The right format for genuinely large graphs. Graphviz lays out thousands of nodes where both
    Mermaid and a fixed server-side layout stop being useful:
        dot  -Tsvg graph.dot -o graph.svg     hierarchical, good up to a few thousand
        sfdp -Tsvg graph.dot -o graph.svg     force-directed, for the very large
    """
    safe_name = re.sub(r'[^A-Za-z0-9_]', '_', name)
    lines = [f'digraph {safe_name} {{',
             '  rankdir=LR;',
             '  bgcolor="#121212";',
             '  node [shape=box style="rounded,filled" fontname="Helvetica" fontsize=10];',
             '  edge [color="#55606e" arrowsize=0.7];']
    known = {j["id"] for j in jobs_data}
    for j in jobs_data:
        fill, border = _DOT_COLOR.get(j["status"], ("#22262e", "#9090b0"))
        label = (j["id"][4:] if j["id"].startswith("DAG-") else j["id"]).replace('"', "'")
        lines.append(f'  "{j["id"]}" [label="{label}" fillcolor="{fill}" '
                     f'color="{border}" fontcolor="#e0e0e0"];')
    for j in jobs_data:
        for p in j.get("depends_on", []):
            if p in known:
                lines.append(f'  "{p}" -> "{j["id"]}";')
    lines.append("}")
    return "\n".join(lines)


@app.route('/dags/<dag_id>/graph.mmd')
def api_dag_graph_mermaid(dag_id):
    """Mermaid source for the selected run. Paste into mermaid.live or a Markdown fence."""
    if dag_id not in dag_registry:
        return jsonify({"error": "Unknown pipeline"}), 404
    run = request.args.get("run", type=int)
    jobs = _graph_jobs(dag_id, run)
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", dag_id)
    return Response(build_dag_mermaid(jobs), mimetype="text/plain; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{safe}.mmd"'})


@app.route('/dags/<dag_id>/graph.dot')
def api_dag_graph_dot(dag_id):
    """Graphviz DOT source for the selected run. The format that scales to thousands of nodes."""
    if dag_id not in dag_registry:
        return jsonify({"error": "Unknown pipeline"}), 404
    run = request.args.get("run", type=int)
    jobs = _graph_jobs(dag_id, run)
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", dag_id)
    return Response(build_dag_dot(jobs, dag_id), mimetype="text/vnd.graphviz; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{safe}.dot"'})


@app.route('/dags/<dag_id>/graph.svg')
def api_dag_graph_svg(dag_id):
    """The complete graph as a standalone SVG, with no node cap.

    The on-page view is capped because a thousand nodes is slow to lay out and impossible to read
    in a panel. That is a rendering decision, not a limit on what you are allowed to see, so the
    whole graph stays available as a file you can open, zoom and share.
    """
    if dag_id not in dag_registry:
        return jsonify({"error": "Unknown pipeline"}), 404

    meta = dag_registry[dag_id]
    jobs_meta = meta.get("job_meta", {})
    authoritative = fetch_bulk_status(list(meta["jobs"]))
    jobs_data = []
    for jid in meta["jobs"]:
        stored = jobs_meta.get(jid, {})
        status = authoritative.get(jid) or stored.get("status", "WAITING")
        if status in ("DEAD", "UNKNOWN", ""):
            status = "FAILED"
        jobs_data.append({
            "id":          jid,
            "status":      status,
            "requirement": stored.get("requirement", "GENERAL"),
            "depends_on":  _yaml_job_deps.get(jid, []),
            "worker":      stored.get("worker"),
            "time":        stored.get("time", ""),
            "is_service":  "svc" in jid.lower() or "service" in jid.lower(),
        })

    svg = build_dag_svg(jobs_data, max_nodes=0)      # 0 = draw everything
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", dag_id)
    return Response(svg, mimetype="image/svg+xml", headers={
        "Content-Disposition": f'attachment; filename="{safe}-graph.svg"'
    })


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

    # Paginated, newest first. A long-lived workspace accumulates thousands of artifacts, and
    # returning all of them made the response large and the grid expensive to lay out for files
    # nobody was going to scroll to.
    total = len(files)
    try:
        per = max(1, min(int(request.args.get("per", 60)), 500))
    except (TypeError, ValueError):
        per = 60
    try:
        page = max(1, int(request.args.get("page", 1)))
    except (TypeError, ValueError):
        page = 1
    pages = max(1, (total + per - 1) // per)
    page = min(page, pages)
    start = (page - 1) * per
    return jsonify({
        "files": files[start:start + per],
        "total": total, "page": page, "pages": pages, "per": per,
        "showing_from": (start + 1) if total else 0,
        "showing_to": min(start + per, total),
    })


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
        # Take the SAME advisory lock the SDK uses. This handler read-modify-writes the whole
        # manifest, and without the lock it can interleave with an SDK submission and silently
        # drop one side's entries. The SDK now pushes from a background thread, so this overlap is
        # more likely than when the push blocked the submitting process.
        lock_file = None
        try:
            import fcntl as _fcntl
            lock_file = open(manifest_path + ".lock", "w")
            _fcntl.flock(lock_file.fileno(), _fcntl.LOCK_EX)
        except (ImportError, OSError):
            lock_file = None
        try:
            existing = {}
            if os.path.exists(manifest_path):
                with open(manifest_path) as f:
                    existing = json.loads(f.read())
            existing.update(incoming)
            # Write to a temp file and rename, so a reader never sees a half-written manifest.
            # No indent: the SDK posts only one submission's entries now, but this handler still
            # rewrites the whole merged map, and pretty-printing several MB is pure cost.
            tmp = "{}.sync.{}.tmp".format(manifest_path, os.getpid())
            with open(tmp, 'w') as f:
                f.write(json.dumps(existing))
            os.replace(tmp, manifest_path)
        finally:
            if lock_file is not None:
                try:
                    import fcntl as _fcntl
                    _fcntl.flock(lock_file.fileno(), _fcntl.LOCK_UN)
                except (ImportError, OSError):
                    pass
                try:
                    lock_file.close()
                except OSError:
                    pass
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
    # One bulk call for every job, so this endpoint agrees with the page.
    api_authoritative = fetch_bulk_status(
        [j for mt in dag_registry.values() for j in mt.get("jobs", [])])

    for did, meta in dag_registry.items():
        jobs_meta = meta.get("job_meta", {})
        jobs_out  = []
        for jid in meta["jobs"]:
            live = api_authoritative.get(jid)
            jobs_out.append({
                "id":     jid,
                "status": live if live and live not in ("UNKNOWN", "NULL", "")
                          else jobs_meta.get(jid, {}).get("status", "WAITING"),
                "worker": jobs_meta.get(jid, {}).get("worker"),
            })
        result.append({
            "id":     did,
            "name":   meta["name"],
            "status": _resolve_dag_status_from_meta(meta, api_authoritative),
            "jobs":   jobs_out,
        })
    return jsonify(result)


# ================================================================
# Helpers
# ================================================================

def fetch_bulk_status(job_ids):
    """Resolve job statuses from the Master's store, which retains every job.

    OP_STATS_JSON only carries workerRecentHistory — 10 entries per worker — so any DAG with more
    nodes than that has statuses missing from the live payload, and they render as PENDING long
    after finishing. This asks the authoritative source instead.
    """
    ids = [j for j in job_ids if j]
    if not ids:
        return {}
    out = {}
    # Chunked so the request stays well under the protocol's 10MB frame cap.
    for i in range(0, len(ids), 120):
        chunk = ids[i:i + 120]
        raw = titan_communicate(OP_STATS_JSON, "status:" + ",".join(chunk))
        if not raw:
            continue
        try:
            start = raw.find('{')
            if start != -1:
                out.update(json.loads(raw[start:]))
        except json.JSONDecodeError:
            continue
    return out


def _resolve_dag_status_from_jobs(jobs):
    statuses = [j["status"] for j in jobs]
    if "FAILED"    in statuses: return "FAILED"
    if "RUNNING"   in statuses: return "RUNNING"
    if "CANCELLED" in statuses: return "CANCELLED"
    if all(s == "COMPLETED" for s in statuses): return "COMPLETED"
    return "PENDING"

def _resolve_dag_status_from_meta(meta, authoritative=None):
    """Roll a DAG's jobs up into one status.

    `authoritative` is a job-id -> status map read from the Master's store, which retains every
    job. Without it this falls back to job_meta, which is built from workerRecentHistory (10
    entries per worker) — so any DAG whose jobs have rolled out of that window resolved to
    PENDING even after completing. That is what made every pipeline look stuck.
    """
    jobs_meta = meta.get("job_meta", {})
    statuses = []
    for jid in meta.get("jobs", []) or jobs_meta.keys():
        live = (authoritative or {}).get(jid)
        if live and live not in ("UNKNOWN", "NULL", ""):
            statuses.append(live)
            continue
        v = jobs_meta.get(jid, {})
        statuses.append(v.get("status") or v.get("final_status") or "WAITING")

    if not statuses:            return "PENDING"
    if "DEAD"      in statuses: return "FAILED"
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
