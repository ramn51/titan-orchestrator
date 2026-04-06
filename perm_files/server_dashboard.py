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
import re
from flask import Flask, render_template_string, request, jsonify, send_from_directory

app = Flask(__name__)

# --- CONFIGURATION ---
SCHEDULER_HOST = "127.0.0.1"
SCHEDULER_PORT = 9090

# --- TITAN PROTOCOL CONSTANTS ---
CURRENT_VERSION = 1
OP_STATS_JSON      = 0x09
OP_GET_LOGS        = 0x16
OP_GET_JOB_STATUS  = 0x55
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
        _SEV = {"RUNNING": 5, "DEAD": 4, "FAILED": 4, "COMPLETED": 3, "WAITING": 2}
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

        dag_registry[dag_key]["job_meta"][job_id] = {
            "worker": meta["worker"],
            "status": incoming_status,
            "time":   meta["time"],
        }
        # Once a job genuinely completes in the current run (has a real timestamp),
        # clear the reset guard so it doesn't keep getting wiped on future polls.
        if incoming_status == "COMPLETED" and meta.get("completed_at", 0) > 0:
            dag_registry[dag_key].pop("_reset_ts", None)

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
# SHARED CSS + NAV — injected into both pages
# ================================================================

SHARED_STYLE = """
<style>
  body { background:#121212; color:#e0e0e0; font-family:'Segoe UI',sans-serif; padding:0; margin:0; }
  a { text-decoration:none; color:inherit; }
  a:hover { text-decoration:underline; color:#64b5f6; }

  /* NAV */
  .topnav { background:#1a1a2e; border-bottom:1px solid #333; padding:0 24px; display:flex; align-items:center; gap:0; }
  .topnav-brand { font-size:15px; font-weight:700; letter-spacing:1px; padding:14px 20px 14px 0; color:#fff; border-right:1px solid #333; margin-right:8px; }
  .topnav a.tab { padding:14px 18px; font-size:13px; color:#aaa; border-bottom:3px solid transparent; display:inline-block; }
  .topnav a.tab:hover { color:#fff; text-decoration:none; }
  .topnav a.tab.active { color:#64b5f6; border-bottom-color:#64b5f6; }
  .topnav-right { margin-left:auto; display:flex; align-items:center; gap:10px; font-size:12px; color:#777; }
  .conn-dot { width:8px; height:8px; border-radius:50%; display:inline-block; }

  .page-wrap { padding:20px 24px; }

  /* Cards */
  .card { background:#1e1e1e; border:1px solid #333; border-radius:8px; padding:20px; }

  /* Badges */
  .badge { font-weight:600; font-size:11px; padding:2px 8px; border-radius:99px; display:inline-flex; align-items:center; gap:4px; }
  .badge-dot { width:6px; height:6px; border-radius:50%; flex-shrink:0; }
  .b-COMPLETED { background:rgba(0,230,118,0.1); color:#00e676; }
  .b-RUNNING   { background:rgba(255,183,77,0.12); color:#ffb74d; }
  .b-WAITING   { background:rgba(120,120,160,0.12); color:#9090b0; }
  .b-FAILED    { background:rgba(255,82,82,0.1); color:#ff5252; }
  .b-CANCELLED { background:rgba(255,160,0,0.1); color:#ffa000; }
  .b-PENDING   { background:rgba(120,120,160,0.12); color:#9090b0; }

  .req-gpu { background:#2a1040; color:#ce93d8; font-size:10px; padding:1px 7px; border-radius:99px; font-weight:500; }
  .req-gen { background:#1e2a1e; color:#81c784; font-size:10px; padding:1px 7px; border-radius:99px; font-weight:500; }

  /* Section label */
  .sec-title { font-size:10px; font-weight:600; text-transform:uppercase; letter-spacing:.07em; color:#555; margin:14px 0 8px; border-bottom:1px solid #2a2a2a; padding-bottom:4px; }

  @keyframes pulse { 0%{opacity:1;box-shadow:0 0 0 0 rgba(255,183,77,.7)} 70%{opacity:.8;box-shadow:0 0 0 6px rgba(255,183,77,0)} 100%{opacity:1;box-shadow:0 0 0 0 rgba(255,183,77,0)} }
</style>
"""

# ================================================================
# ORIGINAL DASHBOARD HTML  (worker view — unchanged in appearance)
# ================================================================

DASHBOARD_HTML = SHARED_STYLE + """
<head><title>Titan — Orchestrator</title><meta http-equiv="refresh" content="2"></head>
<body>
<nav class="topnav">
  <span class="topnav-brand">⚡ TITAN</span>
  <a class="tab active" href="/">🖥 Orchestrator</a>
  <a class="tab" href="/dags">🔀 DAG Pipelines</a>
  <a class="tab" href="/dags/new">✏️ Constructor</a>
  <div class="topnav-right">
    <span class="conn-dot" style="background:{{ status_color }}"></span>
    <span style="color:{{ status_color }}; font-weight:600;">{{ status_text }}</span>
    &nbsp;|&nbsp; Workers: {{ stats.active_workers }}
    &nbsp;|&nbsp; Queue: {{ stats.queue_size }}
  </div>
</nav>

<div class="page-wrap">
<style>
  .worker-card { background:#1e1e1e; border:1px solid #333; border-radius:8px; padding:20px; width:320px; display:flex; flex-direction:column; }
  .grid { display:flex; flex-wrap:wrap; gap:20px; justify-content:center; }
  .skill-badge { font-size:.7em; background:#424242; padding:2px 6px; border-radius:4px; color:#bbb; margin-left:8px; vertical-align:middle; border:1px solid #555; }
  .service-tag { background:#263238; padding:5px 8px; margin-top:5px; border-radius:4px; border-left:3px solid #0288d1; font-family:monospace; font-size:.9em; cursor:pointer; transition:background .2s; }
  .service-tag:hover { background:#37474f; }
  .hist-list { list-style:none; padding:0; margin:0; }
  .hist-item { display:flex; justify-content:space-between; align-items:center; font-size:.85em; padding:6px 0; border-bottom:1px solid #2c2c2c; }
  .hist-item:last-child { border-bottom:none; }
  .hist-id { color:#90caf9; font-family:monospace; }
  .hist-meta { text-align:right; }
  .hist-time { display:block; font-size:.8em; color:#757575; }
  .status-badge { font-weight:bold; font-size:.85em; padding:2px 6px; border-radius:4px; }
  .st-COMPLETED { color:#00e676; background:rgba(0,230,118,.1); }
  .st-FAILED    { color:#ff5252; background:rgba(255,82,82,.1); }
  .st-RUNNING   { color:#ffb74d; background:rgba(255,183,77,.12); }
  .st-DEAD      { color:#9e9e9e; background:rgba(158,158,158,.1); border:1px solid #555; }
</style>

<div style="text-align:center; margin-bottom:24px;">
  <h2 style="letter-spacing:2px; margin:0;">🛰️ TITAN ORCHESTRATOR</h2>
</div>

<div class="grid">
  {% for w in stats.workers %}
  <div class="worker-card" style="border-top:4px solid {% if 'GPU' in w.capabilities %}#7c4dff{% else %}#4CAF50{% endif %};">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <div>
        <h3 style="margin:0; display:inline;">Node :{{ w.port }}</h3>
        <span class="skill-badge">{{ w.capabilities }}</span>
      </div>
      <span style="background:#004d40; color:#00e676; padding:2px 8px; border-radius:4px; font-size:.8em;">Load: {{ w.load }}</span>
    </div>

    <div class="sec-title">Current Status</div>
    {% if w.active_job %}
      <div style="background:#2e2010; border:1px solid #f57c00; padding:12px; border-radius:6px; display:flex; align-items:center; gap:12px; margin-top:5px;">
        <div style="width:12px; height:12px; background:#ffb74d; border-radius:50%; animation:pulse 1.5s infinite;"></div>
        <div style="flex-grow:1;">
          <div style="font-size:.7em; color:#ffcc80; letter-spacing:.5px; font-weight:bold;">EXECUTING NOW</div>
          <div style="font-family:monospace; font-size:1.1em; color:#fff;">
            <a href="/logs/{{ w.active_job }}" target="_blank">📄 {{ w.active_job }}</a>
          </div>
        </div>
      </div>
    {% else %}
      <div style="background:#1b2e23; border:1px solid #2e7d32; padding:8px; border-radius:6px; color:#81c784; font-size:.85em; text-align:center; margin-top:5px;">● Worker Idle</div>
    {% endif %}

    <div class="sec-title">Running Services</div>
    {% for svc in w.services %}
      <a href="/logs/{{ svc }}" target="_blank"><div class="service-tag">⚙️ {{ svc }}</div></a>
    {% else %}
      <div style="color:#555; font-style:italic; font-size:.9em; padding:5px 0;">• No active services</div>
    {% endfor %}

    <div class="sec-title">Recent Activity</div>
    <ul class="hist-list">
      {% for job in w.history %}
        <li class="hist-item">
          <span class="hist-id"><a href="/logs/{{ job.id }}" target="_blank">{{ job.id }}</a></span>
          <div class="hist-meta">
            <span class="status-badge st-{{ job.status }}">{{ job.status }}</span>
            <span class="hist-time">{{ job.time }}</span>
          </div>
        </li>
      {% else %}
        <li style="color:#555; font-style:italic; font-size:.9em; padding:5px 0;">• No history yet</li>
      {% endfor %}
    </ul>
  </div>
  {% endfor %}
</div>
</div>
</body>
"""

# ================================================================
# DAG DASHBOARD HTML
# ================================================================

DAG_DASHBOARD_HTML = SHARED_STYLE + """
<head>
<title>Titan — DAG Pipelines</title>
<style>
  .dag-sidebar { width:260px; flex-shrink:0; border-right:1px solid #2a2a2a; overflow-y:auto; padding:12px; height:calc(100vh - 49px); }
  .dag-main { flex:1; overflow-y:auto; padding:20px 24px; height:calc(100vh - 49px); }
  .layout { display:flex; }

  .dag-item { padding:10px 12px; border-radius:8px; border:1px solid transparent; cursor:pointer; margin-bottom:6px; transition:background .15s; }
  .dag-item:hover { background:#1e1e2e; }
  .dag-item.sel { background:#1a2040; border-color:#4f8ef7; }
  .dag-name { font-size:13px; font-weight:500; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; margin-bottom:4px; }
  .dag-meta { font-size:11px; color:#666; display:flex; align-items:center; gap:8px; }

  .graph-wrap { background:#0d0d14; border:1px solid #2a2a2a; border-radius:10px; padding:14px; overflow-x:auto; margin-bottom:16px; }

  .log-panel { background:#1a1a24; border:1px solid #2a2a2a; border-radius:10px; padding:16px; }
  .log-meta-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:8px; margin-bottom:12px; }
  .log-meta-item { background:#0d0d14; border-radius:6px; padding:8px 10px; }
  .log-meta-label { font-size:10px; color:#555; margin-bottom:3px; text-transform:uppercase; }
  .log-meta-val { font-size:13px; font-weight:500; color:#e0e0e0; }
  .log-body { background:#000; border-radius:6px; padding:10px 14px; font-family:'Consolas','Fira Code',monospace; font-size:12px; color:#a8b8d8; line-height:1.8; white-space:pre-wrap; min-height:80px; max-height:220px; overflow-y:auto; }

  .progress-bar { height:5px; background:#2a2a2a; border-radius:3px; overflow:hidden; display:flex; margin:6px 0 10px; }
  .progress-seg { height:100%; transition:width .4s; }

  .stat-pills { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:14px; }
  .stat-pill { background:#1a1a24; border:1px solid #2a2a2a; border-radius:8px; padding:7px 14px; font-size:12px; }
  .stat-pill b { font-size:18px; display:block; margin-bottom:1px; }

  .empty-state { color:#444; font-size:14px; text-align:center; padding:60px 0; }
  .close-x { background:none; border:none; color:#555; font-size:20px; cursor:pointer; line-height:1; }
  .close-x:hover { color:#aaa; }

  .node-g { cursor:pointer; }
</style>
</head>
<body>

<nav class="topnav">
  <span class="topnav-brand">⚡ TITAN</span>
  <a class="tab" href="/">🖥 Orchestrator</a>
  <a class="tab active" href="/dags">🔀 DAG Pipelines</a>
  <a class="tab" href="/dags/new">✏️ Constructor</a>
  <div class="topnav-right">
    <span class="conn-dot" style="background:{{ status_color }}"></span>
    <span style="color:{{ status_color }}; font-weight:600;">{{ status_text }}</span>
    &nbsp;|&nbsp; {{ dag_count }} DAGs tracked
  </div>
</nav>

<div class="layout">

  <!-- Sidebar -->
  <div class="dag-sidebar">
    <div class="sec-title" style="padding:4px 8px 8px;">Pipelines</div>
    {% if dags %}
      {% for d in dags %}
      <div class="dag-item {% if selected_dag and selected_dag.id == d.id %}sel{% endif %}"
           onclick="window.location='/dags/{{ d.id }}'">
        <div class="dag-name">{{ d.name }}</div>
        <div class="dag-meta">
          <span class="badge b-{{ d.status }}" style="font-size:10px; padding:1px 7px;"
                data-dag-badge="{{ d.id }}">{{ d.status }}</span>
          <span data-dag-count="{{ d.id }}">{{ d.done }}/{{ d.total }}</span>
        </div>
      </div>
      {% endfor %}
    {% else %}
      <div style="color:#444; font-size:12px; padding:12px 8px;">No DAGs discovered yet.<br><br>
      Jobs prefixed with <span style="color:#64b5f6; font-family:monospace;">DAG-</span> are auto-tracked here.</div>
    {% endif %}
  </div>

  <!-- Main panel -->
  <div class="dag-main" id="dag-main-panel" data-dag-id="{{ selected_dag.id if selected_dag else '' }}">
    {% if selected_dag %}

      <!-- Header -->
      <div style="display:flex; align-items:flex-start; justify-content:space-between; margin-bottom:14px; flex-wrap:wrap; gap:10px;">
        <div>
          <div style="font-size:18px; font-weight:600; margin-bottom:4px;">{{ selected_dag.name }}</div>
          <div style="font-size:12px; color:#555;">{{ selected_dag.id }}</div>
        </div>
        <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
          <span class="badge b-{{ selected_dag.status }}" id="dag-header-badge">
            <span class="badge-dot" style="background:{{ dot_colors[selected_dag.status] }}"></span>
            {{ selected_dag.status }}
          </span>
          {% for req in selected_dag.requirements %}
            <span class="{% if req == 'GPU' %}req-gpu{% else %}req-gen{% endif %}">{{ req }}</span>
          {% endfor %}
          {% if selected_dag.can_redeploy %}
          <button id="redeploy-btn"
            onclick="redeployDag('{{ selected_dag.id }}')"
            style="background:#0d1a2e; border:1px solid #4f8ef766; color:#64b5f6;
                   font-size:12px; padding:5px 14px; border-radius:6px; cursor:pointer;
                   font-weight:600; transition:background .2s;">
            ↺ Redeploy
          </button>
          {% endif %}
        </div>
      </div>

      <!-- Stats -->
      <div class="stat-pills">
        {% for label, count, color in selected_dag.stat_pills %}
        <div class="stat-pill"><b data-stat-pill="{{ label.upper() }}" style="color:{{ color }}">{{ count }}</b>{{ label }}</div>
        {% endfor %}
      </div>

      <!-- Progress -->
      <div style="display:flex; justify-content:space-between; font-size:11px; color:#555; margin-bottom:4px;">
        <span>Progress</span><span id="progress-pct">{{ selected_dag.done_pct }}%</span>
      </div>
      <div class="progress-bar">
        <div data-progress-seg="COMPLETED" class="progress-seg" style="width:{{ selected_dag.done_pct }}%; background:#4caf6e;"></div>
        <div data-progress-seg="RUNNING"   class="progress-seg" style="width:{{ selected_dag.run_pct }}%;  background:#ffb74d;"></div>
        <div data-progress-seg="FAILED"    class="progress-seg" style="width:{{ selected_dag.fail_pct }}%; background:#ff5252;"></div>
      </div>

      <!-- Graph -->
      <div class="sec-title">DAG Graph — click a node to view logs</div>
      <div class="graph-wrap">{{ selected_dag.graph_svg | safe }}</div>

      <!-- HITL approval banner (shown when any job in this DAG is waiting for approval) -->
      <div id="hitl-banner" style="display:none; margin-bottom:16px; border:1px solid #ffb74d66;
           background:#1a160a; border-radius:10px; padding:14px 16px;">
        <div style="font-size:12px; font-weight:600; color:#ffb74d; margin-bottom:10px; letter-spacing:.05em;">
          ⏸ HUMAN-IN-THE-LOOP — AWAITING APPROVAL
        </div>
        <div id="hitl-cards" style="display:flex; flex-direction:column; gap:8px;"></div>
      </div>

      <!-- Log panel (shown if job selected) -->
      {% if selected_job %}
      <div class="log-panel">
        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px;">
          <div style="display:flex; align-items:center; gap:10px;">
            <span style="font-size:14px; font-weight:500;">{{ selected_job.id }}</span>
            <span class="badge b-{{ selected_job.status }}"
                  id="log-panel-badge" data-job-id="{{ selected_job.id }}">
              <span class="badge-dot" style="background:{{ dot_colors[selected_job.status] }}"></span>
              {{ selected_job.status }}
            </span>
            {% if selected_job.is_service %}
              <span class="badge" style="background:#162040; color:#4f8ef7;">service</span>
            {% endif %}
          </div>
          <div style="display:flex; align-items:center; gap:8px;">
            <button id="replay-btn"
              onclick="replayJob('{{ selected_job.id }}')"
              style="background:#0d0d14; border:1px solid #2a2a2a; color:#aaa; padding:4px 12px; border-radius:6px; cursor:pointer; font-size:12px;">
              ↺ Replay
            </button>
            <a href="/dags/{{ selected_dag.id }}"><button class="close-x">×</button></a>
          </div>
        </div>
        <div class="log-meta-grid">
          <div class="log-meta-item"><div class="log-meta-label">Worker</div><div class="log-meta-val">{{ selected_job.worker or 'unassigned' }}</div></div>
          <div class="log-meta-item"><div class="log-meta-label">Requirement</div><div class="log-meta-val">{{ selected_job.requirement }}</div></div>
          <div class="log-meta-item"><div class="log-meta-label">Last Seen</div><div class="log-meta-val">{{ selected_job.time or '—' }}</div></div>
        </div>
        <div class="sec-title">Stdout / Stderr</div>
        <div class="log-body" id="log-body">{{ selected_job.logs }}</div>
      </div>
      <script>
        // Live-poll logs for this job every 2s
        const jobId = "{{ selected_job.id }}";
        async function pollLogs() {
          try {
            const r = await fetch(`/api/logs_raw/${jobId}`);
            if (r.ok) {
              const t = await r.text();
              document.getElementById('log-body').textContent = t || 'No logs yet.';
            }
          } catch(e) {}
          setTimeout(pollLogs, 2000);
        }
        pollLogs();
      </script>
      {% endif %}

    {% else %}
      <div class="empty-state">
        ← Select a pipeline to visualize<br><br>
        <span style="font-size:12px; color:#333;">DAGs are auto-discovered from job IDs prefixed with <span style="color:#64b5f6; font-family:monospace;">DAG-</span></span>
      </div>
    {% endif %}
  </div>

</div>

<script>
// ── Live polling — no full page reload ──────────────────────
// Polls /api/dag_status every 3s and patches only what changed.

const STATUS_DOT = {
  COMPLETED: "#4caf6e", RUNNING: "#ffb74d", WAITING: "#9090b0",
  FAILED: "#ff5252",   DEAD: "#ff5252",    CANCELLED: "#ffa000", PENDING: "#9090b0"
};
const STATUS_FILL = {
  COMPLETED:"#132213", RUNNING:"#1e2010", WAITING:"#141420",
  FAILED:"#221313",    DEAD:"#221313",    CANCELLED:"#221a10", PENDING:"#141420"
};
const BADGE_BG = {
  COMPLETED:"rgba(0,230,118,.1)",  RUNNING:"rgba(255,183,77,.12)",
  WAITING:"rgba(120,120,160,.12)", FAILED:"rgba(255,82,82,.1)",
  DEAD:"rgba(255,82,82,.1)",       CANCELLED:"rgba(255,160,0,.1)", PENDING:"rgba(120,120,160,.12)"
};
const BADGE_COLOR = {
  COMPLETED:"#00e676", RUNNING:"#ffb74d", WAITING:"#9090b0",
  FAILED:"#ff5252",    DEAD:"#ff5252",    CANCELLED:"#ffa000", PENDING:"#9090b0"
};

// Track last known statuses so we only repaint changed nodes
let lastStatuses = {};

async function pollDagStatus() {
  try {
    const r = await fetch("/api/dag_status");
    if (!r.ok) return;
    const dags = await r.json();

    dags.forEach(dag => {
      // ── 1. Sidebar badge for this DAG ──
      const sidebarBadge = document.querySelector(`[data-dag-badge="${dag.id}"]`);
      if (sidebarBadge) patchBadge(sidebarBadge, dag.status);

      // ── 2. Sidebar progress count ──
      const sidebarCount = document.querySelector(`[data-dag-count="${dag.id}"]`);
      if (sidebarCount) {
        const done = dag.jobs.filter(j => j.status === "COMPLETED").length;
        sidebarCount.textContent = `${done}/${dag.jobs.length}`;
      }

      // ── 3. If this dag is currently open in the main panel ──
      const mainPanel = document.getElementById("dag-main-panel");
      if (!mainPanel) return;
      const openDagId = mainPanel.dataset.dagId;
      if (openDagId !== dag.id) return;

      // Header badge
      const headerBadge = document.getElementById("dag-header-badge");
      if (headerBadge) patchBadge(headerBadge, dag.status);

      // Stat pill counts
      const counts = { COMPLETED:0, RUNNING:0, FAILED:0, WAITING:0 };
      dag.jobs.forEach(j => { if (j.status in counts) counts[j.status]++; });
      Object.entries(counts).forEach(([s, n]) => {
        const el = document.querySelector(`[data-stat-pill="${s}"]`);
        if (el) el.textContent = n;
      });

      // Progress bar segments
      const total = dag.jobs.length || 1;
      const pcts = {
        COMPLETED: Math.round(counts.COMPLETED / total * 100),
        RUNNING:   Math.round(counts.RUNNING   / total * 100),
        FAILED:    Math.round(counts.FAILED    / total * 100),
      };
      ["COMPLETED","RUNNING","FAILED"].forEach(s => {
        const seg = document.querySelector(`[data-progress-seg="${s}"]`);
        if (seg) seg.style.width = pcts[s] + "%";
      });

      // Progress % label
      const pctLabel = document.getElementById("progress-pct");
      if (pctLabel) pctLabel.textContent = pcts.COMPLETED + "%";

      // Update stat pill progress % label
      const seg = document.querySelector('[data-progress-seg="COMPLETED"]');
      if (seg) seg.style.width = pcts.COMPLETED + "%";

      // ── 4. SVG graph nodes ──
      dag.jobs.forEach(j => {
        const prev = lastStatuses[j.id];
        if (prev === j.status) return; // no change — skip repaint
        lastStatuses[j.id] = j.status;

        const dot  = STATUS_DOT[j.status]  || "#666";
        const fill = STATUS_FILL[j.status] || "#141420";

        // Status dot circle inside node
        const circle = document.querySelector(`[data-node-dot="${j.id}"]`);
        if (circle) circle.setAttribute("fill", dot);

        // Node background rect
        const rect = document.querySelector(`[data-node-rect="${j.id}"]`);
        if (rect) {
          rect.setAttribute("fill", fill);
          rect.setAttribute("stroke", dot);
        }

        // Edge strokes leading OUT of this node
        document.querySelectorAll(`[data-edge-from="${j.id}"]`).forEach(path => {
          path.setAttribute("stroke", dot);
          path.setAttribute("stroke-dasharray", j.status === "WAITING" ? "4 3" : "none");
        });

        // If this job's log panel is open, refresh its badge too
        const logBadge = document.getElementById("log-panel-badge");
        if (logBadge && logBadge.dataset.jobId === j.id) {
          patchBadge(logBadge, j.status);
        }
      });
    });

  } catch(e) { /* silent — master may be briefly unreachable */ }

  setTimeout(pollDagStatus, 3000);
}

function patchBadge(el, status) {
  el.textContent = status;
  el.style.background = BADGE_BG[status]    || BADGE_BG.PENDING;
  el.style.color       = BADGE_COLOR[status] || BADGE_COLOR.PENDING;
}

// Kick off polling immediately
pollDagStatus();

// ── Redeploy ─────────────────────────────────────────────────
async function redeployDag(dagId) {
  const btn = document.getElementById("redeploy-btn");
  if (!btn) return;
  btn.disabled = true;
  btn.textContent = "⏳ Redeploying…";
  btn.style.color = "#ffb74d";
  btn.style.borderColor = "#ffb74d66";
  try {
    const r = await fetch(`/api/dag/redeploy/${encodeURIComponent(dagId)}`, { method: "POST" });
    const data = await r.json();
    if (r.ok && data.status === "ok") {
      btn.textContent = "✓ Redeployed";
      btn.style.color = "#4caf6e";
      btn.style.borderColor = "#4caf6e66";
      btn.style.background = "#0d2218";
      setTimeout(() => {
        btn.textContent = "↺ Redeploy";
        btn.style.color = "#64b5f6";
        btn.style.borderColor = "#4f8ef766";
        btn.style.background = "#0d1a2e";
        btn.disabled = false;
      }, 3000);
    } else {
      btn.textContent = "✗ Failed";
      btn.style.color = "#ff5252";
      btn.style.borderColor = "#ff525266";
      setTimeout(() => {
        btn.textContent = "↺ Redeploy";
        btn.style.color = "#64b5f6";
        btn.style.borderColor = "#4f8ef766";
        btn.style.background = "#0d1a2e";
        btn.disabled = false;
      }, 3000);
    }
  } catch {
    btn.textContent = "✗ No connection";
    btn.style.color = "#ff5252";
    setTimeout(() => {
      btn.textContent = "↺ Redeploy";
      btn.style.color = "#64b5f6";
      btn.disabled = false;
    }, 3000);
  }
}

// ── HITL polling ─────────────────────────────────────────────
const mainPanel = document.getElementById("dag-main-panel");
const openDagId = mainPanel ? mainPanel.dataset.dagId : null;

// Track which jobs we've already rendered amber to avoid flicker
let hitlNodes = new Set();

async function pollHitl() {
  if (!openDagId) { setTimeout(pollHitl, 5000); return; }
  try {
    const r = await fetch("/api/hitl/pending");
    if (!r.ok) { setTimeout(pollHitl, 5000); return; }
    const pending = await r.json();

    // job_id in the HITL queue may or may not have the DAG- prefix.
    // Try both so we match whatever the gate script registered.
    const relevant = pending.filter(p => {
      return !!(document.querySelector(`[data-node-rect="${p.job_id}"]`) ||
                document.querySelector(`[data-node-rect="DAG-${p.job_id}"]`));
    });

    const banner = document.getElementById("hitl-banner");
    const cards  = document.getElementById("hitl-cards");
    if (!banner || !cards) { setTimeout(pollHitl, 5000); return; }

    if (relevant.length === 0) {
      banner.style.display = "none";
      hitlNodes.forEach(jid => clearHitlHighlight(jid));
      hitlNodes.clear();
      setTimeout(pollHitl, 5000);
      return;
    }

    banner.style.display = "block";
    const newIds = new Set(relevant.map(p => p.job_id));

    // Remove amber from nodes no longer pending
    hitlNodes.forEach(jid => { if (!newIds.has(jid)) clearHitlHighlight(jid); });
    hitlNodes = newIds;

    // Highlight waiting nodes in amber
    relevant.forEach(p => setHitlHighlight(p.job_id));

    // Rebuild approval cards
    cards.innerHTML = relevant.map(p => {
      const ago = p.ts ? Math.round((Date.now() - p.ts) / 1000) : null;
      const agoStr = ago !== null ? (ago < 60 ? `${ago}s ago` : `${Math.round(ago/60)}m ago`) : '';
      return `<div style="background:#0d0d14; border:1px solid #2a2a2a; border-radius:8px; padding:10px 14px;
                           display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap;">
        <div>
          <div style="font-size:12px; font-family:monospace; color:#e0e0e0;">${p.job_id}</div>
          <div style="font-size:11px; color:#888; margin-top:3px;">${p.message}${agoStr ? ' · ' + agoStr : ''}</div>
        </div>
        <div style="display:flex; gap:8px;">
          <button onclick="hitlDecide('${p.job_id}','approve',this)"
            style="background:#0d2218; border:1px solid #4caf6e66; color:#4caf6e;
                   padding:5px 16px; border-radius:6px; cursor:pointer; font-size:12px; font-weight:600;">
            ✓ Approve
          </button>
          <button onclick="hitlDecide('${p.job_id}','reject',this)"
            style="background:#22100d; border:1px solid #ff525266; color:#ff5252;
                   padding:5px 16px; border-radius:6px; cursor:pointer; font-size:12px; font-weight:600;">
            ✗ Reject
          </button>
        </div>
      </div>`;
    }).join('');

  } catch(e) { /* silent */ }
  setTimeout(pollHitl, 5000);
}

function setHitlHighlight(jobId) {
  const rect = document.querySelector(`[data-node-rect="${jobId}"]`) ||
               document.querySelector(`[data-node-rect="DAG-${jobId}"]`);
  if (rect) {
    rect.setAttribute("stroke", "#ffb74d");
    rect.setAttribute("stroke-width", "2.5");
    rect.setAttribute("fill", "#1a1408");
  }
  const dot = document.querySelector(`[data-node-dot="${jobId}"]`) ||
              document.querySelector(`[data-node-dot="DAG-${jobId}"]`);
  if (dot) dot.setAttribute("fill", "#ffb74d");
}

function clearHitlHighlight(jobId) {
  const rect = document.querySelector(`[data-node-rect="${jobId}"]`) ||
               document.querySelector(`[data-node-rect="DAG-${jobId}"]`);
  if (rect) rect.setAttribute("stroke-width", "1.5");
}

async function hitlDecide(jobId, decision, btn) {
  btn.disabled = true;
  btn.textContent = "⏳ Sending…";
  try {
    const r = await fetch(`/api/hitl/${decision}/${encodeURIComponent(jobId)}`, { method: "POST" });
    const data = await r.json();
    if (r.ok && data.status === "ok") {
      btn.textContent = decision === "approve" ? "✓ Approved" : "✗ Rejected";
      // Banner will hide itself on next pollHitl tick
    } else {
      btn.textContent = "✗ Error";
      btn.disabled = false;
    }
  } catch {
    btn.textContent = "✗ No connection";
    btn.disabled = false;
  }
}

// Kick off HITL polling (slightly offset from dag_status to spread load)
setTimeout(pollHitl, 1500);

// ── Replay single job ─────────────────────────────────────────
async function replayJob(jobId) {
  const btn = document.getElementById("replay-btn");
  if (!btn) return;
  btn.disabled = true;
  btn.textContent = "⏳ Replaying…";
  btn.style.color = "#ffb74d";
  btn.style.borderColor = "#ffb74d66";
  try {
    const r = await fetch(`/api/dag/replay/${encodeURIComponent(jobId)}`, { method: "POST" });
    const data = await r.json();
    if (r.ok && data.status === "ok") {
      btn.textContent = "✓ Replayed";
      btn.style.color = "#4caf6e";
      btn.style.borderColor = "#4caf6e66";
      btn.style.background = "#0d2218";
      setTimeout(() => {
        btn.textContent = "↺ Replay";
        btn.style.color = "#aaa";
        btn.style.borderColor = "#2a2a2a";
        btn.style.background = "#0d0d14";
        btn.disabled = false;
      }, 3000);
    } else {
      btn.textContent = "✗ Failed";
      btn.style.color = "#ff5252";
      btn.style.borderColor = "#ff525266";
      setTimeout(() => {
        btn.textContent = "↺ Replay";
        btn.style.color = "#aaa";
        btn.style.borderColor = "#2a2a2a";
        btn.style.background = "#0d0d14";
        btn.disabled = false;
      }, 3000);
    }
  } catch {
    btn.textContent = "✗ No connection";
    btn.style.color = "#ff5252";
    setTimeout(() => {
      btn.textContent = "↺ Replay";
      btn.style.color = "#aaa";
      btn.disabled = false;
    }, 3000);
  }
}
</script>
</body>
"""

LOG_VIEW_HTML = SHARED_STYLE + """
<head><title>Titan Logs: {{ job_id }}</title></head>
<body>
<nav class="topnav">
  <span class="topnav-brand">⚡ TITAN</span>
  <a class="tab" href="/">🖥 Orchestrator</a>
  <a class="tab" href="/dags">🔀 DAG Pipelines</a>
  <a class="tab" href="/dags/new">✏️ Constructor</a>
</nav>
<div style="padding:20px; display:flex; flex-direction:column; height:calc(100vh - 89px);">
  <h2 style="margin-top:0; color:#64b5f6; border-bottom:1px solid #333; padding-bottom:10px; display:flex; justify-content:space-between; align-items:center;">
    <span>Log Stream: <span style="color:#fff">{{ job_id }}</span></span>
    <span id="status-badge" style="background:#333; font-size:.6em; padding:4px 10px; border-radius:4px;">CONNECTING...</span>
  </h2>
  <div id="log-container" style="background:#000; border:1px solid #333; flex-grow:1; overflow-y:scroll; padding:15px; border-radius:4px; white-space:pre-wrap; font-family:'Consolas',monospace; font-size:14px; line-height:1.4;"></div>
  <div style="margin-top:10px; text-align:right; display:flex; gap:8px; justify-content:flex-end;">
    <button onclick="window.history.back()" style="background:#333; color:white; border:1px solid #555; padding:5px 14px; cursor:pointer; border-radius:4px;">← Back</button>
    <button onclick="toggleScroll()" id="scrollBtn" style="background:#333; color:white; border:1px solid #555; padding:5px 14px; cursor:pointer; border-radius:4px;">Auto-Scroll: ON</button>
  </div>
</div>
<script>
  const jobId = "{{ job_id }}";
  const container = document.getElementById('log-container');
  let autoScroll = true;
  function toggleScroll() {
    autoScroll = !autoScroll;
    document.getElementById('scrollBtn').innerText = "Auto-Scroll: " + (autoScroll ? "ON" : "OFF");
  }
  async function fetchLogs() {
    try {
      const resp = await fetch(`/api/logs_raw/${jobId}`);
      if (resp.status !== 200) throw new Error();
      const text = await resp.text();
      document.getElementById('status-badge').innerText = "LIVE";
      document.getElementById('status-badge').style.background = "#00c853";
      if (container.innerText !== text) {
        container.innerText = text;
        if (autoScroll) container.scrollTop = container.scrollHeight;
      }
    } catch(e) {
      document.getElementById('status-badge').innerText = "DISCONNECTED";
      document.getElementById('status-badge').style.background = "#d32f2f";
    }
  }
  setInterval(fetchLogs, 1000);
  fetchLogs();
</script>
</body>
"""

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
                        "worker": None, "status": "WAITING", "time": ""
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

    return render_template_string(DASHBOARD_HTML, stats=stats,
                                  status_color=status_color, status_text=status_text)


@app.route('/logs/<job_id>')
def view_logs(job_id):
    return render_template_string(LOG_VIEW_HTML, job_id=job_id)


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
            # Normalise: map DEAD/UNKNOWN to FAILED so the UI makes sense
            if status in ("DEAD", "UNKNOWN", ""):
                status = "FAILED"
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

    return render_template_string(
        DAG_DASHBOARD_HTML,
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
        os.path.join(os.path.dirname(__file__)),
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

    dag_payload = ' ; '.join(job_strings)
    resp = titan_communicate(0x04, dag_payload)  # 0x04 = OP_SUBMIT_DAG

    if resp and 'ERROR' not in (resp or '').upper():
        # Write manifest so dashboard groups these jobs under the DAG name
        _write_constructor_manifest(dag_name, jobs_raw, dag_payload)
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
        for j in jobs_raw:
            full_id   = f"DAG-{j['id']}"
            full_deps = [f"DAG-{p}" for p in (j.get('depends_on') or [])]
            existing[full_id] = {
                "dag":      dag_name,
                "deps":     full_deps,
                "run_ts":   run_ts,
                "filename": os.path.basename(j.get('filename', '')),
                "req":      j.get('requirement', 'GENERAL'),
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
    statuses  = [v.get("status", "WAITING") for v in jobs_meta.values()]
    if not statuses:           return "PENDING"
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


if __name__ == '__main__':
    print("=" * 50)
    print("  Titan Dashboard")
    print("  http://127.0.0.1:5000          ← Orchestrator view")
    print("  http://127.0.0.1:5000/dags     ← DAG pipeline view")
    print("=" * 50)
    app.run(host='0.0.0.0', port=5000, debug=False)
