#  Copyright 2026 Ram Narayanan
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import socket
import struct
import json
import time
import os
from pathlib import Path
from dag_viewer.routes import create_blueprint
from cluster_monitor.routes import create_blueprint as create_cluster_blueprint
from flask import Flask, render_template_string, request, url_for

app = Flask(__name__)

# --- CONFIGURATION ---
SCHEDULER_HOST = "127.0.0.1"
SCHEDULER_PORT = 9090

# --- TITAN PROTOCOL CONSTANTS ---
CURRENT_VERSION = 1
OP_STATS_JSON = 0x09
OP_GET_LOGS = 0x16  # Matches your Java Master's OpCode

def recv_all(sock, n):
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet: return None
        data += packet
    return data

def titan_communicate(op_code, payload_str="", retries=3):
    attempt = 0
    while attempt < retries:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((SCHEDULER_HOST, SCHEDULER_PORT))

            body_bytes = payload_str.encode('utf-8')
            length = len(body_bytes)
            header = struct.pack('>BBBBI', CURRENT_VERSION, op_code, 0, 0, length)
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
                return None # Fail silently
    return None

# --- HTML TEMPLATES ---

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Titan Dashboard</title>
    <meta http-equiv="refresh" content="2">
    <style>
        body { background-color: #121212; color: #e0e0e0; font-family: 'Segoe UI', sans-serif; margin: 0; }
        a { text-decoration: none; color: inherit; }
        a:hover { text-decoration: underline; color: #64b5f6; }

        .card { background: #1e1e1e; border: 1px solid #333; border-radius: 8px; padding: 20px; width: 320px; box-shadow: 0 4px 6px rgba(0,0,0,0.5); display: flex; flex-direction: column; }
        .grid { display: flex; flex-wrap: wrap; gap: 20px; justify-content: center; padding: 20px; }

        .section-title { font-size: 0.75em; text-transform: uppercase; letter-spacing: 1px; color: #757575; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #333; padding-bottom: 2px;}
        .service-tag { background: #263238; padding: 5px 8px; margin-top: 5px; border-radius: 4px; border-left: 3px solid #0288d1; font-family: monospace; font-size: 0.9em; cursor: pointer; transition: background 0.2s; }
        .service-tag:hover { background: #37474f; }

        .hist-list { list-style: none; padding: 0; margin: 0; }
        .hist-item { display: flex; justify-content: space-between; align-items: center; font-size: 0.85em; padding: 6px 0; border-bottom: 1px solid #2c2c2c; }
        .hist-item:last-child { border-bottom: none; }

        .hist-id { color: #90caf9; font-family: monospace; }
        .hist-meta { text-align: right; }
        .hist-time { display: block; font-size: 0.8em; color: #757575; }

        /* Skill Badge Style */
        .skill-badge { font-size: 0.7em; background: #424242; padding: 2px 6px; border-radius: 4px; color: #bbb; margin-left: 8px; vertical-align: middle; border: 1px solid #555;}

        .status-badge { font-weight: bold; font-size: 0.85em; padding: 2px 6px; border-radius: 4px; }
        .st-COMPLETED { color: #00e676; background: rgba(0, 230, 118, 0.1); }
        .st-FAILED { color: #ff5252; background: rgba(255, 82, 82, 0.1); }
        .st-RUNNING { color: #ffb74d; background: rgba(255, 183, 77, 0.1); }
        .st-DEAD { color: #9e9e9e; background: rgba(158, 158, 158, 0.1); border: 1px solid #555; }

        @keyframes pulse-yellow {
            0% { opacity: 1; box-shadow: 0 0 0 0 rgba(255, 183, 77, 0.7); }
            70% { opacity: 0.8; box-shadow: 0 0 0 6px rgba(255, 183, 77, 0); }
            100% { opacity: 1; box-shadow: 0 0 0 0 rgba(255, 183, 77, 0); }
        }
    </style>
    <link rel="stylesheet" href="{{ url_for('static', filename='section_nav.css', v='1') }}">
</head>
<body>
    {% with active_section='orchestrator', nav_status=status_text ~ ' · Workers: ' ~ stats.active_workers ~ ' · Queue: ' ~ stats.queue_size %}{% include 'section_nav.html' %}{% endwith %}

    <div class="grid">
        {% for w in stats.workers %}
        <div class="card" style="border-top: 4px solid {% if 'GPU' in w.capabilities %}#7c4dff{% else %}#4CAF50{% endif %};">

            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div>
                    <h3 style="margin:0; display:inline;">Node :{{ w.port }}</h3>
                    <span class="skill-badge">{{ w.capabilities }}</span>
                </div>
                <span style="background:#004d40; color:#00e676; padding:2px 8px; border-radius:4px; font-size:0.8em;">Load: {{ w.load }}</span>
            </div>

            <div class="section-title">Current Status</div>
            {% if w.active_job %}
                <div style="background: #2e2010; border: 1px solid #f57c00; padding: 12px; border-radius: 6px; display: flex; align-items: center; gap: 12px; margin-top: 5px;">
                    <div style="width: 12px; height: 12px; background: #ffb74d; border-radius: 50%; animation: pulse-yellow 1.5s infinite;"></div>
                    <div style="flex-grow: 1;">
                        <div style="font-size: 0.7em; color: #ffcc80; letter-spacing: 0.5px; font-weight: bold;">EXECUTING NOW</div>
                        <div style="font-family: monospace; font-size: 1.1em; color: #fff;">
                            <a href="/logs/{{ w.active_job }}" target="_blank">📄 {{ w.active_job }}</a>
                        </div>
                    </div>
                </div>
            {% else %}
                <div style="background: #1b2e23; border: 1px solid #2e7d32; padding: 8px; border-radius: 6px; color: #81c784; font-size: 0.85em; text-align: center; margin-top: 5px;">
                    ● Worker Idle
                </div>
            {% endif %}

            <div class="section-title">Running Services</div>
            <div>
                {% for svc in w.services %}
                    <a href="/logs/{{ svc }}" target="_blank">
                        <div class="service-tag">⚙️ {{ svc }}</div>
                    </a>
                {% else %}
                    <div style="color:#555; font-style:italic; font-size:0.9em; padding: 5px 0;">• No active services</div>
                {% endfor %}
            </div>

            <div class="section-title">Recent Activity</div>
            <ul class="hist-list">
                {% for job in w.history %}
                    <li class="hist-item">
                        <span class="hist-id">
                             <a href="/logs/{{ job.id }}" target="_blank">{{ job.id }}</a>
                        </span>
                        <div class="hist-meta">
                            <span class="status-badge st-{{ job.status }}">{{ job.status }}</span>
                            <span class="hist-time">{{ job.time }}</span>
                        </div>
                    </li>
                {% else %}
                    <li style="color:#555; font-style:italic; font-size:0.9em; padding: 5px 0;">• No history yet</li>
                {% endfor %}
            </ul>

        </div>
        {% endfor %}
    </div>
</body>
</html>
"""

LOG_VIEW_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <title>Titan Logs: {{ job_id }}</title>
    <style>
        body { background: #1e1e1e; color: #d4d4d4; font-family: 'Consolas', monospace; padding: 20px; display: flex; flex-direction: column; height: 95vh; margin: 0;}
        h2 { margin-top: 0; color: #64b5f6; border-bottom: 1px solid #333; padding-bottom: 10px; display: flex; justify-content: space-between; align-items: center;}
        #log-container {
            background: #000;
            border: 1px solid #333;
            flex-grow: 1;
            overflow-y: scroll;
            padding: 15px;
            border-radius: 4px;
            white-space: pre-wrap;
            font-size: 14px;
            line-height: 1.4;
        }
        .controls { margin-top: 10px; text-align: right; }
        button { background: #333; color: white; border: 1px solid #555; padding: 5px 15px; cursor: pointer; }
        button:hover { background: #444; }
        .badge { background: #333; font-size: 0.6em; padding: 4px 8px; border-radius: 4px; vertical-align: middle; color: #fff; }
    </style>
</head>
<body>
    <h2>
        <span>Log Stream: <span style="color:white">{{ job_id }}</span></span>
        <span id="status-badge" class="badge">CONNECTING...</span>
    </h2>

    <div id="log-container">Loading logs...</div>

    <div class="controls">
        <button onclick="window.close()">Close</button>
        <button onclick="toggleScroll()" id="scrollBtn">Auto-Scroll: ON</button>
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
                if(resp.status !== 200) throw new Error("Server Error");

                const text = await resp.text();
                document.getElementById('status-badge').innerText = "LIVE";
                document.getElementById('status-badge').style.background = "#00c853";

                const wasAtBottom = container.scrollTop + container.clientHeight >= container.scrollHeight - 50;

                // Only update if text changed to avoid flicker
                if (container.innerText !== text) {
                    container.innerText = text;
                    if(autoScroll) container.scrollTop = container.scrollHeight;
                }
            } catch(e) {
                document.getElementById('status-badge').innerText = "DISCONNECTED";
                document.getElementById('status-badge').style.background = "#d32f2f";
            }
        }

        setInterval(fetchLogs, 1000); // Poll every second
        fetchLogs();
    </script>
</body>
</html>
"""

# --- ROUTES ---

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
        status_color = "#f44336"
        status_text = "OFFLINE"
    else:
        status_color = "#00e676"
        status_text = "ONLINE"

    return render_template_string(DASHBOARD_HTML, stats=stats, status_color=status_color, status_text=status_text)

@app.route('/logs/<job_id>')
def view_logs(job_id):
    # Just render the container; the JS inside will poll the API
    return render_template_string(LOG_VIEW_HTML, job_id=job_id)

@app.route('/api/logs_raw/<job_id>')
def get_raw_logs(job_id):
    # Communicate with Java Master to get log string
    logs = titan_communicate(OP_GET_LOGS, job_id)
    if logs is None:
        return "Error fetching logs", 500
    return logs if logs else "[Titan] No logs found for this ID yet."

app.register_blueprint(create_blueprint(
    titan_communicate, OP_STATS_JSON, OP_GET_LOGS,
    Path(os.environ.get("TITAN_DAG_MANIFEST", ".titan_dag_manifest.json")),
    Path(os.environ.get("TITAN_DAG_YAML_DIR", ".")),
    scripts_root=Path(os.environ.get("TITAN_DAG_SCRIPT_DIR",
        Path(__file__).resolve().parent / "dag_samples")),
))
app.register_blueprint(create_cluster_blueprint(
    titan_communicate, Path(os.environ.get("TITAN_DAG_MANIFEST", ".titan_dag_manifest.json"))
))

if __name__ == '__main__':
    print(f"Starting Dashboard on http://127.0.0.1:5000")
    app.run(host='127.0.0.1', port=5000)
