import socket
import struct
import json
import time
from flask import Flask, render_template_string

app = Flask(__name__)

# --- CONFIGURATION ---
SCHEDULER_HOST = "127.0.0.1"
SCHEDULER_PORT = 9090

# --- TITAN PROTOCOL CONSTANTS ---
CURRENT_VERSION = 1
OP_STATS_JSON = 0x09

# Add this helper at the top
def recv_all(sock, n):
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet: return None
        data += packet
    return data

def titan_communicate(op_code, payload_str=""):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((SCHEDULER_HOST, SCHEDULER_PORT))

        # --- SEND ---
        body_bytes = payload_str.encode('utf-8')
        length = len(body_bytes)
        header = struct.pack('>BBBBI', CURRENT_VERSION, op_code, 0, 0, length)
        s.sendall(header + body_bytes)

        # --- READ HEADER (FIXED) ---
        raw_header = recv_all(s, 8)
        if not raw_header:
            s.close()
            return None

        ver, resp_op, flags, spare, resp_len = struct.unpack('>BBBBI', raw_header)

        # --- READ BODY (FIXED) ---
        response_payload = ""
        if resp_len > 0:
            raw_body = recv_all(s, resp_len)
            if raw_body:
                response_payload = raw_body.decode('utf-8')

        s.close()
        return response_payload

    except Exception as e:
        print(f"[FAIL] Communication Error: {e}")
        return None

@app.route('/')
def index():
    # Fetch Data from Scheduler
    raw_json = titan_communicate(OP_STATS_JSON, "")
    stats = None

    if raw_json:
        try:
            json_start = raw_json.find('{')
            if json_start != -1:
                stats = json.loads(raw_json[json_start:])
        except json.JSONDecodeError:
            pass

    # Default Data if Offline
    if not stats:
        stats = {"active_workers": 0, "queue_size": 0, "workers": []}
        status_color = "#f44336" # Red
        status_text = "OFFLINE"
    else:
        status_color = "#00e676" # Green
        status_text = "ONLINE"

    # HTML TEMPLATE
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Titan Dashboard</title>
        <meta http-equiv="refresh" content="2"> <style>
            body { background-color: #121212; color: #e0e0e0; font-family: 'Segoe UI', sans-serif; padding: 20px; }
            .header { text-align: center; margin-bottom: 30px; }
            .status-dot { height: 12px; width: 12px; background-color: {{ status_color }}; border-radius: 50%; display: inline-block; }
            .card { background: #1e1e1e; border: 1px solid #333; border-radius: 8px; padding: 20px; width: 300px; box-shadow: 0 4px 6px rgba(0,0,0,0.5); }
            .grid { display: flex; flex-wrap: wrap; gap: 20px; justify-content: center; }
            .service-tag { background: #263238; padding: 5px 8px; margin-top: 5px; border-radius: 4px; border-left: 3px solid #0288d1; font-family: monospace; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1 style="letter-spacing: 2px;">🛰️ TITAN ORCHESTRATOR</h1>
            <div>
                <span class="status-dot"></span>
                <span style="font-weight:bold; color:{{ status_color }}">{{ status_text }}</span>
                &nbsp;|&nbsp; Workers: {{ stats.active_workers }} &nbsp;|&nbsp; Queue: {{ stats.queue_size }}
            </div>
        </div>

        <div class="grid">
            {% for w in stats.workers %}
            <div class="card" style="border-top: 4px solid #4CAF50;">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <h3 style="margin:0;">Node :{{ w.port }}</h3>
                    <span style="background:#004d40; color:#00e676; padding:2px 8px; border-radius:4px; font-size:0.8em;">Load: {{ w.load }}</span>
                </div>
                <hr style="border:0; border-top:1px solid #333; margin:15px 0;">
                <div style="font-size:0.9em; color:#bbb;">
                    {% for svc in w.services %}
                        <div class="service-tag">{{ svc }}</div>
                    {% else %}
                        <div style="color:#666; font-style:italic;">• Idle</div>
                    {% endfor %}
                </div>
            </div>
            {% endfor %}
        </div>
    </body>
    </html>
    """, stats=stats, status_color=status_color, status_text=status_text)

if __name__ == '__main__':
    print(f"Starting Dashboard on http://127.0.0.1:5000")
    app.run(host='0.0.0.0', port=5000)