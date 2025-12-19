import socket
import struct
import json
import sys
from flask import Flask, render_template_string

app = Flask(__name__)

# --- DYNAMIC CONFIGURATION ---
# If running on a worker, 'localhost' won't work.
# We should ideally use the actual IP of your Master machine.
SCHEDULER_HOST = "127.0.0.1" # Change this to your Master's IP if deploying remotely
SCHEDULER_PORT = 9090

def titan_communicate(command):
    print(f"📡 Attempting to contact Scheduler at {SCHEDULER_HOST}:{SCHEDULER_PORT}...")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((SCHEDULER_HOST, SCHEDULER_PORT))

            # --- SEND ---
            payload = command.encode('utf-8')
            header = struct.pack('>I', len(payload))
            s.sendall(header + payload)

            # --- READ ---
            raw_len = s.recv(4)
            if not raw_len:
                print("⚠️ Received no data from Scheduler.")
                return None

            msg_len = struct.unpack('>I', raw_len)[0]
            chunks = []
            bytes_recd = 0
            while bytes_recd < msg_len:
                chunk = s.recv(min(msg_len - bytes_recd, 4096))
                if not chunk: break
                chunks.append(chunk)
                bytes_recd += len(chunk)

            response = b"".join(chunks).decode('utf-8')
            print(f"✅ Successfully received {len(response)} bytes.")
            return response

    except Exception as e:
        print(f"❌ Communication Error: {e}")
        return None

@app.route('/')
def index():
    raw_json = titan_communicate("STATS_JSON")
    stats = None

    if raw_json:
        try:
            # 1. FIND THE START OF JSON (Look for the first curly brace)
            json_start = raw_json.find('{')

            if json_start != -1:
                # 2. Slice the string to get only the JSON part
                clean_json_str = raw_json[json_start:]
                stats = json.loads(clean_json_str)
            else:
                print(f"⚠️ Response contained no JSON: {raw_json}")

        except json.JSONDecodeError as e:
            print(f"❌ JSON Parse Error: {e}")
            print(f"   Raw received: {raw_json}")

    if not stats:
        # Pass a dummy object or error message so the page renders even if empty
        return "<h1 style='color:red;'>⚠️ Dashboard Offline or Bad Data</h1><p>Check console logs for 'Raw received'</p>"

    return render_template_string("""
    <body style="background:#0f0f0f; color:#eee; font-family:sans-serif; padding:40px;">
        <h1 style="color:#4CAF50; text-align:center;">🛰️ TITAN CLUSTER STATUS</h1>

        <div style="text-align:center; margin-bottom:30px;">
            <form action="/scale" method="post">
                <button type="submit" style="background:#2e7d32; color:white; border:none; padding:12px 24px; border-radius:4px; cursor:pointer; font-weight:bold;">
                    🚀 SCALE UP (+3 WORKERS)
                </button>
            </form>
        </div>

        <div style="display:grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap:20px;">
            {% for w in stats.workers %}
            <div style="background:#1a1a1a; border:1px solid #333; border-radius:8px; padding:20px; border-top: 4px solid #4CAF50;">
                <h3 style="margin:0;">Worker Node: {{ w.port }}</h3>
                <p style="color:#888;">Load: {{ w.load }}</p>
                <div style="margin-top:15px;">
                    {% for svc in w.services %}
                    <div style="background:#222; margin-top:8px; padding:8px; border-radius:4px; border-left:3px solid #2196F3; font-size:0.85em;">
                        {{ svc[:25] }}...
                    </div>
                    {% else %}
                    <p style="font-size:0.8em; color:#555;">No active sub-services</p>
                    {% endfor %}
                </div>
            </div>
            {% endfor %}
        </div>
    </body>
    """, stats=stats)

@app.route('/scale', methods=['POST'])
def scale():
    titan_communicate("SCALE_UP")
    return "OK", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)