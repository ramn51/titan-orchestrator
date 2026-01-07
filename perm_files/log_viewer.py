from flask import Flask, render_template_string, request
import socket
import struct
import sys

app = Flask(__name__)

# CONFIG
MASTER_IP = "127.0.0.1"
MASTER_PORT = 9090
OP_GET_LOGS = 0x16

def fetch_logs_from_titan(job_id):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((MASTER_IP, MASTER_PORT))

            # Protocol: Version(1)|Op(16)|Flags|Spare|Length
            payload = job_id.encode('utf-8')
            header = struct.pack('>BBBBI', 1, OP_GET_LOGS, 0, 0, len(payload))
            s.sendall(header + payload)

            # Response: Op|Flags|Spare|Spare|Length
            resp_header = s.recv(8)
            if not resp_header: return "No Response from Master"

            _, _, _, _, length = struct.unpack('>BBBBI', resp_header)

            # Read the actual log data
            data = b""
            while len(data) < length:
                chunk = s.recv(min(4096, length - len(data)))
                if not chunk: break
                data += chunk

            return data.decode('utf-8')
    except Exception as e:
        return f"Connection Error: {e}"

# Single-File HTML Template
HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Titan Orchestrator - Live Logs</title>
    <style>
        body { background-color: #121212; color: #e0e0e0; font-family: 'Consolas', monospace; padding: 20px; }
        .container { max-width: 900px; margin: 0 auto; }
        .controls { display: flex; gap: 10px; margin-bottom: 20px; }
        input { background: #333; border: 1px solid #555; color: white; padding: 10px; flex-grow: 1; border-radius: 4px;}
        button { background: #007acc; border: none; color: white; padding: 10px 20px; cursor: pointer; border-radius: 4px;}
        button:hover { background: #005f9e; }

        #log-window {
            background: #1e1e1e;
            border: 1px solid #333;
            height: 500px;
            overflow-y: auto;
            padding: 15px;
            white-space: pre-wrap;
            border-radius: 4px;
            box-shadow: 0 4px 10px rgba(0,0,0,0.5);
        }
        .log-line { border-bottom: 1px solid #2a2a2a; padding: 2px 0; }
        .log-line:last-child { border-bottom: none; }
    </style>
</head>
<body>
    <div class="container">
        <h2>🚀 Titan Orchestrator Logs</h2>
        <div class="controls">
            <input type="text" id="jobIdInput" placeholder="Enter Job ID (e.g., svc_12345)" value="svc_">
            <button onclick="togglePolling()" id="btn-poll">Start Watch</button>
        </div>
        <div id="log-window">Waiting for input...</div>
    </div>

    <script>
        let polling = false;
        let intervalId = null;

        async function fetchLogs() {
            const jobId = document.getElementById('jobIdInput').value;
            if(!jobId) return;

            try {
                const res = await fetch(`/api/logs?id=${jobId}`);
                const text = await res.text();
                const win = document.getElementById('log-window');

                // Only scroll down if we were already near the bottom
                const isAtBottom = win.scrollHeight - win.scrollTop <= win.clientHeight + 50;

                win.innerText = text; // Update logs

                if(isAtBottom) win.scrollTop = win.scrollHeight;
            } catch(e) {
                console.error("Fetch failed", e);
            }
        }

        function togglePolling() {
            const btn = document.getElementById('btn-poll');
            if (polling) {
                clearInterval(intervalId);
                polling = false;
                btn.innerText = "Start Watch";
                btn.style.background = "#007acc";
            } else {
                fetchLogs(); // Immediate fetch
                intervalId = setInterval(fetchLogs, 1000); // Poll every 1s
                polling = true;
                btn.innerText = "Stop Watch";
                btn.style.background = "#c0392b";
            }
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML)

@app.route('/api/logs')
def get_logs():
    job_id = request.args.get('id')
    return fetch_logs_from_titan(job_id)

if __name__ == "__main__":
    # DEFAULT to 9991 if no argument provided
        port = 9991

        # Check if Titan sent a port argument (sys.argv[1])
        if len(sys.argv) > 1:
            try:
                port = int(sys.argv[1])
            except ValueError:
                print(f"Invalid port argument: {sys.argv[1]}, defaulting to {port}")

        print(f"Starting Log Viewer on port {port}...")

        # IMPORTANT: Listen on all interfaces '0.0.0.0' so Titan can check connectivity
        app.run(host='0.0.0.0', port=port)