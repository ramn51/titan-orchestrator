import socket
import struct
import time
import os
import base64

# CONFIG
HOST = "127.0.0.1"
PORT = 9090
OP_SUBMIT_JOB = 3
VERSION = 1

def get_calc_content():
    # Go up one level from the current script to find calc.py
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    calc_path = os.path.join(parent_dir, "calc.py")

    if not os.path.exists(calc_path):
        print(f"⚠️ Warning: {calc_path} not found! Using dummy content.")
        return "print('Hello from Titan')"

    with open(calc_path, "r") as f:
        return f.read()

def submit_job(job_id, script_content):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((HOST, PORT))

        # We encode the script in Base64 so special characters
        # or newlines don't break our '|' pipe delimiters
        encoded_script = base64.b64encode(script_content.encode()).decode()

        # Format matches your handleDeploy logic:
        # DEPLOY_PAYLOAD|filename|base64data|port(optional/0)
        payload = f"DEPLOY_PAYLOAD|calc.py|{encoded_script}|0"
        payload_bytes = payload.encode('utf-8')

        # Header: Ver(1) | Op(3) | Flags(0) | Spare(0) | Length
        header = struct.pack('>BBBBI', VERSION, OP_SUBMIT_JOB, 0, 0, len(payload_bytes))

        s.sendall(header + payload_bytes)

        # Basic wait for acknowledgment
        resp_header = s.recv(8)
        s.close()
        print(f"[OK] Job {job_id}: Sent calc.py ({len(payload_bytes)} bytes)")
    except Exception as e:
        print(f"[FAIL] Job {job_id}: {e}")

if __name__ == "__main__":
    content = get_calc_content()
    print(f"🌊 Flooding {HOST}:{PORT} with 10 instances of calc.py...")

    for i in range(1, 11):
        submit_job(i, content)
        time.sleep(0.05)

    print("\n✅ Done. Check your Dashboard to see the workers balancing these 10 tasks!")