import socket
import struct
import base64
import os
import time

# CONFIG
HOST = "127.0.0.1"
PORT = 9090
VERSION = 1
OP_SUBMIT_DAG = 4

def get_b64_content(filepath):
    """Reads a file in binary mode and returns its Base64 string."""
    if not os.path.exists(filepath):
        print(f"❌ ERROR: File not found: {filepath}")
        # Return a safe dummy (Valid Base64) to prevent Worker crash
        return "UEsDBA=="

    with open(filepath, 'rb') as f:
        return base64.b64encode(f.read()).decode('utf-8')

def send_dag():
    # --- PATH FINDER LOGIC ---
    # Get the directory where THIS script is running
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # We need to find the folder containing "Worker.jar"
    # Search Order:
    # 1. Current Folder
    # 2. Parent Folder (../)
    # 3. Grandparent Folder (../../)
    # 4. Hardcoded Fallback

    search_dirs = [
        script_dir,
        os.path.abspath(os.path.join(script_dir, "..")),
        os.path.abspath(os.path.join(script_dir, "..", "..")),
        r"C:\Users\ASUS\IdeaProjects\DistributedOrchestrator\perm_files"
    ]

    resolved_dir = script_dir
    found = False

    for d in search_dirs:
        test_path = os.path.join(d, "Worker.jar")
        if os.path.exists(test_path):
            resolved_dir = d
            found = True
            break

    if found:
        print(f"📂 Resolved Artifact Directory: {resolved_dir}")
    else:
        print(f"⚠️ WARNING: Could not auto-locate files. Defaulting to: {resolved_dir}")

    # ----------------

    # 2. File Names
    worker_jar = os.path.join(resolved_dir, "Worker.jar")
    calc_py    = os.path.join(resolved_dir, "pytests", "calc.py")
    server_py  = os.path.join(resolved_dir, "pytests", "log_viewer.py")

    # 3. Load Payloads
    worker_b64 = get_b64_content(worker_jar)
    calc_b64   = get_b64_content(calc_py)
    server_b64 = get_b64_content(server_py)

    print(f"📦 Payload Sizes: Worker={len(worker_b64)}b, Calc={len(calc_b64)}b, Svc={len(server_b64)}b")

    # 4. Construct DAG
    print(f"🚀 Constructing Integration DAG...")

    dag_plan = (
        f"JOB_SPAWN|GENERAL|DEPLOY_PAYLOAD|Worker.jar|{worker_b64}|8086|2|0|[] ; "
        f"JOB_CALC|GENERAL|RUN_PAYLOAD|calc.py|{calc_b64}|2|0|[] ; "
        f"JOB_DEPLOY|GENERAL|DEPLOY_PAYLOAD|log_viewer.py|{server_b64}|9991|2|0|[JOB_CALC] ; "
        f"JOB_PDF|GENERAL|PDF_CONVERT|final_report.pdf|1|0|[JOB_DEPLOY]"
    )

    # 5. Send to Scheduler
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))
        payload_bytes = dag_plan.encode('utf-8')
        header = struct.pack('>BBBBI', VERSION, OP_SUBMIT_DAG, 0, 0, len(payload_bytes))
        s.sendall(header + payload_bytes)

        # Read Ack
        s.settimeout(5)
        if s.recv(8):
            print(f"✅ DAG Submitted!")
        else:
            print(f"⚠️ Server closed connection.")
        s.close()
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    send_dag()