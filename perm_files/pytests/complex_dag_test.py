import socket
import struct
import base64
import time

# CONFIG
HOST = "127.0.0.1"
PORT = 9090
VERSION = 1
OP_SUBMIT_DAG = 4

import os

# ... imports ...

def get_base64_payloads():
    # Define paths relative to where you run this script
    # Assuming send_dag.py is in perm_files/pytests/ and calc.py is in perm_files/
    base_dir = os.path.dirname(os.path.abspath(__file__)) # perm_files/pytests
    parent_dir = os.path.dirname(base_dir)                # perm_files/

    server_path = os.path.join(parent_dir, "deploy_test_svc.py")
    calc_path = os.path.join(parent_dir, "calc.py")
    worker_path = os.path.join(parent_dir, "Worker.jar")

    # Read and encode actual files
    try:
        with open(server_path, 'r') as f:
            server_b64 = base64.b64encode(f.read().encode('utf-8')).decode('utf-8')

        with open(calc_path, 'r') as f:
            calc_b64 = base64.b64encode(f.read().encode('utf-8')).decode('utf-8')

        with open(calc_path, 'r') as f:
            worker_b64 = base64.b64encode(f.read().encode('utf-8')).decode('utf-8')

        print(f"[INFO] Loaded {calc_path} and {server_path}")
        return server_b64, calc_b64, worker_b64

    except FileNotFoundError as e:
        print(f"❌ Could not find file: {e}")
        # Fallback to dummy data so script doesn't crash during test
        return "ZHVtbXk=", "ZHVtbXk=", "ZHVtbXk="

def send_dag():
    server_b64, calc_b64, worker_b64 = get_base64_payloads()

    print(f"🚀 Constructing Integration DAG...")

    # DAG STRUCTURE:
    # 1. JOB_CALC   (Run calc.py)
    # 2. JOB_DEPLOY (Deploy UDP Server) -> Depends on CALC
    # 3. JOB_PDF    (Convert PDF)       -> Depends on DEPLOY

    dag = (
        # JOB 1: Run Calc
        # Payload: RUN_PAYLOAD | filename | b64_content
        f"JOB_SPAWN|GENERAL|DEPLOY_PAYLOAD|Worker.jar|{worker_b64}|8086|2|0|[] ; "

        f"JOB_CALC|GENERAL|RUN_PAYLOAD|calc.py|{calc_b64}|2|0|[] ; "

        # JOB 2: Deploy New Service
        # Payload: DEPLOY_PAYLOAD | filename | b64_content | port
        f"JOB_DEPLOY|GENERAL|DEPLOY_PAYLOAD|deploy_test_svc.py|{server_b64}|9991|2|0|[JOB_CALC] ; "

        # JOB 3: PDF Convert (Standard Task)
        # Payload: PDF_CONVERT | filename
        f"JOB_PDF|GENERAL|PDF_CONVERT|final_report.pdf|1|0|[JOB_DEPLOY]"
    )

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))

        payload_bytes = dag.encode('utf-8')
        # Header: Ver | Op | Flags | Spare | Length
        header = struct.pack('>BBBBI', VERSION, OP_SUBMIT_DAG, 0, 0, len(payload_bytes))

        s.sendall(header + payload_bytes)

        # Read Ack
        s.settimeout(5)
        resp_header = s.recv(8)
        if resp_header:
            print(f"✅ DAG Submitted Successfully!")
            print("   Structure: CALC -> DEPLOY -> PDF")
            print("   Monitor the dashboard to see them execute in order.")
        else:
            print(f"❌ Submission Failed.")
        s.close()
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    send_dag()