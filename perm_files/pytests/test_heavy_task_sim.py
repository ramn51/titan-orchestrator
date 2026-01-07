import socket
import struct
import base64
import os

# CONFIG
HOST = "127.0.0.1"
PORT = 9090
OP_SUBMIT_JOB = 3

def get_heavy_task_content():
    # Helper to find the file safely
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    heavy_task_path = os.path.join(parent_dir, "pytests", "heavy_task.py")

    if not os.path.exists(heavy_task_path):
        print(f"⚠️ Warning: {heavy_task_path} not found! Using dummy content.")
        return "print('Hello from Titan')"

    with open(heavy_task_path, "r") as f:
        return f.read()

    # Fallback checks
    if not os.path.exists(heavy_task_path):
        heavy_task_path = "perm_files/heavy_task.py"

    if not os.path.exists(heavy_task_path):
        print(f"❌ Error: Could not find {heavy_task_path}")
        return None

    with open(heavy_task_path, "r") as f:
        return f.read()

def submit():
    raw_content = get_heavy_task_content()
    if not raw_content: return

    # --- THE FIX IS HERE ---
    # 1. Encode raw string to bytes
    # 2. Base64 encode those bytes
    # 3. Decode back to UTF-8 string so we can put it in the payload
    b64_content = base64.b64encode(raw_content.encode('utf-8')).decode('utf-8')

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))

    # Format: RUN_PAYLOAD | filename | base64_content | priority | delay
    # Note: We must encode the final message to bytes before sending
    payload_str = f"RUN_PAYLOAD|heavy_task.py|{b64_content}|5|0"
    payload_bytes = payload_str.encode('utf-8')

    # Header: Ver | Op | Flags | Spare | Length
    header = struct.pack('>BBBBI', 1, OP_SUBMIT_JOB, 0, 0, len(payload_bytes))

    s.sendall(header + payload_bytes)

    # Check for Ack
    try:
        s.settimeout(2)
        resp = s.recv(1024)
        print("✅ Heavy Job Submitted! Check Dashboard.")
    except:
        print("⚠️  Sent, but no ACK received.")

    s.close()

if __name__ == "__main__":
    submit()