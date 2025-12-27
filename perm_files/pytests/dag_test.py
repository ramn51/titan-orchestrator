import socket
import struct

# CONFIG
HOST = "127.0.0.1"
PORT = 9090
OP_SUBMIT_DAG = 4
VERSION = 1

def submit_dag():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))

        # DAG FORMAT: "ID|SKILL|DATA|PRIORITY|DELAY|[PARENTS] ; ..."
        # We add 'SLEEP' to the data so you have time to see it running.

        dag_payload = (
            "ROOT|TEST|TASK_A_SLEEP|2|0|[] ; "             # Runs First
            "LEFT|TEST|TASK_B_SLEEP|1|0|[ROOT] ; "         # Runs after Root
            "RIGHT|TEST|TASK_C_SLEEP|1|0|[ROOT] ; "        # Runs after Root (Parallel with Left)
            "FINAL|TEST|TASK_D_SLEEP|1|0|[LEFT,RIGHT]"     # Runs last
        )

        payload_bytes = dag_payload.encode('utf-8')

        # Header: Ver(1) | Op(4) | Flags(0) | Spare(0) | Length
        header = struct.pack('>BBBBI', VERSION, OP_SUBMIT_DAG, 0, 0, len(payload_bytes))

        s.sendall(header + payload_bytes)

        # Read Ack
        resp_header = s.recv(8)
        s.close()
        print(f"[OK] Submitted DAG: Root -> (Left, Right) -> Final")
    except Exception as e:
        print(f"[FAIL] {e}")

if __name__ == "__main__":
    submit_dag()
    print("✅ DAG Sent. Watch the workers coordinate!")