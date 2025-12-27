import socket
import struct
import json
import time

# CONFIG
HOST = "127.0.0.1"
PORT = 9090
OP_STATS_JSON = 0x09
VERSION = 1

def recv_all(sock, n):
    """Helper to ensure we read exactly n bytes from the stream."""
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None # Connection closed
        data += packet
    return data

def debug_stats():
    print(f"🔌 Connecting to {HOST}:{PORT}...")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((HOST, PORT))
        print("✅ Connected.")

        # 1. SEND REQUEST
        # Header: Ver(1) | Op(9) | Flags(0) | Spare(0) | Length(0)
        header = struct.pack('>BBBBI', VERSION, OP_STATS_JSON, 0, 0, 0)
        s.sendall(header)
        print("📤 Sent Request Header (8 bytes). Waiting for response...")

        # 2. READ HEADER (Using recv_all fix)
        resp_header = recv_all(s, 8)

        if not resp_header:
            print("❌ [FAIL] Server closed connection (0 bytes).")
            return

        ver, op, flags, spare, length = struct.unpack('>BBBBI', resp_header)
        print(f"📥 Received Header -> Ver:{ver} | Op:{op} | Length:{length} bytes")

        # 3. READ BODY
        raw_body = b''
        if length > 0:
            raw_body = recv_all(s, length)
            if not raw_body:
                print("❌ [FAIL] Connection closed while reading body.")
                return

        print(f"✅ Body Read Complete ({len(raw_body)} bytes).")

        # 4. PRINT JSON
        if length > 0:
            decoded_body = raw_body.decode('utf-8')
            try:
                json_obj = json.loads(decoded_body)
                print("\n📊 JSON STATS:")
                print(json.dumps(json_obj, indent=2))
            except json.JSONDecodeError:
                print(f"⚠️ Raw Body (Not JSON): {decoded_body}")

        s.close()

    except Exception as e:
        print(f"❌ [CRITICAL ERROR] {e}")

if __name__ == "__main__":
    debug_stats()