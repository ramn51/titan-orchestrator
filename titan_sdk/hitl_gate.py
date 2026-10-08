"""Worker-side approval checkpoint for SDK-injected Titan DAG gates.

No third-party package or local SDK installation is required on the worker.
The script speaks Titan's existing KV protocol to the master.
"""

import base64
import os
import socket
import struct
import sys
import time

KV_SET = 0x60
KV_GET = 0x61
KV_SADD = 0x62
PREFIX = "titan:hitl:"


class TitanStore:
    def __init__(self, host=None, port=None):
        self.host = host or os.environ.get("TITAN_HOST", "127.0.0.1")
        self.port = int(port or os.environ.get("TITAN_PORT", "9090"))

    def request(self, opcode, payload):
        data = payload.encode("utf-8")
        with socket.create_connection((self.host, self.port), timeout=10) as connection:
            connection.settimeout(10)
            connection.sendall(struct.pack(">BBBBI", 1, opcode, 0, 0, len(data)) + data)
            header = read_exact(connection, 8)
            version, _, _, _, length = struct.unpack(">BBBBI", header)
            if version != 1 or length > 10_000_000:
                raise RuntimeError("Invalid TitanStore response")
            response = read_exact(connection, length).decode("utf-8")
            if response.startswith("ERROR") or response.startswith("[FAIL]"):
                raise RuntimeError(response)
            return response

    def get(self, key):
        return self.request(KV_GET, key)

    def put(self, key, value):
        if self.request(KV_SET, key + "|" + value) != "OK":
            raise RuntimeError("TitanStore did not acknowledge the decision")

    def add(self, key, member):
        self.request(KV_SADD, key + "|" + member)


def read_exact(connection, length):
    chunks = []
    remaining = length
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise ConnectionError("Master closed the connection")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def run(store, gate_id, message, max_wait_seconds, poll_seconds=5, now=time.time, sleep=time.sleep):
    if not gate_id.startswith("DAG-hitl-gate-") or max_wait_seconds < 1:
        raise ValueError("Invalid HITL gate ID or timeout")
    status_key = PREFIX + "status:" + gate_id
    stamp_key = PREFIX + "ts:" + gate_id
    status = store.get(status_key)
    if status in ("NULL", "CLEARED", ""):
        started_ms = int(now() * 1000)
        store.put(PREFIX + "message:" + gate_id, message)
        store.put(stamp_key, str(started_ms))
        store.add(PREFIX + "queue", gate_id)
        store.put(status_key, "WAITING")
    else:
        try:
            started_ms = int(store.get(stamp_key))
        except ValueError:
            started_ms = int(now() * 1000)
    deadline = started_ms / 1000 + max_wait_seconds
    print(f"[HITL] Gate: {gate_id}", flush=True)
    print(f"[HITL] Review: {message}", flush=True)
    print(f"[HITL] Timeout: {max_wait_seconds}s", flush=True)
    while True:
        status = store.get(status_key)
        if status == "APPROVED":
            print("[HITL] Approved. Releasing downstream jobs.", flush=True)
            return 0
        if status in ("REJECTED", "TIMEOUT"):
            print(f"[HITL] {status}. Failing gate.", flush=True)
            return 1
        remaining = deadline - now()
        if remaining <= 0:
            store.put(status_key, "TIMEOUT")
            print("[HITL] Timed out without a decision.", flush=True)
            return 1
        print(f"[HITL] Waiting for decision ({int(remaining)}s remaining)", flush=True)
        sleep(min(poll_seconds, remaining))


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 3:
        print("Usage: hitl_gate.py <full_gate_id> <max_wait_seconds> <base64_message>", file=sys.stderr)
        return 2
    try:
        message = base64.b64decode(args[2], validate=True).decode("utf-8")
        return run(TitanStore(), args[0], message, int(args[1]))
    except Exception as error:
        print(f"[HITL] Gate error: {error}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
