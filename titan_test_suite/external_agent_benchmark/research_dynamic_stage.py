"""Worker-side research tool for Titan's model-driven Research Agent benchmark."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import sys

VERSION = 1
OP_KV_SET = 0x60


def safe(value: str) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    if not value or any(char not in allowed for char in value):
        raise ValueError(f"unsafe identifier: {value!r}")
    return value


def decode(raw: str) -> dict:
    return json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)).decode())


def recv_exact(stream: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = stream.recv(size - len(data))
        if not chunk:
            raise ConnectionError("master closed the connection")
        data.extend(chunk)
    return bytes(data)


def request(opcode: int, payload: str) -> str:
    host = os.environ.get("TITAN_HOST", "127.0.0.1")
    port = int(os.environ.get("TITAN_PORT", "9090"))
    body = payload.encode()
    with socket.create_connection((host, port), timeout=10) as stream:
        stream.sendall(struct.pack(">BBBBI", VERSION, opcode, 0, 0, len(body)) + body)
        version, _, _, _, length = struct.unpack(">BBBBI", recv_exact(stream, 8))
        if version != VERSION:
            raise RuntimeError(f"unexpected protocol version {version}")
        return recv_exact(stream, length).decode() if length else ""


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("usage: research_dynamic_stage.py <run> <operation> <node> <payload>", file=sys.stderr)
        return 2
    run_id, operation, node = map(safe, argv[:3])
    payload = decode(argv[3])
    if operation == "research":
        result = {
            "topic": payload["topic"],
            "finding": payload["finding"],
            "source": payload["source"],
            "worker_pid": os.getpid(),
        }
    elif operation == "publish":
        report = str(payload["report"])
        artifact = Path("artifacts") / f"DAG-{node}"
        artifact.mkdir(parents=True, exist_ok=True)
        (artifact / "research-report.md").write_text(report, encoding="utf-8")
        result = {"report_sha256": hashlib.sha256(report.encode()).hexdigest(),
                  "characters": len(report), "artifact": "research-report.md"}
    else:
        raise ValueError(f"unsupported operation {operation}")
    key = f"agent:{run_id}:{node}"
    response = request(OP_KV_SET, key + "|" + json.dumps(result, separators=(",", ":")))
    if response != "OK":
        raise RuntimeError("could not persist result: " + response)
    print("RESEARCH_AGENT_RESULT " + json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
