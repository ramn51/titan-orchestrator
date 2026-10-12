"""Dependency-free cross-host producer and consumer stages."""
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
OP_UPLOAD_ASSET = 0x53
OP_FETCH_ASSET = 0x54
OP_KV_SET = 0x60
OP_KV_GET = 0x61


def safe(value: str) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    if not value or any(char not in allowed for char in value):
        raise ValueError(f"unsafe identifier: {value!r}")
    return value


def recv_exact(stream: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = stream.recv(size - len(data))
        if not chunk:
            raise ConnectionError("master closed the connection early")
        data.extend(chunk)
    return bytes(data)


def request(opcode: int, payload: str) -> str:
    host = os.environ.get("TITAN_HOST", "127.0.0.1")
    port = int(os.environ.get("TITAN_PORT", "9090"))
    encoded = payload.encode("utf-8")
    with socket.create_connection((host, port), timeout=10) as stream:
        stream.sendall(struct.pack(">BBBBI", VERSION, opcode, 0, 0, len(encoded)) + encoded)
        version, _, _, _, length = struct.unpack(">BBBBI", recv_exact(stream, 8))
        if version != VERSION:
            raise RuntimeError(f"unexpected protocol version: {version}")
        return recv_exact(stream, length).decode("utf-8") if length else ""


def payload(run_id: str, size: int = 8192) -> bytes:
    output = bytearray()
    counter = 0
    while len(output) < size:
        output.extend(hashlib.sha256(f"{run_id}:{counter}".encode()).digest())
        counter += 1
    return bytes(output[:size])


def emit(operation: str, record: dict) -> None:
    print("CROSS_HOST_RESULT " + json.dumps({"operation": operation, **record}, sort_keys=True), flush=True)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: stage.py <run-id> <operation>", file=sys.stderr)
        return 2
    run_id, operation = map(safe, argv)
    host = socket.gethostname()
    content = payload(run_id)
    digest = hashlib.sha256(content).hexdigest()
    key = "cross-host-" + run_id
    asset = "cross-host-" + run_id + ".json"
    local_path = Path("cross_host_local") / run_id / "payload.bin"

    if operation == "kv_produce":
        envelope = json.dumps({"producer_host": host, "sha256": digest,
                               "data": base64.b64encode(content).decode("ascii")}, separators=(",", ":"))
        response = request(OP_KV_SET, key + "|" + envelope)
        if response != "OK":
            raise RuntimeError("store write failed: " + response)
        emit(operation, {"host": host, "sha256": digest, "bytes": len(content)})
    elif operation == "kv_consume":
        envelope = json.loads(request(OP_KV_GET, key))
        received = base64.b64decode(envelope["data"])
        actual = hashlib.sha256(received).hexdigest()
        if actual != envelope["sha256"] or actual != digest:
            raise RuntimeError("TitanStore cross-host hash mismatch")
        emit(operation, {"producer_host": envelope["producer_host"], "consumer_host": host,
                         "sha256": actual, "bytes": len(received),
                         "different_hosts": envelope["producer_host"] != host})
    elif operation == "asset_produce":
        envelope = json.dumps({"producer_host": host, "sha256": digest,
                               "data": base64.b64encode(content).decode("ascii")}).encode()
        response = request(OP_UPLOAD_ASSET, asset + "|" + base64.b64encode(envelope).decode("ascii"))
        if response != "UPLOAD_SUCCESS":
            raise RuntimeError("asset upload failed: " + response)
        emit(operation, {"host": host, "sha256": digest, "bytes": len(content), "asset": asset})
    elif operation == "asset_consume":
        response = request(OP_FETCH_ASSET, asset)
        if response.startswith("ERROR"):
            raise RuntimeError("asset fetch failed: " + response)
        envelope = json.loads(base64.b64decode(response))
        received = base64.b64decode(envelope["data"])
        actual = hashlib.sha256(received).hexdigest()
        if actual != envelope["sha256"] or actual != digest:
            raise RuntimeError("asset cross-host hash mismatch")
        emit(operation, {"producer_host": envelope["producer_host"], "consumer_host": host,
                         "sha256": actual, "bytes": len(received),
                         "different_hosts": envelope["producer_host"] != host})
    elif operation == "local_produce":
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(content)
        emit(operation, {"host": host, "sha256": digest, "path": str(local_path)})
    elif operation == "local_absence":
        exists = local_path.exists()
        emit(operation, {"consumer_host": host, "path": str(local_path), "exists": exists})
        if exists:
            raise RuntimeError("host-local file unexpectedly visible on the other worker host")
    else:
        raise ValueError(f"unknown operation: {operation}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
