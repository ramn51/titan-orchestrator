"""Worker-side tools for a model-planned, runtime-evolving ParallelQA DAG."""
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
OP_KV_GET = 0x61


def safe(value: str) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    if not value or any(char not in allowed for char in value):
        raise ValueError(f"unsafe identifier: {value!r}")
    return value


def decode(raw: str) -> dict:
    return json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)).decode("utf-8"))


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


def key(run_id: str, node: str) -> str:
    return f"agent:{run_id}:{node}"


def load(run_id: str, node: str) -> dict:
    value = request(OP_KV_GET, key(run_id, safe(node)))
    if not value or value.startswith("ERROR"):
        raise RuntimeError(f"missing upstream result for {node}: {value}")
    return json.loads(value)


def execute(run_id: str, operation: str, payload: dict) -> dict:
    if operation == "lookup":
        catalog = payload["catalog"]
        entity = payload["entity"]
        if entity not in catalog:
            raise ValueError(f"unknown lookup entity: {entity}")
        item = catalog[entity]
        return {"entity": entity, "measurement": float(item["measurement"]),
                "unit": item["unit"], "tool": "knowledge_lookup"}
    if operation == "scale":
        source = load(run_id, payload["source"])
        factor = float(payload["factor"])
        return {**source, "factor": factor,
                "adjusted": float(source["measurement"]) * factor,
                "tool": "calculator"}
    if operation == "compare":
        values = [load(run_id, name) for name in payload["sources"]]
        selection = payload.get("selection", "min")
        if selection not in {"min", "max"}:
            raise ValueError(f"unsupported comparison selection: {selection}")
        winner = (min if selection == "min" else max)(
            values, key=lambda item: item["adjusted"])
        return {"answer": winner["entity"], "adjusted_values": values,
                "rule": f"{selection} adjusted measurement"}
    if operation == "answer":
        source = load(run_id, payload["source"])
        return {"answer": source["answer"], "evidence": source.get("adjusted_values", []),
                "question": payload["question"]}
    raise ValueError(f"unsupported operation: {operation}")


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("usage: llm_dynamic_stage.py <run-id> <operation> <node> <payload-b64>", file=sys.stderr)
        return 2
    run_id, operation, node = map(safe, argv[:3])
    result = execute(run_id, operation, decode(argv[3]))
    response = request(OP_KV_SET, key(run_id, node) + "|" + json.dumps(result, separators=(",", ":")))
    if response != "OK":
        raise RuntimeError("could not persist tool result: " + response)
    digest = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
    record = {"operation": operation, "node": node, "result": result, "sha256": digest}
    if operation == "answer":
        artifact = Path("artifacts") / f"DAG-{node}"
        artifact.mkdir(parents=True, exist_ok=True)
        (artifact / "model-planned-answer.json").write_text(
            json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print("DYNAMIC_AGENT_RESULT " + json.dumps(record, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
