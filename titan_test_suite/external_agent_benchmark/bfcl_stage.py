"""Mock tool executor and verifier for BFCL orchestration measurements."""
from __future__ import annotations

import base64
import json
import os
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


def canonical(call: dict) -> tuple[str, str]:
    return call["name"], json.dumps(call["arguments"], sort_keys=True, separators=(",", ":"))


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


def result_key(run_id: str, node: str) -> str:
    return f"agent:{run_id}:{safe(node)}"


def load_result(run_id: str, node: str) -> dict:
    value = request(OP_KV_GET, result_key(run_id, node))
    if not value or value.startswith("ERROR"):
        raise RuntimeError(f"missing upstream result for {node}: {value}")
    return json.loads(value)


def save_result(run_id: str, node: str, value: dict) -> None:
    response = request(
        OP_KV_SET,
        result_key(run_id, node) + "|" + json.dumps(value, separators=(",", ":")),
    )
    if response != "OK":
        raise RuntimeError("could not persist BFCL stage result: " + response)


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("usage: bfcl_stage.py <run-id> <operation> <node> <payload-b64>", file=sys.stderr)
        return 2
    run_id, operation, node = map(safe, argv[:3])
    payload = decode(argv[3])
    if operation == "plan":
        result = {"question": payload["question"], "calls": payload["calls"],
                  "planner": payload["planner"]}
    elif operation == "tool":
        call = payload["call"]
        arguments = call["arguments"]
        if call["name"] == "spotify.play":
            result = {"call": call, "tool_result": {
                "accepted": True, "artist": arguments["artist"],
                "scheduled_minutes": int(arguments["duration"]),
            }}
        elif call["name"] == "calculate_em_force":
            interval = float(arguments["d_time"])
            result = {"call": call, "tool_result": {
                "em_force": abs(float(arguments["b_field"]) * float(arguments["area"]) / interval),
                "unit": "volt",
            }}
        elif call["name"] == "calculate_resistance":
            named_resistivity = {"copper": 1.68e-8, "aluminum": 2.82e-8}
            raw_resistivity = arguments["resistivity"]
            rho = named_resistivity.get(str(raw_resistivity).lower())
            if rho is None and not isinstance(raw_resistivity, str):
                rho = float(raw_resistivity)
            if rho is None:
                raise ValueError(f"unknown material resistivity: {raw_resistivity}")
            result = {"call": call, "tool_result": {
                "resistance_ohms": rho * float(arguments["length"]) / float(arguments["area"]),
            }}
        else:
            raise ValueError(f"no benchmark mock implementation for {call['name']}")
    elif operation == "verify":
        observed = [load_result(run_id, name)["call"] for name in payload["sources"]]
        expected = payload["expected"]
        valid = sorted(map(canonical, observed)) == sorted(map(canonical, expected))
        result = {"case_id": payload["case_id"], "expected_calls": expected,
                  "observed_calls": observed, "function_calls_correct": valid}
        artifact = Path("artifacts") / f"DAG-{node}"
        artifact.mkdir(parents=True, exist_ok=True)
        (artifact / "bfcl-result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print("BFCL_RESULT " + json.dumps(result, sort_keys=True), flush=True)
        if not valid:
            raise RuntimeError("observed function calls do not match BFCL ground truth")
    else:
        raise ValueError(f"unsupported operation: {operation}")
    save_result(run_id, node, result)
    print("BFCL_STAGE " + json.dumps({"operation": operation, "node": node}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
