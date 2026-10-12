"""Worker-side state, tool, and observation nodes for the BFCL multi-turn adapter."""
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
    with socket.create_connection((host, port), timeout=15) as stream:
        stream.sendall(struct.pack(">BBBBI", VERSION, opcode, 0, 0, len(body)) + body)
        version, _, _, _, length = struct.unpack(">BBBBI", recv_exact(stream, 8))
        if version != VERSION:
            raise RuntimeError(f"unexpected protocol version {version}")
        return recv_exact(stream, length).decode() if length else ""


def result_key(run_id: str, node: str) -> str:
    return f"agent:{run_id}:{node}"


def persist(run_id: str, node: str, value: dict) -> None:
    response = request(OP_KV_SET, result_key(run_id, node) + "|" + json.dumps(value, separators=(",", ":")))
    if response != "OK":
        raise RuntimeError("could not persist result: " + response)


def load(run_id: str, node: str) -> dict:
    value = request(OP_KV_GET, result_key(run_id, safe(node)))
    if not value or value.startswith("ERROR"):
        raise RuntimeError(f"missing result {node}: {value}")
    return json.loads(value)


def execute_tool(name: str, arguments: dict, state: dict) -> dict:
    if name == "get_zipcode_based_on_city":
        zipcodes = {"Rivermist": "83214", "Stonebrook": "74532"}
        return {"zipcode": zipcodes[arguments["city"]]}
    if name == "estimate_distance":
        pair = {str(arguments["cityA"]), str(arguments["cityB"])}
        if pair != {"83214", "74532"}:
            raise ValueError("unsupported BFCL city pair")
        return {"distance": 750.0, "intermediaryCities": ["Cedar Junction"]}
    if name == "estimate_drive_feasibility_by_mileage":
        distance = float(arguments["distance"])
        fuel = float(state["VehicleControlAPI"]["fuelLevel"])
        return {"canDrive": fuel * 25.0 >= distance, "availableRange": fuel * 25.0}
    if name == "fillFuelTank":
        current = float(state["VehicleControlAPI"]["fuelLevel"])
        return {"fuelLevel": min(50.0, current + float(arguments["fuelAmount"]))}
    if name == "lockDoors":
        return {"lockStatus": "unlocked" if arguments["unlock"] else "locked",
                "doors": list(arguments["door"]), "remainingUnlockedDoors": 0}
    if name == "pressBrakePedal":
        position = float(arguments["pedalPosition"])
        return {"brakePedalStatus": "pressed" if position > 0 else "released",
                "brakePedalForce": round(position * 500.0, 3)}
    if name == "startEngine":
        return {"engineState": "running" if arguments["ignitionMode"] == "START" else "stopped",
                "fuelLevel": state["VehicleControlAPI"]["fuelLevel"],
                "batteryVoltage": state["VehicleControlAPI"]["batteryVoltage"]}
    if name == "send_message":
        return {"sent_status": True, "receiver_id": arguments["receiver_id"],
                "message_id": int(state["MessageAPI"]["message_count"]) + 1,
                "message": arguments["message"]}
    if name == "view_messages_sent":
        return {"messages": state["MessageAPI"].get("sent", {})}
    raise ValueError(f"unsupported BFCL tool {name}")


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("usage: bfcl_multiturn_stage.py <run-id> <operation> <node> <payload>", file=sys.stderr)
        return 2
    run_id, operation, node = map(safe, argv[:3])
    payload = decode(argv[3])
    if operation == "context":
        result = {"turn": payload["turn"], "frontier": payload["frontier"],
                  "user": payload["user"], "state_sha256": hashlib.sha256(
                      json.dumps(payload["state"], sort_keys=True).encode()).hexdigest()}
    elif operation == "tool":
        call = payload["call"]
        arguments = dict(call["arguments"])
        upstream = [load(run_id, source) for source in payload.get("sources", [])]
        if call["name"] == "estimate_distance" and upstream:
            by_city = {item["call"]["arguments"]["city"]: item["output"]["zipcode"]
                       for item in upstream}
            arguments.update({"cityA": by_city["Rivermist"], "cityB": by_city["Stonebrook"]})
        elif call["name"] == "estimate_drive_feasibility_by_mileage" and upstream:
            arguments["distance"] = upstream[0]["output"]["distance"]
        try:
            output = execute_tool(call["name"], arguments, payload["state"])
        except (KeyError, TypeError, ValueError) as error:
            # A bad model call is benchmark evidence, not a worker-process crash.
            # Return the tool error so the next model frontier can repair it.
            output = {"error": type(error).__name__, "message": str(error)}
        result = {"call": {"name": call["name"], "arguments": arguments},
                  "upstream": payload.get("sources", []), "output": output}
    elif operation == "observe":
        results = [load(run_id, source) for source in payload["sources"]]
        result = {"turn": payload["turn"], "frontier": payload["frontier"],
                  "tool_results": results}
        artifact = Path("artifacts") / f"DAG-{node}"
        artifact.mkdir(parents=True, exist_ok=True)
        (artifact / "bfcl-frontier-observation.json").write_text(
            json.dumps(result, indent=2) + "\n", encoding="utf-8")
    else:
        raise ValueError(f"unsupported operation {operation}")
    persist(run_id, node, result)
    print("BFCL_MULTITURN " + json.dumps({"operation": operation, "node": node,
          "result": result}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
