"""Dependency-free data-flow stage used by Titan's correctness benchmark."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys


def safe_part(value: str) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    if not value or any(char not in allowed for char in value):
        raise ValueError(f"unsafe path component: {value!r}")
    return value


def deterministic_bytes(seed: str, size: int) -> bytes:
    output = bytearray()
    counter = 0
    while len(output) < size:
        output.extend(hashlib.sha256(f"{seed}:{counter}".encode()).digest())
        counter += 1
    return bytes(output[:size])


def transformed(label: str, inputs: list[bytes]) -> bytes:
    digest = hashlib.sha256()
    digest.update(label.encode())
    for value in inputs:
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.digest()


def read_inputs(root: Path, names: str) -> list[bytes]:
    return [(root / f"{safe_part(name)}.bin").read_bytes()
            for name in names.split(",") if name]


def main() -> None:
    if len(sys.argv) < 5:
        raise SystemExit("usage: stage.py <run-id> <produce|transform|flaky|verify> <label> <args...>")
    run_id, operation, label = map(safe_part, sys.argv[1:4])
    root = Path("data_flow") / run_id
    root.mkdir(parents=True, exist_ok=True)
    output = root / f"{label}.bin"

    if operation == "produce":
        size = int(sys.argv[4])
        if not 1 <= size <= 16 * 1024 * 1024:
            raise ValueError("payload must be between 1 byte and 16 MiB")
        seed = safe_part(sys.argv[5]) if len(sys.argv) > 5 else run_id
        output.write_bytes(deterministic_bytes(seed, size))
    elif operation in {"transform", "flaky"}:
        if operation == "flaky":
            marker = root / f".{label}.first-attempt"
            if not marker.exists():
                marker.write_text("intentional first-attempt failure\n", encoding="utf-8")
                print(f"DATA_FLOW_RETRY run={run_id} stage={label} intentional_exit=7", flush=True)
                raise SystemExit(7)
        output.write_bytes(transformed(label, read_inputs(root, sys.argv[4])))
    elif operation == "verify":
        expected = sys.argv[4]
        sources = sys.argv[5]
        values = read_inputs(root, sources)
        actual = hashlib.sha256(b"".join(values)).hexdigest()
        if actual != expected:
            raise RuntimeError(f"hash mismatch: expected {expected}, got {actual}")
        result = {"run_id": run_id, "sources": sources.split(","), "sha256": actual,
                  "bytes": sum(map(len, values)),
                  "files": sorted(path.name for path in root.glob("*.bin"))}
        artifact = Path("artifacts") / f"DAG-{label}"
        artifact.mkdir(parents=True, exist_ok=True)
        (artifact / "verification.json").write_text(
            json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print("DATA_FLOW_RESULT " + json.dumps(result, sort_keys=True), flush=True)
        return
    else:
        raise ValueError(f"unknown operation: {operation}")

    content = output.read_bytes()
    print(f"DATA_FLOW_STAGE run={run_id} stage={label} operation={operation} "
          f"bytes={len(content)} sha256={hashlib.sha256(content).hexdigest()}", flush=True)


if __name__ == "__main__":
    main()
