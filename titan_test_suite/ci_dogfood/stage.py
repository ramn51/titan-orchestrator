"""Execute one bounded CI check as a Titan task and persist its evidence."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time


SUITES = {
    "preflight": [sys.executable, "-c", "import titan_sdk, titan_mcp, flask; print('imports-ok')"],
    "mcp": [sys.executable, "-m", "pytest", "-q", "titan_mcp/tests"],
    "benchmarks": [
        sys.executable, "-m", "pytest", "-q",
        "titan_test_suite/paper_benchmark/test_common.py",
        "titan_test_suite/store_backend_benchmark/test_runner.py",
        "titan_test_suite/task_granularity_benchmark/test_runner.py",
    ],
    "dashboard": [sys.executable, "-m", "pytest", "-q", "perm_files/tests/test_dag_viewer.py"],
}


def safe_name(value: str) -> str:
    if not value or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for char in value):
        raise ValueError(f"invalid CI suite name: {value!r}")
    return value


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: stage.py <preflight|mcp|benchmarks|dashboard>")
    suite = safe_name(sys.argv[1])
    if suite not in SUITES:
        raise ValueError(f"unknown CI suite: {suite}")

    repository = Path(os.environ["TITAN_CI_REPO"]).resolve()
    results = Path(os.environ["TITAN_CI_RESULTS"]).resolve()
    results.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    completed = subprocess.run(
        SUITES[suite], cwd=repository, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=240,
    )
    record = {
        "schema": "titan-ci-stage-v1",
        "suite": suite,
        "returncode": completed.returncode,
        "duration_seconds": round(time.monotonic() - started, 3),
        "command": SUITES[suite],
        "output": completed.stdout[-20000:],
    }
    destination = results / f"{suite}.json"
    destination.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print("TITAN_CI_RESULT " + json.dumps({
        "suite": suite, "returncode": completed.returncode,
        "duration_seconds": record["duration_seconds"],
    }, sort_keys=True), flush=True)
    if completed.returncode:
        print(completed.stdout, flush=True)
        raise SystemExit(completed.returncode)


if __name__ == "__main__":
    main()
