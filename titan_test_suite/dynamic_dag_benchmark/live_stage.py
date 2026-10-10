"""Small deterministic task executed by Titan's real worker during benchmarks."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import time


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("usage: live_stage.py <job-id> <scenario> <stage> <seconds>", file=sys.stderr)
        return 2
    job_id, scenario, stage, raw_seconds = argv
    seconds = max(0.0, min(float(raw_seconds), 5.0))
    started = time.time_ns()
    print(f"[{scenario}] {stage} started for DAG-{job_id}", flush=True)
    time.sleep(seconds)
    artifact = Path("artifacts") / f"DAG-{job_id}"
    artifact.mkdir(parents=True, exist_ok=True)
    record = {
        "job_id": f"DAG-{job_id}",
        "scenario": scenario,
        "stage": stage,
        "started_ns": started,
        "ended_ns": time.time_ns(),
    }
    (artifact / "benchmark.json").write_text(
        json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print(f"[{scenario}] {stage} completed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
