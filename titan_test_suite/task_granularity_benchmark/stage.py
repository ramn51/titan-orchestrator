"""Deterministic benchmark task with no third-party dependencies."""

from __future__ import annotations

import math
import sys
import time


def busy_wait(seconds: float) -> None:
    deadline = time.perf_counter() + seconds
    value = 0.5
    while time.perf_counter() < deadline:
        value = math.sin(value) * math.cos(value) + 0.5
    if value == float("inf"):  # Keep the calculation observable to the interpreter.
        print(value)


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("usage: stage.py <job-id> <sleep|cpu> <duration-seconds>")
    job_id, mode, raw_seconds = sys.argv[1:]
    seconds = float(raw_seconds)
    if seconds < 0 or seconds > 60:
        raise ValueError("duration must be between 0 and 60 seconds")
    started = time.perf_counter()
    if mode == "sleep":
        time.sleep(seconds)
    elif mode == "cpu":
        busy_wait(seconds)
    else:
        raise ValueError(f"unknown benchmark mode: {mode}")
    elapsed = time.perf_counter() - started
    print(f"TASK_BENCH job={job_id} mode={mode} requested_s={seconds:.6f} "
          f"payload_elapsed_s={elapsed:.6f}", flush=True)


if __name__ == "__main__":
    main()
