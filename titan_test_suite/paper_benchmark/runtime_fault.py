"""Matched worker-loss and control-plane-loss probes for Dask and Ray.

The external controller starts this client, waits for ``ready_file``, injects
the requested process failure, and then reads the JSON result.  The workload
is deliberately long enough to ensure tasks are active when the fault lands.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import time
from pathlib import Path


def task(task_id: int, seconds: float) -> dict:
    started = time.time()
    time.sleep(seconds)
    return {
        "task_id": task_id,
        "host": os.uname().nodename,
        "started": started,
        "finished": time.time(),
    }


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    temporary.replace(path)


def run_dask(address: str, count: int, seconds: float, ready: Path) -> list[dict]:
    from dask.distributed import Client

    client = Client(address)
    futures = [client.submit(task, i, seconds, pure=False, retries=1) for i in range(count)]
    atomic_json(ready, {"ready": True, "submitted_at": time.time(), "count": count})
    try:
        return client.gather(futures)
    finally:
        client.close()


def run_ray(address: str, count: int, seconds: float, ready: Path) -> list[dict]:
    import ray

    ray.init(address=address, ignore_reinit_error=True, logging_level="ERROR")
    remote = ray.remote(max_retries=1)(task)
    refs = [remote.remote(i, seconds) for i in range(count)]
    atomic_json(ready, {"ready": True, "submitted_at": time.time(), "count": count})
    try:
        return ray.get(refs)
    finally:
        ray.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime", choices=["dask", "ray"], required=True)
    parser.add_argument("--address", required=True)
    parser.add_argument("--tasks", type=int, default=16)
    parser.add_argument("--duration", type=float, default=12.0)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    started = time.time()
    result = {
        "schema": "titan-peer-fault-v1",
        "runtime": args.runtime,
        "fault": "externally_injected",
        "tasks": args.tasks,
        "duration_seconds": args.duration,
        "started": started,
        "completed": False,
        "error": None,
        "results": [],
    }

    def interrupted(signum, _frame):
        result["error"] = f"SignalExit: received signal {signum} while awaiting recovery"
        result["finished"] = time.time()
        result["elapsed_seconds"] = result["finished"] - started
        atomic_json(args.output, result)
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    try:
        runner = run_dask if args.runtime == "dask" else run_ray
        values = runner(args.address, args.tasks, args.duration, args.ready_file)
        result["results"] = values
        result["completed"] = len(values) == args.tasks
        result["worker_hosts"] = sorted({v["host"] for v in values})
    except Exception as exc:  # Evidence must survive expected control-plane loss.
        result["error"] = f"{type(exc).__name__}: {exc}"
    result["finished"] = time.time()
    result["elapsed_seconds"] = result["finished"] - started
    atomic_json(args.output, result)


if __name__ == "__main__":
    main()
