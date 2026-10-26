"""Matched live fault probe for Titan's external-process execution model."""
from __future__ import annotations

import argparse
import json
import signal
import time
import uuid
from pathlib import Path

import titan_sdk.titan_sdk as sdk
from titan_sdk.titan_sdk import TitanClient, TitanJob


TERMINAL = {"COMPLETED", "FAILED", "DEAD", "CANCELLED"}


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--tasks", type=int, default=12)
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--timeout", type=float, default=240.0)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    sdk.TITAN_HOST, sdk.TITAN_PORT = args.host, args.port
    started = time.time()
    token = uuid.uuid4().hex[:8]
    stage = Path(__file__).parents[1] / "task_granularity_benchmark" / "stage.py"
    ids = [f"peerfault_{token}_{index:02d}" for index in range(args.tasks)]
    jobs = [TitanJob(job_id, str(stage), args=f"{job_id} sleep {args.duration}")
            for job_id in ids]
    result = {
        "schema": "titan-peer-fault-v1", "runtime": "titan",
        "fault": "externally_injected", "tasks": args.tasks,
        "duration_seconds": args.duration, "started": started,
        "dag": f"PEER_FAULT_{token}", "job_ids": ids,
        "completed": False, "error": None, "statuses": {},
        "transient_rpc_errors": [],
    }

    def finish(error: str | None = None) -> None:
        if error is not None:
            result["error"] = error
        result["finished"] = time.time()
        result["elapsed_seconds"] = result["finished"] - started
        atomic_json(args.output, result)

    def interrupted(signum, _frame):
        finish(f"SignalExit: received signal {signum} while awaiting recovery")
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    try:
        response = TitanClient().submit_dag(result["dag"], jobs)
        if response != "DAG_ACCEPTED":
            raise RuntimeError(f"submission rejected: {response}")
        atomic_json(args.ready_file, {"ready": True, "submitted_at": time.time(),
                                      "count": args.tasks, "dag": result["dag"]})
        deadline = time.monotonic() + args.timeout
        statuses: dict[str, str] = {}
        while time.monotonic() < deadline:
            try:
                client = TitanClient()
                statuses = {job_id: client.get_job_status("DAG-" + job_id).strip()
                            for job_id in ids}
                result["statuses"] = statuses
                if statuses and all(status in TERMINAL for status in statuses.values()):
                    break
            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"
                if not result["transient_rpc_errors"] or result["transient_rpc_errors"][-1] != message:
                    result["transient_rpc_errors"].append(message)
            time.sleep(0.25)
        result["completed"] = (len(statuses) == args.tasks
                               and all(status == "COMPLETED" for status in statuses.values()))
        if not result["completed"] and result["error"] is None:
            result["error"] = "terminal completion invariant not reached before timeout"
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finish()


if __name__ == "__main__":
    main()
