"""Submit the Titan CI workload to a live local Titan cluster."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time

from titan_sdk import TitanClient, TitanJob


TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}


def wait_for(client: TitanClient, job_ids: list[str], timeout: float) -> dict[str, str]:
    deadline = time.monotonic() + timeout
    statuses: dict[str, str] = {}
    while time.monotonic() < deadline:
        statuses = {job_id: client.get_job_status("DAG-" + job_id).strip()
                    for job_id in job_ids}
        if all(status in TERMINAL for status in statuses.values()):
            return statuses
        time.sleep(.5)
    return statuses


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=420)
    args = parser.parse_args()

    repository = Path(os.environ["TITAN_CI_REPO"]).resolve()
    stage = repository / "titan_test_suite" / "ci_dogfood" / "stage.py"
    stamp = str(int(time.time()))
    prefix = "ci_" + stamp
    jobs = [
        TitanJob(prefix + "_preflight", str(stage), args="preflight"),
        TitanJob(prefix + "_mcp", str(stage), args="mcp", parents=[prefix + "_preflight"]),
        TitanJob(prefix + "_benchmarks", str(stage), args="benchmarks", parents=[prefix + "_preflight"]),
        TitanJob(prefix + "_dashboard", str(stage), args="dashboard", parents=[prefix + "_preflight"]),
    ]
    client = TitanClient()
    started = time.monotonic()
    response = client.submit_dag("TITAN_CI_" + stamp, jobs)
    if response != "DAG_ACCEPTED":
        raise RuntimeError(f"Titan rejected its CI DAG: {response}")

    ids = [job.id for job in jobs]
    statuses = wait_for(client, ids, args.timeout)
    results_root = Path(os.environ["TITAN_CI_RESULTS"])
    stage_results = {}
    for suite in ("preflight", "mcp", "benchmarks", "dashboard"):
        path = results_root / f"{suite}.json"
        if path.is_file():
            stage_results[suite] = json.loads(path.read_text(encoding="utf-8"))

    passed = (len(stage_results) == 4
              and all(status == "COMPLETED" for status in statuses.values())
              and all(result["returncode"] == 0 for result in stage_results.values()))
    summary = {
        "schema": "titan-dogfood-ci-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dag": "TITAN_CI_" + stamp,
        "submission": response,
        "wall_seconds": round(time.monotonic() - started, 3),
        "statuses": statuses,
        "stage_results": stage_results,
        "passed": passed,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)
    if not passed:
        for job_id, status in statuses.items():
            if status != "COMPLETED":
                print(f"--- {job_id} ({status}) ---", file=sys.stderr)
                print(client.fetch_logs("DAG-" + job_id), file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
