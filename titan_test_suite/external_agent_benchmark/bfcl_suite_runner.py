"""Submit a small BFCL parallel-call pilot concurrently to a live Titan cluster."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import time

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
import titan_sdk.titan_sdk as sdk  # noqa: E402
from titan_sdk.titan_sdk import TitanClient  # noqa: E402
from titan_test_suite.external_agent_benchmark.bfcl_runner import (  # noqa: E402
    build_jobs, configure_dashboard_manifest, load_case,
)
from titan_test_suite.external_agent_benchmark.runner import wait_for  # noqa: E402

RESULT_RE = re.compile(r"BFCL_RESULT\s+(\{.*\})")


def run_case(case_id: str, host: str, port: int, timeout: float, suite_stamp: str) -> dict:
    source, case = load_case(case_id)
    run_id = f"{case_id}_{suite_stamp}"
    jobs = build_jobs(run_id, case)
    dag_name = f"BFCL_V4_{case_id.upper()}_{suite_stamp}"
    client = TitanClient()
    started = time.perf_counter()
    response = client.submit_dag(
        dag_name, jobs, agent_run_id=f"bfcl-{run_id}",
        agent_run_name="BFCL V4 Concurrent Pilot",
        agent_stage="PARALLEL_FUNCTION_CALLS", agent_stage_order=0)
    if response != "DAG_ACCEPTED":
        raise RuntimeError(f"{case_id} submission rejected: {response}")
    verify_id = "DAG-" + jobs[-1].id
    status = wait_for(client, verify_id, timeout)
    logs = client.fetch_logs(verify_id) or ""
    matches = RESULT_RE.findall(logs)
    observed = json.loads(matches[-1]) if matches else None
    statuses = {"DAG-" + job.id: client.get_job_status("DAG-" + job.id).strip()
                for job in jobs}
    elapsed = time.perf_counter() - started
    valid = (status == "COMPLETED" and observed is not None
             and observed.get("function_calls_correct") is True
             and all(value == "COMPLETED" for value in statuses.values()))
    return {
        "source": source, "case_id": case_id, "dag": dag_name,
        "jobs": len(jobs), "edges": sum(len(job.parents) for job in jobs),
        "wall_seconds": round(elapsed, 3), "status": status,
        "statuses": statuses, "observed": observed, "valid": valid,
        "dag_url": f"/dags/DAG-{dag_name}",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", nargs="+", default=["parallel_0", "parallel_1", "parallel_2"])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sdk.TITAN_HOST, sdk.TITAN_PORT = args.host, args.port
    configure_dashboard_manifest()
    suite_stamp = str(int(time.time()))
    suite_started = time.perf_counter()
    results = []
    with ThreadPoolExecutor(max_workers=len(args.cases)) as executor:
        futures = {executor.submit(run_case, case, args.host, args.port,
                                   args.timeout, suite_stamp): case for case in args.cases}
        for future in as_completed(futures):
            results.append(future.result())
    wall = time.perf_counter() - suite_started
    results.sort(key=lambda value: value["case_id"])
    total_jobs = sum(value["jobs"] for value in results)
    summary = {
        "schema": "titan-bfcl-concurrent-pilot-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "claim_boundary": "Three official BFCL V4 parallel cases use pinned ground-truth plans and deterministic mock tools. This measures Titan concurrent DAG orchestration, not model accuracy or an official BFCL score.",
        "submission_mode": "one client and one DAG per case, submitted concurrently",
        "case_count": len(results), "total_jobs": total_jobs,
        "total_edges": sum(value["edges"] for value in results),
        "suite_wall_seconds": round(wall, 3),
        "aggregate_jobs_per_second": round(total_jobs / wall, 3) if wall else None,
        "all_valid": all(value["valid"] for value in results),
        "cases": results, "agent_url": "/agents",
    }
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    if not summary["all_valid"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
