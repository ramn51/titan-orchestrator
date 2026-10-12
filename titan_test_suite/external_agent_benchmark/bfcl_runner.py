"""Run a BFCL V4 parallel function-calling case as a visible Titan DAG."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import time

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
import titan_sdk.titan_sdk as sdk  # noqa: E402
from titan_sdk.titan_sdk import TitanClient, TitanJob  # noqa: E402
from titan_test_suite.external_agent_benchmark.runner import encoded, wait_for  # noqa: E402
from titan_test_suite.external_agent_benchmark.model_planner import GeminiFunctionPlanner  # noqa: E402

RESULT_RE = re.compile(r"BFCL_RESULT\s+(\{.*\})")


def configure_dashboard_manifest() -> None:
    manifest = REPO / ".titan_dag_manifest.json"
    os.environ.update({
        "TITAN_DAG_MANIFEST": str(manifest),
        "TITAN_DAG_JOURNAL_DIR": str(REPO / ".titan_dag_journal"),
        "TITAN_DAG_RUN_HISTORY": str(REPO / ".titan_dag_run_history.jsonl"),
        "TITAN_DAG_WORKSPACE_DIR": str(REPO / "titan_workspace"),
    })


def load_case(case_id: str) -> tuple[dict, dict]:
    document = json.loads(Path(__file__).with_name("bfcl_cases.json").read_text(encoding="utf-8"))
    for case in document["cases"]:
        if case["id"] == case_id:
            return document["source"], case
    raise ValueError(f"unknown BFCL case: {case_id}")


def build_jobs(run_id: str, case: dict, calls: list[dict] | None = None,
               planner: str = "pinned-ground-truth") -> list[TitanJob]:
    calls = calls or case["ground_truth"]
    script = str(Path(__file__).with_name("bfcl_stage.py"))
    prefix = f"bfcl_{run_id}"
    plan = f"{prefix}_plan"
    jobs = [TitanJob(plan, script, args=f"{run_id} plan plan "
                     f"{encoded({'question': case['question'], 'calls': calls, 'planner': planner})}")]
    tool_ids = []
    for index, call in enumerate(calls):
        job_id = f"{prefix}_tool{index}"
        jobs.append(TitanJob(job_id, script,
                             args=f"{run_id} tool tool{index} {encoded({'call': call})}",
                             parents=[plan]))
        tool_ids.append(job_id)
    verify = f"{prefix}_verify"
    jobs.append(TitanJob(verify, script,
                         args=f"{run_id} verify verify "
                              f"{encoded({'case_id': case['id'], 'sources': [f'tool{i}' for i in range(len(tool_ids))], 'expected': case['ground_truth']})}",
                         parents=tool_ids))
    return jobs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", default="parallel_0")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--planner", choices=("pinned", "gemini"), default="pinned")
    parser.add_argument("--model", default="gemini-2.5-flash")
    parser.add_argument("--api-key-env", default="GEMINI_API_KEY")
    args = parser.parse_args()
    sdk.TITAN_HOST, sdk.TITAN_PORT = args.host, args.port
    configure_dashboard_manifest()
    source, case = load_case(args.case)
    if args.planner == "gemini":
        key = os.environ.get(args.api_key_env, "")
        if not key:
            raise RuntimeError(f"{args.api_key_env} is not set on the runner host")
        calls = GeminiFunctionPlanner(key, args.model).plan(case["question"], case["functions"])
        planner_label = "gemini:" + args.model
    else:
        calls = case["ground_truth"]
        planner_label = "pinned-ground-truth"
    run_id = f"{case['id']}_{int(time.time())}".replace("-", "_")
    jobs = build_jobs(run_id, case, calls=calls, planner=planner_label)
    dag_name = f"BFCL_V4_{case['id'].upper()}_{int(time.time())}"
    client = TitanClient()
    started = time.time()
    response = client.submit_dag(
        dag_name, jobs, agent_run_id=f"bfcl-{run_id}", agent_run_name="BFCL V4 Parallel",
        agent_stage="PARALLEL_FUNCTION_CALLS", agent_stage_order=0)
    if response != "DAG_ACCEPTED":
        raise RuntimeError(f"submission rejected: {response}")
    verify_id = "DAG-" + jobs[-1].id
    status = wait_for(client, verify_id, args.timeout)
    logs = client.fetch_logs(verify_id) or ""
    matches = RESULT_RE.findall(logs)
    observed = json.loads(matches[-1]) if matches else None
    statuses = {"DAG-" + job.id: client.get_job_status("DAG-" + job.id).strip() for job in jobs}
    valid = (status == "COMPLETED" and observed is not None
             and observed.get("function_calls_correct") is True
             and all(value == "COMPLETED" for value in statuses.values()))
    result = {
        "schema": "titan-bfcl-adapter-v1", "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": source, "case_id": case["id"], "question": case["question"],
        "mode": planner_label + " with deterministic mock tools",
        "claim_boundary": ("This run scores model function selection and Titan orchestration against BFCL ground truth."
                           if args.planner == "gemini" else
                           "This run validates Titan orchestration of BFCL's parallel call graph; it does not score model function selection."),
        "planned_calls": calls,
        "dag": dag_name, "jobs": len(jobs), "edges": sum(len(job.parents) for job in jobs),
        "wall_seconds": round(time.time() - started, 3), "status": status,
        "statuses": statuses, "observed": observed, "valid": valid,
        "dag_url": f"/dags/DAG-{dag_name}", "agent_url": "/agents",
    }
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    if not valid:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
