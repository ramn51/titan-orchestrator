"""Run an official LLMCompiler ParallelQA task as a visible Titan agent DAG."""
from __future__ import annotations

import argparse
import base64
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

TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "DEAD"}
RESULT_RE = re.compile(r"PARALLELQA_RESULT\s+(\{.*\})")


def encoded(value: dict) -> str:
    return base64.urlsafe_b64encode(
        json.dumps(value, separators=(",", ":")).encode("utf-8")).decode("ascii").rstrip("=")


def wait_for(client: TitanClient, job_id: str, timeout: float) -> str:
    deadline = time.monotonic() + timeout
    status = ""
    while time.monotonic() < deadline:
        status = client.get_job_status(job_id).strip()
        if status in TERMINAL:
            return status
        time.sleep(.25)
    return status or "TIMEOUT"


def configure_dashboard_manifest() -> None:
    manifest = REPO / ".titan_dag_manifest.json"
    os.environ.update({
        "TITAN_DAG_MANIFEST": str(manifest),
        "TITAN_DAG_JOURNAL_DIR": str(REPO / ".titan_dag_journal"),
        "TITAN_DAG_RUN_HISTORY": str(REPO / ".titan_dag_run_history.jsonl"),
        "TITAN_DAG_WORKSPACE_DIR": str(REPO / "titan_workspace"),
    })


def load_case(case_id: str) -> tuple[dict, dict]:
    document = json.loads(Path(__file__).with_name("parallelqa_cases.json").read_text(encoding="utf-8"))
    for case in document["cases"]:
        if case["id"] == case_id:
            return document["source"], case
    raise ValueError(f"unknown case id: {case_id}")


def build_jobs(run_id: str, case: dict) -> list[TitanJob]:
    script = str(Path(__file__).with_name("stage.py"))
    prefix = f"pqa_{run_id}"
    plan = f"{prefix}_plan"
    jobs = [TitanJob(plan, script,
                     args=f"{run_id} plan plan {encoded(case)}")]
    transforms = []
    for index, entity in enumerate(case["entities"]):
        lookup = f"{prefix}_lookup{index}"
        transform = f"{prefix}_transform{index}"
        jobs.append(TitanJob(lookup, script,
                             args=f"{run_id} lookup lookup{index} {encoded(entity)}",
                             parents=[plan]))
        jobs.append(TitanJob(transform, script,
                             args=f"{run_id} transform transform{index} "
                                  f"{encoded({'source': f'lookup{index}', 'factor': entity['factor']})}",
                             parents=[lookup]))
        transforms.append(transform)
    compare = f"{prefix}_compare"
    jobs.append(TitanJob(compare, script,
                         args=f"{run_id} compare compare "
                              f"{encoded({'sources': [f'transform{i}' for i in range(len(transforms))], 'selection': case.get('selection', 'min')})}",
                         parents=transforms))
    answer = f"{prefix}_answer"
    jobs.append(TitanJob(answer, script,
                         args=f"{run_id} answer answer "
                              f"{encoded({'source': 'compare', 'question': case['question'], 'expected': case['answer']})}",
                         parents=[compare]))
    return jobs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", default="1")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sdk.TITAN_HOST, sdk.TITAN_PORT = args.host, args.port
    configure_dashboard_manifest()
    source, case = load_case(args.case)
    run_id = f"{case['id']}_{int(time.time())}"
    jobs = build_jobs(run_id, case)
    dag_name = f"LLMCOMPILER_PARALLELQA_{case['id']}_{run_id}"
    client = TitanClient()
    started = time.time()
    response = client.submit_dag(
        dag_name, jobs, agent_run_id=f"parallelqa-{run_id}",
        agent_run_name="LLMCompiler ParallelQA",
        agent_stage="PARALLEL_TOOL_GRAPH", agent_stage_order=0)
    if response != "DAG_ACCEPTED":
        raise RuntimeError(f"submission rejected: {response}")
    answer_id = "DAG-" + jobs[-1].id
    status = wait_for(client, answer_id, args.timeout)
    logs = client.fetch_logs(answer_id) or ""
    matches = RESULT_RE.findall(logs)
    observed = json.loads(matches[-1]) if matches else None
    statuses = {"DAG-" + job.id: client.get_job_status("DAG-" + job.id).strip() for job in jobs}
    valid = (status == "COMPLETED" and observed is not None and observed.get("correct") is True
             and all(value == "COMPLETED" for value in statuses.values()))
    result = {
        "schema": "titan-external-agent-benchmark-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": source, "case": case, "mode": "pinned plan and pinned knowledge",
        "claim_boundary": "Titan executes the public task's parallel tool graph; this run does not evaluate LLM planning or live retrieval.",
        "dag": dag_name, "jobs": len(jobs),
        "edges": sum(len(job.parents) for job in jobs),
        "wall_seconds": round(time.time() - started, 3),
        "status": status, "statuses": statuses, "observed": observed, "valid": valid,
        "dag_url": f"/dags/DAG-{dag_name}", "agent_url": "/agents",
    }
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    if not valid:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
