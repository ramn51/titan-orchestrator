"""Run forced cross-host TitanStore, asset, and local-file data-path checks."""
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
from titan_test_suite.cross_host_data_benchmark.stage import payload  # noqa: E402

TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}
RESULT_RE = re.compile(r"CROSS_HOST_RESULT\s+(\{.*\})")


def wait_all(client: TitanClient, jobs: list[TitanJob], timeout: float) -> dict[str, str]:
    pending = {"DAG-" + job.id for job in jobs}
    statuses = {}
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        for job_id in list(pending):
            status = client.get_job_status(job_id).strip()
            statuses[job_id] = status
            if status in TERMINAL:
                pending.remove(job_id)
        if pending:
            time.sleep(.25)
    for job_id in pending:
        statuses[job_id] = "TIMEOUT"
    return statuses


def result_from_logs(client: TitanClient, job_id: str) -> dict | None:
    matches = RESULT_RE.findall(client.fetch_logs("DAG-" + job_id) or "")
    return json.loads(matches[-1]) if matches else None


def configure_manifest() -> None:
    manifest = REPO / ".titan_dag_manifest.json"
    os.environ.update({
        "TITAN_DAG_MANIFEST": str(manifest),
        "TITAN_DAG_JOURNAL_DIR": str(REPO / ".titan_dag_journal"),
        "TITAN_DAG_RUN_HISTORY": str(REPO / ".titan_dag_run_history.jsonl"),
        "TITAN_DAG_WORKSPACE_DIR": str(REPO / "titan_workspace"),
    })


def build_jobs(run_id: str) -> list[TitanJob]:
    script = str(Path(__file__).with_name("stage.py"))
    jobs = []
    for lane in ("kv", "asset", "local"):
        producer = f"xhost_{run_id}_{lane}_producer"
        consumer = f"xhost_{run_id}_{lane}_consumer"
        jobs.append(TitanJob(producer, script, args=f"{run_id} {lane}_produce", requirement="HOST_A"))
        operation = "local_absence" if lane == "local" else f"{lane}_consume"
        jobs.append(TitanJob(consumer, script, args=f"{run_id} {operation}",
                             parents=[producer], requirement="GENERAL"))
    return jobs


def materialize_dashboard_artifacts(run_id: str, jobs: list[TitanJob], evidence: dict) -> list[str]:
    """Copy verified evidence into the dashboard host's job artifact namespace.

    Remote worker files are host-local and the dashboard has no remote filesystem
    browser. These snapshots make the bytes and provenance verified by each worker
    downloadable without implying that worker directories are shared.
    """
    artifact_names = []
    shared = REPO / "titan_workspace" / "shared" / "artifacts"
    content = payload(run_id)
    for job in jobs:
        job_id = "DAG-" + job.id
        root = shared / job_id
        root.mkdir(parents=True, exist_ok=True)
        record = evidence.get(job.id) or {}
        evidence_path = root / "transfer-evidence.json"
        evidence_path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        artifact_names.append(f"artifacts/{job_id}/{evidence_path.name}")
        if "_kv_" in job.id or "_asset_" in job.id or "_local_producer" in job.id:
            role = "received-payload.bin" if job.id.endswith("_consumer") else "generated-payload.bin"
            (root / role).write_bytes(content)
            artifact_names.append(f"artifacts/{job_id}/{role}")
    return artifact_names


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sdk.TITAN_HOST, sdk.TITAN_PORT = args.host, args.port
    configure_manifest()
    run_id = str(int(time.time()))
    jobs = build_jobs(run_id)
    dag_name = "CROSS_HOST_DATA_" + run_id
    client = TitanClient()
    started = time.time()
    response = client.submit_dag(dag_name, jobs)
    if response != "DAG_ACCEPTED":
        raise RuntimeError("submission rejected: " + response)
    statuses = wait_all(client, jobs, args.timeout)
    evidence = {job.id: result_from_logs(client, job.id) for job in jobs}
    kv = evidence[f"xhost_{run_id}_kv_consumer"] or {}
    asset = evidence[f"xhost_{run_id}_asset_consumer"] or {}
    local_producer = evidence[f"xhost_{run_id}_local_producer"] or {}
    local_consumer = evidence[f"xhost_{run_id}_local_consumer"] or {}
    dashboard_artifacts = materialize_dashboard_artifacts(run_id, jobs, evidence)
    valid = (all(value == "COMPLETED" for value in statuses.values())
             and kv.get("different_hosts") is True
             and asset.get("different_hosts") is True
             and kv.get("sha256") == asset.get("sha256")
             and local_consumer.get("exists") is False
             and local_producer.get("host") != local_consumer.get("consumer_host"))
    summary = {
        "schema": "titan-cross-host-data-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "topology": "master+store+HOST_A worker on VM1; GENERAL worker on VM2",
        "claim_boundary": "Correctness experiment; master-side worker shares resources with control plane.",
        "dag": dag_name, "jobs": len(jobs), "edges": 3,
        "wall_seconds": round(time.time() - started, 3), "statuses": statuses,
        "evidence": evidence, "valid": valid,
        "dashboard_artifacts": dashboard_artifacts,
        "artifact_note": "Remote outputs are copied after verification into the dashboard host's per-job artifact namespace.",
        "dag_url": f"/dags/DAG-{dag_name}",
    }
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    if not valid:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
