"""Live Titan data-flow correctness benchmark.

The suite proves that dependency edges protect usable file data on a shared
worker workspace.  It intentionally makes no claim about automatic file
movement between different worker machines.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from titan_sdk.titan_sdk import TitanClient, TitanJob  # noqa: E402
try:
    from titan_test_suite.data_flow_benchmark.stage import deterministic_bytes, transformed  # noqa: E402
except ModuleNotFoundError:  # Allows isolated model tests before copying into the repository.
    from .stage import deterministic_bytes, transformed  # type: ignore  # noqa: E402

TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}
RESULT_RE = re.compile(r"DATA_FLOW_RESULT\s+(\{.*\})")


@dataclass(frozen=True)
class Node:
    label: str
    parents: tuple[str, ...]
    operation: str = "transform"


def patterns() -> dict[str, list[Node]]:
    return {
        "chain": [Node("root", (), "produce"), Node("step1", ("root",)),
                  Node("step2", ("step1",)), Node("final", ("step2",))],
        "diamond": [Node("root", (), "produce"), Node("left", ("root",)),
                    Node("right", ("root",)), Node("final", ("left", "right"))],
        "fanout-fanin": [Node("root", (), "produce"),
                         *[Node(f"branch{i}", ("root",)) for i in range(8)],
                         Node("final", tuple(f"branch{i}" for i in range(8)))],
        "layered": [Node("root", (), "produce"),
                    Node("l1a", ("root",)), Node("l1b", ("root",)), Node("l1c", ("root",)),
                    Node("l2a", ("l1a", "l1b")), Node("l2b", ("l1b", "l1c")),
                    Node("final", ("l2a", "l2b"))],
    }


def expected_outputs(seed: str, graph: list[Node], payload_bytes: int) -> dict[str, bytes]:
    values: dict[str, bytes] = {}
    for node in graph:
        if node.operation == "produce":
            values[node.label] = deterministic_bytes(seed, payload_bytes)
        else:
            values[node.label] = transformed(node.label, [values[p] for p in node.parents])
    return values


def full_id(run_id: str, label: str) -> str:
    return f"DAG-df_{run_id}_{label}"


def configure_dashboard_manifest() -> None:
    manifest = REPO / ".titan_dag_manifest.json"
    os.environ.update({
        "TITAN_DAG_MANIFEST": str(manifest),
        "TITAN_DAG_JOURNAL_DIR": str(REPO / ".titan_dag_journal"),
        "TITAN_DAG_RUN_HISTORY": str(REPO / ".titan_dag_run_history.jsonl"),
        "TITAN_DAG_WORKSPACE_DIR": str(REPO / "titan_workspace"),
    })


def wait_for(client: TitanClient, job_id: str, timeout: float) -> str:
    deadline = time.monotonic() + timeout
    status = ""
    while time.monotonic() < deadline:
        status = client.get_job_status(job_id).strip()
        if status in TERMINAL:
            return status
        time.sleep(.25)
    return status or "TIMEOUT"


def run_case(client: TitanClient, pattern: str, graph: list[Node], pass_index: int,
             payload_bytes: int, timeout: float) -> dict:
    stamp = f"{int(time.time())}_{pass_index}_{pattern.replace('-', '')}"
    content_seed = f"{pattern.replace('-', '_')}_v1"
    expected = expected_outputs(content_seed, graph, payload_bytes)
    retry_label = ""
    if pass_index == 5:
        retry_label = next(node.label for node in graph if node.operation != "produce" and node.label != "final")
    verifier_id = f"df_{stamp}_verify"
    jobs = []
    for node in graph:
        job_id = f"df_{stamp}_{node.label}"
        parents = [f"df_{stamp}_{parent}" for parent in node.parents]
        operation = "flaky" if node.label == retry_label else node.operation
        if operation == "produce":
            args = f"{stamp} produce {node.label} {payload_bytes} {content_seed}"
        else:
            args = f"{stamp} {operation} {node.label} {','.join(node.parents)}"
        jobs.append(TitanJob(job_id, str(Path(__file__).with_name("stage.py")), args=args, parents=parents))
    final = graph[-1]
    source_names = final.label
    expected_hash = hashlib.sha256(expected[final.label]).hexdigest()
    jobs.append(TitanJob(verifier_id, str(Path(__file__).with_name("stage.py")),
                         args=f"{stamp} verify {verifier_id} {expected_hash} {source_names}",
                         parents=[f"df_{stamp}_{final.label}"]))
    dag_name = f"DATA_FLOW_{pattern.upper().replace('-', '_')}_P{pass_index}_{stamp}"
    started = time.monotonic()
    response = client.submit_dag(dag_name, jobs)
    if response != "DAG_ACCEPTED":
        raise RuntimeError(f"{dag_name} rejected: {response}")
    status = wait_for(client, "DAG-" + verifier_id, timeout)
    wall = time.monotonic() - started
    logs = client.fetch_logs("DAG-" + verifier_id)
    matches = RESULT_RE.findall(logs or "")
    evidence = json.loads(matches[-1]) if matches else None
    stages = {node.label: client.get_job_status(full_id(stamp, node.label)).strip() for node in graph}
    retry_logs = client.fetch_logs(full_id(stamp, retry_label)) if retry_label else ""
    retry_observed = "DATA_FLOW_RETRY" in (retry_logs or "") if retry_label else None
    valid = (status == "COMPLETED" and evidence is not None
             and evidence.get("sha256") == expected_hash
             and all(value == "COMPLETED" for value in stages.values())
             and (pass_index != 5 or retry_observed))
    return {"pattern": pattern, "pass": pass_index, "dag": dag_name,
            "tasks": len(jobs), "edges": sum(len(job.parents) for job in jobs),
            "payload_bytes": payload_bytes, "wall_seconds": round(wall, 3),
            "status": status, "stage_status": stages, "expected_sha256": expected_hash,
            "observed": evidence, "retry_stage": retry_label or None,
            "retry_observed": retry_observed, "valid": valid,
            "dag_url": f"/dags/DAG-{dag_name}"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--passes", type=int, default=5)
    parser.add_argument("--payload-bytes", type=int, default=262_144)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.passes <= 10 or not 1 <= args.payload_bytes <= 16 * 1024 * 1024:
        parser.error("passes must be 1-10 and payload bytes 1-16777216")
    configure_dashboard_manifest()
    args.output.mkdir(parents=True, exist_ok=True)
    client = TitanClient()
    cases = []
    for pass_index in range(1, args.passes + 1):
        for name, graph in patterns().items():
            result = run_case(client, name, graph, pass_index, args.payload_bytes, args.timeout)
            cases.append(result)
            print(json.dumps({"pattern": name, "pass": pass_index,
                              "valid": result["valid"], "wall_seconds": result["wall_seconds"]}), flush=True)
    summary = {"schema": "titan-data-flow-v1", "generated_at": datetime.now(timezone.utc).isoformat(),
               "scope": "shared worker workspace; no cross-machine file-transfer claim",
               "passes": args.passes, "payload_bytes": args.payload_bytes,
               "valid_cases": sum(case["valid"] for case in cases),
               "total_cases": len(cases), "cases": cases}
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(args.output / "summary.json")
    if summary["valid_cases"] != summary["total_cases"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
