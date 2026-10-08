"""Submit a branched, artifact-producing DAG for dashboard verification.

Usage: python showcase.py review|failure
Set TITAN_DAG_MANIFEST and TITAN_DAG_WORKSPACE_DIR to the dashboard's paths.
"""

import sys
from pathlib import Path

from titan_sdk import TitanClient, TitanJob


STAGES = (
    ("DISCOVER", [], 1),
    ("INGEST_A", ["DISCOVER"], 2),
    ("INGEST_B", ["DISCOVER"], 2),
    ("INGEST_C", ["DISCOVER"], 2),
    ("CLEAN_A", ["INGEST_A"], 2),
    ("CLEAN_B", ["INGEST_B"], 2),
    ("CLEAN_C", ["INGEST_C"], 2),
    ("MERGE", ["CLEAN_A", "CLEAN_B", "CLEAN_C"], 1),
    ("QA", ["MERGE"], 1),
    ("TRAIN_BASELINE", ["QA"], 3),
    ("TRAIN_CANDIDATE", ["QA"], 4),
    ("EVALUATE", ["TRAIN_BASELINE", "TRAIN_CANDIDATE"], 2),
    ("PUBLISH", ["EVALUATE"], 1),
    ("AUDIT", ["DISCOVER"], 2),
    ("ARCHIVE", ["PUBLISH", "AUDIT"], 1),
)


def build_jobs(scenario):
    script = str(Path(__file__).with_name("stage.py"))
    jobs = []
    for stage, parents, seconds in STAGES:
        job_id = f"SC_{scenario.upper()}_{stage}"
        jobs.append(TitanJob(
            job_id=job_id,
            filename=script,
            args=f"{job_id} {scenario} {stage} {seconds}",
            parents=[f"SC_{scenario.upper()}_{parent}" for parent in parents],
            delay=1000 if stage == "CLEAN_C" else 0,
            hitl_message=("Review the QA artifact and logs before model training."
                          if stage == "QA" else None),
            max_wait_seconds=3600,
        ))
    return jobs


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in {"review", "failure"}:
        raise SystemExit("Usage: python showcase.py review|failure")
    scenario = sys.argv[1]
    name = "SHOWCASE_COMPLEX_" + scenario.upper()
    result = TitanClient().submit_dag(name, build_jobs(scenario))
    print(f"{name}: {result}")
    if result != "DAG_ACCEPTED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
