"""Deterministic worker stage for the complex dashboard showcase."""

import json
import sys
import time
from pathlib import Path


def main(args):
    if len(args) != 4:
        print("Usage: stage.py <job_id> <scenario> <stage> <seconds>", file=sys.stderr)
        return 2
    job_id, scenario, stage, duration = args
    if scenario not in {"review", "failure"}:
        print(f"Unsupported scenario: {scenario}", file=sys.stderr)
        return 2

    full_id = "DAG-" + job_id
    output = Path("artifacts") / full_id
    output.mkdir(parents=True, exist_ok=True)
    print(f"[{stage}] Starting {full_id} ({scenario})", flush=True)

    required = {
        "MERGE": ["CLEAN_A", "CLEAN_B", "CLEAN_C"],
        "EVALUATE": ["TRAIN_BASELINE", "TRAIN_CANDIDATE"],
        "ARCHIVE": ["PUBLISH", "AUDIT"],
    }.get(stage, [])
    for parent in required:
        parent_id = f"DAG-SC_{scenario.upper()}_{parent}"
        if not (Path("artifacts") / parent_id / "result.json").is_file():
            print(f"[{stage}] Missing required output from {parent_id}", file=sys.stderr)
            return 3

    seconds = max(0.0, min(float(duration), 20.0))
    for step in range(2):
        time.sleep(seconds / 2)
        print(f"[{stage}] Progress {(step + 1) * 50}%", flush=True)

    result = {"job_id": full_id, "scenario": scenario, "stage": stage,
              "parents_checked": required, "records": 1200 if stage.startswith("INGEST") else 3600,
              "quality_score": 0.98 if stage == "QA" else None}
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    (output / "summary.txt").write_text(
        f"{stage} finished for {scenario}; reviewed parents: {', '.join(required) or 'none'}\n",
        encoding="utf-8")
    if stage == "AUDIT" and scenario == "failure":
        print("[AUDIT] Deliberate validation error: checksum mismatch", file=sys.stderr, flush=True)
        return 7
    print(f"[{stage}] Wrote {output / 'result.json'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
