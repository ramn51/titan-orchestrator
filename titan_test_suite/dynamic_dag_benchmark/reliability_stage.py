"""Dependency-free worker task used by the live reliability benchmark."""

from pathlib import Path
import sys
import time


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if len(args) < 3:
        print("usage: reliability_stage.py <job-id> <mode> <seconds>", file=sys.stderr)
        return 2
    job_id, mode, seconds_text = args[:3]
    seconds = max(0.0, min(float(seconds_text), 60.0))
    print(f"[RELIABILITY] {job_id} mode={mode} sleep={seconds}s", flush=True)
    time.sleep(seconds)
    if mode == "fail":
        print(f"[RELIABILITY] intentional failure for {job_id}", file=sys.stderr, flush=True)
        return 7
    artifact = Path(f"{job_id}.txt")
    artifact.write_text(f"{job_id} completed in mode {mode}\n", encoding="utf-8")
    print(f"[RELIABILITY] completed {job_id}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
