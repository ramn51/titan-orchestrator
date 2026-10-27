"""Live Redis outage/reconnect/AOF recovery probe for Titan's Store adapter."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import uuid

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from titan_sdk.titan_sdk import TitanClient, TitanJob
from titan_test_suite.task_granularity_benchmark.runner import (
    read_new_spans, telemetry_offsets, titan_stats_query,
)


def wait_port(port: int, available: bool, timeout: float = 10) -> float:
    started = time.monotonic()
    deadline = started + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=.25):
                current = True
        except OSError:
            current = False
        if current == available:
            return time.monotonic() - started
        time.sleep(.1)
    raise TimeoutError(f"port {port} did not become {'available' if available else 'unavailable'}")


def stop_redis(pid_file: Path) -> float:
    pid = int(pid_file.read_text().strip())
    started = time.monotonic()
    os.kill(pid, signal.SIGTERM)
    wait_port(6379, False)
    return time.monotonic() - started


def start_redis(data_dir: Path, pid_file: Path) -> float:
    subprocess.run([
        "redis-server", "--bind", "127.0.0.1", "--port", "6379", "--save", "",
        "--appendonly", "yes", "--appendfsync", "everysec", "--dir", str(data_dir),
        "--daemonize", "yes", "--pidfile", str(pid_file),
        "--logfile", str(data_dir / "redis.log"),
    ], check=True)
    return wait_port(6379, True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--telemetry", type=Path, required=True)
    parser.add_argument("--redis-data-dir", type=Path, required=True)
    parser.add_argument("--redis-pid-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--outage-seconds", type=float, default=3)
    args = parser.parse_args()

    client = TitanClient()
    token = uuid.uuid4().hex[:10]
    sentinel_key = f"titan:redis-fault:{token}"
    sentinel_value = f"survives-{token}"
    assert client.store_put(sentinel_key, sentinel_value) == "OK"
    assert client.store_get(sentinel_key) == sentinel_value
    time.sleep(1.2)  # appendfsync everysec durability boundary

    stage = Path(__file__).parents[1] / "task_granularity_benchmark" / "stage.py"
    ids = [f"redis_fault_{token}_{index:02d}" for index in range(8)]
    sink = f"redis_fault_{token}_sink"
    jobs = [TitanJob(job_id, str(stage), args=f"{job_id} sleep 2.0") for job_id in ids]
    jobs.append(TitanJob(sink, str(stage), args=f"{sink} sleep 0.0", parents=ids))
    expected = {"DAG-" + job_id for job_id in [*ids, sink]}
    offsets = telemetry_offsets(args.telemetry)
    started_at = int(time.time() * 1000)
    response = client.submit_dag(f"REDIS_FAULT_{token}", jobs)
    if response != "DAG_ACCEPTED":
        raise RuntimeError(f"DAG rejected: {response}")
    time.sleep(.5)

    stop_seconds = stop_redis(args.redis_pid_file)
    unavailable_read = client.store_get(sentinel_key)
    time.sleep(args.outage_seconds)
    restart_seconds = start_redis(args.redis_data_dir, args.redis_pid_file)

    reconnect_started = time.monotonic()
    recovered_value = ""
    while time.monotonic() - reconnect_started < 15:
        recovered_value = client.store_get(sentinel_key)
        if recovered_value == sentinel_value:
            break
        time.sleep(.2)
    reconnect_seconds = time.monotonic() - reconnect_started
    assert recovered_value == sentinel_value
    assert client.store_put(sentinel_key + ":post", "ok") == "OK"

    deadline = time.monotonic() + 60
    status = ""
    while time.monotonic() < deadline:
        status = client.get_job_status("DAG-" + sink).strip()
        if status in {"COMPLETED", "FAILED", "CANCELLED"}:
            break
        time.sleep(.2)

    spans = {}
    for _ in range(80):
        spans = read_new_spans(args.telemetry, offsets, expected)
        if all(spans.get(job_id) for job_id in expected):
            break
        time.sleep(.1)
    duplicates = sum(max(0, len(spans.get(job_id, [])) - 1) for job_id in expected)
    non_completed = [job_id for job_id in expected
                     if not spans.get(job_id) or spans[job_id][-1].get("status") != "COMPLETED"]
    sink_span = (spans.get("DAG-" + sink) or [{}])[-1]
    ordering = [job_id for job_id in ids if spans.get("DAG-" + job_id)
                and int(sink_span.get("started_at") or 0) < int(spans["DAG-" + job_id][-1]["ended_at"])]

    # A second restart proves the sentinel is recovered from Redis AOF rather than memory.
    second_stop_seconds = stop_redis(args.redis_pid_file)
    second_restart_seconds = start_redis(args.redis_data_dir, args.redis_pid_file)
    post_restart_value = ""
    for _ in range(50):
        post_restart_value = client.store_get(sentinel_key)
        if post_restart_value == sentinel_value:
            break
        time.sleep(.2)

    ended_at = int(time.time() * 1000)
    metrics = titan_stats_query(f"metrics|{started_at}|5000")
    health = [bool(sample.get("store_healthy")) for sample in metrics.get("samples") or []
              if started_at <= int(sample.get("at") or 0) <= ended_at]
    report = {
        "schema": "titan-redis-fault-recovery-v1", "dag": f"REDIS_FAULT_{token}",
        "submission_response": response, "final_status": status,
        "expected_spans": len(expected), "observed_spans": sum(bool(spans.get(x)) for x in expected),
        "duplicate_attempts": duplicates, "non_completed": non_completed,
        "dependency_order_violations": ordering,
        "outage_seconds": args.outage_seconds, "redis_stop_seconds": stop_seconds,
        "redis_restart_seconds": restart_seconds, "adapter_reconnect_seconds": reconnect_seconds,
        "read_during_outage": unavailable_read, "sentinel_after_first_restart": recovered_value,
        "second_stop_seconds": second_stop_seconds, "second_restart_seconds": second_restart_seconds,
        "sentinel_after_second_restart": post_restart_value,
        "store_health_samples": health,
    }
    report["passed"] = (response == "DAG_ACCEPTED" and status == "COMPLETED"
                        and not duplicates and not non_completed and not ordering
                        and recovered_value == sentinel_value
                        and post_restart_value == sentinel_value
                        and client.store_get(sentinel_key + ":post") == "ok")
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (args.output / "report.html").write_text(
        "<!doctype html><meta charset='utf-8'><title>Titan Redis recovery</title>"
        "<style>body{max-width:900px;margin:40px auto;background:#081014;color:#eaf2f2;font:15px/1.6 Segoe UI}"
        "section{background:#101b20;border:1px solid #29414a;border-radius:10px;padding:20px}"
        "b{color:#54e6a8;font-size:28px}pre{white-space:pre-wrap}</style>"
        f"<h1>Redis outage and AOF recovery</h1><section><b>{'PASS' if report['passed'] else 'FINDING'}</b>"
        "<p>Redis was stopped during an active Titan DAG, restarted, then restarted again to prove AOF replay.</p>"
        f"<pre>{json.dumps(report, indent=2)}</pre></section>", encoding="utf-8")
    print(json.dumps(report, indent=2))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
