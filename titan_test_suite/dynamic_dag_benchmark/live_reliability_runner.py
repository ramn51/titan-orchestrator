"""Exercise user-visible Titan reliability paths against a live cluster."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import html
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request

from titan_sdk import TitanClient, TitanJob

from .live_runner import configure_dag_persistence


TERMINAL = {"COMPLETED", "FAILED", "DEAD", "CANCELLED"}


def default_java_executable():
    """Use the configured JDK without embedding a developer-specific path."""
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        executable = "java.exe" if os.name == "nt" else "java"
        return Path(java_home) / "bin" / executable
    discovered = shutil.which("java")
    return Path(discovered) if discovered else Path("java.exe" if os.name == "nt" else "java")


def dashboard_status(status):
    """Match the dashboard's presentation of exhausted engine retries."""
    return "FAILED" if status == "DEAD" else status


def wait_status(client, job_id, wanted, timeout=90.0):
    deadline = time.monotonic() + timeout
    observed = []
    while time.monotonic() < deadline:
        status = client.get_job_status("DAG-" + job_id).strip()
        if not observed or status != observed[-1]:
            observed.append(status)
        if status in wanted:
            return status, observed
        time.sleep(0.2)
    raise TimeoutError(f"DAG-{job_id} did not reach {sorted(wanted)}; observed={observed}")


def submit(client, name, jobs):
    response = client.submit_dag(name, jobs)
    if response != "DAG_ACCEPTED":
        raise RuntimeError(f"{name} rejected: {response}")
    return response


def worker_command(java, jar, port, capability, permanent):
    return [str(java), "-cp", str(jar), "titan.Main", "WORKER", str(port), "127.0.0.1",
            "9090", capability, str(bool(permanent)).lower()]


def start_worker(java, jar, port, capability, output_dir, permanent=True):
    stdout = (output_dir / f"worker-{port}.out.log").open("ab")
    stderr = (output_dir / f"worker-{port}.err.log").open("ab")
    environment = os.environ.copy()
    python_dir = str(Path(sys.executable).resolve().parent)
    environment["PATH"] = python_dir + os.pathsep + environment.get("PATH", "")
    process = subprocess.Popen(worker_command(java, jar, port, capability, permanent),
                               cwd=Path.cwd(), stdout=stdout, stderr=stderr,
                               env=environment)
    process._titan_logs = (stdout, stderr)  # keep handles alive with the process
    return process


def stop_worker(process):
    if process is None:
        return
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=8)
    for stream in getattr(process, "_titan_logs", ()):
        stream.close()


def assigned_worker(job_id):
    with urllib.request.urlopen(
            "http://127.0.0.1:5000/api/cluster?mode=live&since=15m", timeout=20) as response:
        data = json.load(response)
    record = (data.get("stats") or {}).get("dag_jobs", {}).get("DAG-" + job_id, {})
    return str(record.get("worker") or "")


def wait_assigned_worker(job_id, port, timeout=20.0):
    expected = f"127.0.0.1:{port}"
    deadline = time.monotonic() + timeout
    seen = []
    while time.monotonic() < deadline:
        worker = assigned_worker(job_id)
        if worker and (not seen or worker != seen[-1]):
            seen.append(worker)
        if worker == expected:
            return worker
        time.sleep(0.2)
    raise RuntimeError(f"DAG-{job_id} was not assigned to {expected}; observed={seen}")


def job_spans(telemetry_dir, job_id):
    result = []
    full_id = "DAG-" + job_id
    for path in Path(telemetry_dir).glob("spans-*.jsonl"):
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line in stream:
                try:
                    span = json.loads(line)
                except ValueError:
                    continue
                if span.get("job_id") == full_id:
                    result.append(span)
    return sorted(result, key=lambda item: (int(item.get("attempt") or 0),
                                            int(item.get("started_at") or 0)))


def render(report):
    rows = "".join(
        f"<tr><td>{html.escape(case['name'])}</td><td>{html.escape(case['expected'])}</td>"
        f"<td>{html.escape(case.get('final', ''))}</td><td>{case.get('seconds', 0):.2f}s</td>"
        f"<td class={'ok' if case['passed'] else 'bad'}>{'PASS' if case['passed'] else 'FAIL'}</td>"
        f"<td>{html.escape(case.get('evidence', ''))}</td></tr>"
        for case in report["cases"])
    return f"""<!doctype html><html><head><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>
<title>Titan live reliability benchmark</title><style>
:root{{--bg:#07100e;--panel:#101c19;--line:#29453b;--text:#ecf8f2;--muted:#9fb8ae;--green:#45e6a8;--red:#ff7188}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 Segoe UI,sans-serif}}main{{max-width:1180px;margin:auto;padding:28px}}section{{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px;margin:14px 0}}table{{border-collapse:collapse;width:100%}}th,td{{text-align:left;padding:9px;border-bottom:1px solid var(--line);vertical-align:top}}th{{color:var(--muted);font-size:11px;text-transform:uppercase}}.ok{{color:var(--green)}}.bad{{color:var(--red)}}code{{color:#b8f5dc}}.scroll{{overflow:auto}}
</style></head><body><main><p style='color:var(--muted)'>TITAN · LIVE RELIABILITY BENCHMARK</p><h1>Failure and recovery paths</h1>
<section><b class={'ok' if report['passed'] else 'bad'}>{sum(c['passed'] for c in report['cases'])}/{len(report['cases'])} cases passed</b><p>Each case is registered in Titan's normal DAG journal for the dashboard.</p></section>
<section class=scroll><table><thead><tr><th>Case</th><th>Expected</th><th>Final</th><th>Time</th><th>Result</th><th>Evidence</th></tr></thead><tbody>{rows}</tbody></table></section>
</main></body></html>"""


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dashboard-visible", action="store_true")
    parser.add_argument("--dashboard-manifest", type=Path,
                        default=Path(".titan_dag_manifest.json"))
    parser.add_argument("--java", type=Path,
                        default=default_java_executable())
    parser.add_argument("--jar", type=Path,
                        default=Path("target/titan-orchestrator-1.0-SNAPSHOT-shaded.jar"))
    parser.add_argument("--worker-port", type=int, default=8095)
    parser.add_argument("--telemetry-dir", type=Path, default=Path("telemetry"))
    args = parser.parse_args(argv)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    persistence = configure_dag_persistence(
        args.output_dir, args.dashboard_visible, args.dashboard_manifest)
    client = TitanClient()
    script = Path(__file__).with_name("reliability_stage.py")
    stamp = str(int(time.time() * 1000))[-9:]
    cases = []

    def record(name, expected, started, passed, final, evidence=""):
        cases.append({"name": name, "expected": expected, "passed": bool(passed),
                      "final": final, "seconds": round(time.monotonic() - started, 3),
                      "evidence": evidence})

    started = time.monotonic()
    root, child = f"rel{stamp}_success_root", f"rel{stamp}_success_child"
    submit(client, f"RELIABILITY_SUCCESS_{stamp}", [
        TitanJob(root, str(script), args=f"{root} success 0.2"),
        TitanJob(child, str(script), args=f"{child} success 0.2", parents=[root])])
    final, trace = wait_status(client, child, {"COMPLETED"})
    record("success-chain", "COMPLETED", started, final == "COMPLETED", final,
           "status trace: " + " → ".join(trace))

    started = time.monotonic()
    failed = f"rel{stamp}_failure"
    submit(client, f"RELIABILITY_FAILURE_{stamp}", [
        TitanJob(failed, str(script), args=f"{failed} fail 0.1")])
    final, trace = wait_status(client, failed, {"FAILED", "DEAD"}, timeout=120)
    shown = dashboard_status(final)
    record("nonzero-exit", "FAILED", started, shown == "FAILED", shown,
           "exit 7; status trace: " + " → ".join(trace))

    started = time.monotonic()
    cancel_root, cancel_child = f"rel{stamp}_cancel_root", f"rel{stamp}_cancel_child"
    submit(client, f"RELIABILITY_CANCELLATION_{stamp}", [
        TitanJob(cancel_root, str(script), args=f"{cancel_root} success 20"),
        TitanJob(cancel_child, str(script), args=f"{cancel_child} success 0.1",
                 parents=[cancel_root])])
    wait_status(client, cancel_root, {"RUNNING"})
    cancel_response = client.cancel_job("DAG-" + cancel_root)
    root_final, _ = wait_status(client, cancel_root, {"CANCELLED"})
    child_final, _ = wait_status(client, cancel_child, {"CANCELLED"})
    record("cancellation-cascade", "CANCELLED/CANCELLED", started,
           root_final == child_final == "CANCELLED", f"{root_final}/{child_final}",
           cancel_response)

    for decision, expected in (("APPROVED", "COMPLETED"), ("REJECTED", "FAILED")):
        started = time.monotonic()
        suffix = decision.lower()
        source = f"rel{stamp}_hitl_{suffix}"
        gate = "hitl-gate-" + source
        submit(client, f"RELIABILITY_HITL_{decision}_{stamp}", [
            TitanJob(source, str(script), args=f"{source} success 0.1",
                     hitl_message=f"Reliability benchmark: {decision.lower()} this checkpoint",
                     max_wait_seconds=60)])
        status_key = "titan:hitl:status:DAG-" + gate
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and client.store_get(status_key) != "WAITING":
            time.sleep(0.2)
        if client.store_get(status_key) != "WAITING":
            raise TimeoutError(f"HITL gate {gate} did not become WAITING")
        if client.store_put(status_key, decision) != "OK":
            raise RuntimeError(f"could not set HITL decision {decision}")
        wanted = {expected, "DEAD"} if expected == "FAILED" else {expected}
        final, trace = wait_status(client, gate, wanted, timeout=120)
        shown = dashboard_status(final)
        record("hitl-" + suffix, expected, started, shown == expected, shown,
               "status trace: " + " → ".join(trace))

    # Scheduler capability parsing currently accepts non-metadata tokens shorter
    # than 20 characters. Keep benchmark capabilities unique within that contract.
    capability = "CAP" + stamp[-8:]
    recovery_capability = "REC" + stamp[-8:]
    waiting_job = f"rel{stamp}_capability"
    started = time.monotonic()
    submit(client, f"RELIABILITY_CAPABILITY_WAIT_{stamp}", [
        TitanJob(waiting_job, str(script), args=f"{waiting_job} success 0.2",
                 requirement=capability)])
    admission, admission_trace = wait_status(
        client, waiting_job, {"PENDING", "WAITING", "RUNNING", "COMPLETED"}, timeout=20)
    worker = None
    try:
        if admission in {"PENDING", "WAITING"}:
            worker = start_worker(args.java, args.jar.resolve(), args.worker_port,
                                  capability, args.output_dir)
        final, trace = wait_status(client, waiting_job, {"COMPLETED"}, timeout=45)
        record("capability-release", "COMPLETED", started, final == "COMPLETED", final,
               f"admission={admission}; trace=" + " → ".join(admission_trace + trace))

        stop_worker(worker)
        worker = start_worker(args.java, args.jar.resolve(), args.worker_port,
                              recovery_capability, args.output_dir, permanent=True)
        time.sleep(2.0)

        recovery_job = f"rel{stamp}_worker_recovery"
        started = time.monotonic()
        submit(client, f"RELIABILITY_WORKER_RECOVERY_{stamp}", [
            TitanJob(recovery_job, str(script), args=f"{recovery_job} success 12",
                     requirement=recovery_capability)])
        wait_status(client, recovery_job, {"RUNNING"}, timeout=30)
        first_worker = wait_assigned_worker(recovery_job, args.worker_port)
        first_pid = worker.pid
        stop_worker(worker)
        worker = start_worker(args.java, args.jar.resolve(), args.worker_port + 1,
                              recovery_capability, args.output_dir, permanent=True)
        final, trace = wait_status(client, recovery_job,
                                   {"COMPLETED", "FAILED", "DEAD"}, timeout=90)
        shown = dashboard_status(final)
        spans = job_spans(args.telemetry_dir, recovery_job)
        failed_attempts = [span for span in spans if span.get("status") == "FAILED"
                           and "lost heartbeat" in str(span.get("reason") or "").lower()]
        completed_attempts = [span for span in spans if span.get("status") == "COMPLETED"]
        recovered = (shown == "COMPLETED" and len(spans) >= 2 and failed_attempts
                     and completed_attempts
                     and str(completed_attempts[-1].get("worker") or "").endswith(
                         ":" + str(args.worker_port + 1)))
        record("worker-loss-recovery", "COMPLETED after retry", started,
               recovered, shown,
               f"killed {first_worker} pid {first_pid}; attempts="
               + ", ".join(f"#{span.get('attempt')} {span.get('status')} {span.get('worker')}"
                           for span in spans)
               + "; trace=" + " → ".join(trace))
    finally:
        stop_worker(worker)

    report = {"schema": 1, "generated_at": datetime.now(timezone.utc).isoformat(),
              "passed": all(case["passed"] for case in cases), "cases": cases,
              "configuration": {"dashboard_visible": args.dashboard_visible,
                                "dag_manifest": persistence["TITAN_DAG_MANIFEST"],
                                "capability_case": capability,
                                "recovery_capability": recovery_capability}}
    (args.output_dir / "summary.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8")
    (args.output_dir / "report.html").write_text(render(report), encoding="utf-8")
    print(json.dumps({"passed": report["passed"], "cases": len(cases),
                      "report": str(args.output_dir / "report.html")}, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
