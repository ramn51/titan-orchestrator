#  Copyright 2026 Ram Narayanan
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Generates a realistic workload so every dashboard panel has something true to show.

Not a test — a demo-data generator. Each phase exists to light up a specific panel,
so if a chart looks wrong you can tell whether the data or the rendering is at fault.

  Phase 1  repeat pipelines      -> Analytics: pipeline duration trend (needs >= 2 runs each)
  Phase 2  mixed durations       -> Analytics: job duration distribution (fills every bucket)
  Phase 3  deliberate failures   -> Analytics: success-rate dip · Live: dead-letter queue
  Phase 4  queue pressure        -> Live: queue composition, dispatch loop, scaling pressure
                                    Analytics: queue-wait percentiles, per-worker utilisation
  Phase 5  unschedulable job     -> Live: pending reasons, capability dead end, parked lane
  Phase 6  deep DAG              -> Live: blocked lane · Timeline: real depth
  Phase 7  a service             -> Live: long-running work, topology service count

Usage:
    python3 titan_test_suite/tests/populate_dashboard.py            # full run, ~4 min
    python3 titan_test_suite/tests/populate_dashboard.py --quick    # ~90s, fewer repeats
    python3 titan_test_suite/tests/populate_dashboard.py --phase 4  # just one phase

Then open http://localhost:5000/cluster (Live and Analytics tabs) and /timeline.
"""

import os
import shutil
import sys
import time

from titan_sdk import TitanClient, TitanJob

c = TitanClient()
QUICK = "--quick" in sys.argv
ONLY = None
if "--phase" in sys.argv:
    try:
        ONLY = int(sys.argv[sys.argv.index("--phase") + 1])
    except (IndexError, ValueError):
        ONLY = None

SCRIPTS = "/tmp/titan_demo"
TAG = int(time.time()) % 100000


def script(name, body):
    os.makedirs(SCRIPTS, exist_ok=True)
    path = os.path.join(SCRIPTS, name + ".py")
    with open(path, "w") as f:
        f.write(body)
    c.deploy_script(path)
    return path


def sleeper(name, seconds):
    return script(name, f"import time\ntime.sleep({seconds})\nprint('{name} ok')\n")


def banner(n, title, lights_up):
    if ONLY is not None and ONLY != n:
        return False
    print(f"\n[{n}] {title}")
    print(f"    lights up: {lights_up}")
    return True


def wait_for(job_id, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        st = (c.get_job_status(job_id) or "").upper()
        if any(x in st for x in ("COMPLETED", "FAILED", "DEAD", "CANCELLED")):
            return st
        time.sleep(0.4)
    return "TIMEOUT"


def main():
    print(f"Populating the Titan dashboard  (tag {TAG}{', quick mode' if QUICK else ''})")

    # A spread of runtimes so the duration histogram has shape rather than one spike.
    tiny = sleeper("tiny", 0.05)
    fast = sleeper("fast", 0.35)
    med = sleeper("med", 1.2)
    slow = sleeper("slow", 3.0)
    heavy = sleeper("heavy", 7.0)

    # ---- 1. repeat pipelines: the trend chart needs the SAME name run more than once ----
    if banner(1, "Repeat pipelines", "Analytics → pipeline duration trend"):
        runs = 2 if QUICK else 3
        for pipeline, steps in (("nightly-etl", [fast, med, slow]),
                                ("hourly-rollup", [tiny, fast, fast])):
            for r in range(runs):
                jobs = []
                prev = None
                for i, sc in enumerate(steps):
                    jid = f"{pipeline.replace('-', '')}{TAG}r{r}s{i}"
                    jobs.append(TitanJob(job_id=jid, filename=sc,
                                         parents=[prev] if prev else []))
                    prev = jid
                c.submit_dag(pipeline, jobs)
                wait_for(f"DAG-{prev}", 120)
                print(f"    {pipeline} run {r + 1}/{runs} done")

    # ---- 2. mixed durations across every histogram bucket ----
    if banner(2, "Mixed durations", "Analytics → job duration distribution"):
        jobs = []
        for i, sc in enumerate([tiny] * 4 + [fast] * 5 + [med] * 6 + [slow] * 3 + [heavy] * 2):
            jobs.append(TitanJob(job_id=f"mix{TAG}-{i}", filename=sc))
        c.submit_dag("duration-spread", jobs)
        time.sleep(10 if QUICK else 22)
        print("    submitted 20 jobs spanning 50ms → 7s")

    # ---- 3. failures: the success-rate dip and the dead-letter queue ----
    if banner(3, "Deliberate failures", "Analytics → success rate · Live → dead-letter queue"):
        e_conn = script("err_conn", "raise ConnectionError('postgres refused connection on 5432')\n")
        e_code = script("err_code", "import sys\nsys.exit(137)\n")
        e_imp = script("err_import", "import pandas_that_does_not_exist\n")
        c.submit_dag("failure-demo", [
            TitanJob(job_id=f"ok{TAG}-1", filename=fast),
            TitanJob(job_id=f"ok{TAG}-2", filename=fast),
            TitanJob(job_id=f"fail{TAG}-db", filename=e_conn),
            TitanJob(job_id=f"fail{TAG}-oom", filename=e_code),
            TitanJob(job_id=f"fail{TAG}-imp", filename=e_imp),
        ])
        # Each failure retries 3× before reaching the DLQ, so this takes a moment.
        wait_for(f"DAG-fail{TAG}-db", 150)
        print("    3 jobs failed with distinct reasons (exit code, connection, missing module)")

    # ---- 4. queue pressure: percentiles, utilisation, and a real scale-up ----
    if banner(4, "Queue pressure", "Live → queue composition, dispatch, scaling · "
                                   "Analytics → queue-wait percentiles, per-worker utilisation"):
        waves = 2 if QUICK else 3
        for w in range(waves):
            c.submit_dag("bulk-load",
                         [TitanJob(job_id=f"bulk{TAG}w{w}-{i}", filename=heavy) for i in range(12)])
            print(f"    wave {w + 1}/{waves}: 12 heavy jobs (pool holds 4 per worker)")
            time.sleep(8)
        # let the scaler react and the queue drain enough to show the shape
        time.sleep(20 if QUICK else 45)

    # ---- 5. an unschedulable job ----
    if banner(5, "Unschedulable job", "Live → pending reasons, capability dead end, parked lane"):
        c.submit_job(TitanJob(job_id=f"needs-tpu-{TAG}", filename=fast, requirement="TPU"))
        time.sleep(6)
        print("    a job requiring TPU is parked — no worker offers that capability")
        print("    (cancel it later with: client.cancel_job('DAG-needs-tpu-%d'))" % TAG)

    # ---- 6. a deep DAG so the blocked lane and real depth are visible ----
    if banner(6, "Deep DAG", "Live → blocked lane · Timeline → genuine depth"):
        jobs = [TitanJob(job_id=f"deep{TAG}-r1", filename=fast),
                TitanJob(job_id=f"deep{TAG}-r2", filename=fast)]
        prev = [f"deep{TAG}-r1", f"deep{TAG}-r2"]
        for lvl in range(1, 7):
            jid = f"deep{TAG}-l{lvl}"
            jobs.append(TitanJob(job_id=jid, filename=med, parents=prev))
            prev = [jid]
        c.submit_dag("deep-chain", jobs)
        print("    8 nodes, 7 levels deep")
        time.sleep(6)

    # ---- 7. a service, for long-running work ----
    if banner(7, "A long-running service", "Live → long-running work, topology service count"):
        name = f"demo_svc_{TAG}"
        shutil.rmtree(name, ignore_errors=True)
        os.makedirs(name, exist_ok=True)
        port = 9800 + (TAG % 90)
        with open(os.path.join(name, "server.py"), "w") as f:
            f.write("from http.server import BaseHTTPRequestHandler, HTTPServer\n"
                    "class H(BaseHTTPRequestHandler):\n"
                    "    def do_GET(s): s.send_response(200); s.end_headers(); s.wfile.write(b'ok')\n"
                    "    def log_message(s, *a): pass\n"
                    f"httpd = HTTPServer(('0.0.0.0', {port}), H)\n"
                    "print('bound', flush=True)\nhttpd.serve_forever()\n")
        c.upload_project_folder(name)
        svc_id = f"demo-svc-{TAG}"
        c.submit_job(TitanJob(job_id=svc_id, filename=f"{name}.zip/server.py",
                              job_type="SERVICE", port=port, is_archive=True))
        print(f"    service deploying on :{port} — it stays up until stopped")
        print(f"    stop it with: client.stop_service('{svc_id}')")
        shutil.rmtree(name, ignore_errors=True)

    print("\nDone. Open:")
    print("  http://localhost:5000/cluster            (Live tab)")
    print("  http://localhost:5000/cluster            (Analytics tab)")
    print("  http://localhost:5000/timeline           (window: fit to all spans)")
    print("\nNote: series live in memory (~10 min at 1s, longer at coarser resolutions) and")
    print("reset when the Master restarts. Re-run this to repopulate.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
