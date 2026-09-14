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
Readiness-gate and service-discovery tests for the ARCHIVE service path.

The invariant under test:

    When a service deploy job reports COMPLETED, the service is already
    accepting connections on its port.

That matters because completing the job is what unlocks downstream DAG nodes.
If the job completes while the service is still booting, those nodes race the
service's startup, and unlike a client script they have nowhere to put a sleep.

The service used here deliberately sleeps BEFORE binding, which turns a
timing-dependent flake into a deterministic failure.

Run against a live cluster (./titan-dev.sh up), then:
    python3 perm_files/pytests/standard_sdk_tests/test_service_readiness.py
"""

import os
import shutil
import socket
import sys
import threading
import time

from titan_sdk import TitanClient, TitanJob

BOOT_DELAY = 8          # seconds the service sleeps before binding
READY_BUDGET = 60       # how long we let the deploy job settle
SLOW_PORT = 9971
NEVER_PORT = 9972

PASSED, FAILED = [], []


def check(name, condition, detail=""):
    (PASSED if condition else FAILED).append(name)
    print(f"  {'[PASS]' if condition else '[FAIL]'} {name}" + (f" - {detail}" if detail else ""))
    return condition


def port_open(host, port, timeout=1.0):
    """True if something accepts a TCP connection. Same check the Master makes."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def make_service_project(name, port, boot_delay, bind=True):
    """Build a service project that sleeps before binding (or never binds at all)."""
    shutil.rmtree(name, ignore_errors=True)
    os.makedirs(name, exist_ok=True)

    if bind:
        body = f"""
httpd = HTTPServer(('0.0.0.0', {port}), SimpleHandler)
print("[SERVER] Bound and serving on {port}", flush=True)
httpd.serve_forever()
"""
    else:
        # Never binds. Stays alive so the process-exit hook cannot be what fails the deploy.
        body = """
print("[SERVER] Alive but never binding", flush=True)
while True:
    time.sleep(1)
"""

    with open(f"{name}/server.py", "w") as f:
        f.write(f"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys, time

print("[SERVER] Starting, sleeping {boot_delay}s before bind...", flush=True)
time.sleep({boot_delay})

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Titan Service is ALIVE!")
    def log_message(self, *a):
        pass
{body}
""")
    return f"{name}.zip/server.py"


def deploy(client, name, port, boot_delay, bind=True):
    pointer = make_service_project(name, port, boot_delay, bind)
    client.upload_project_folder(name)

    job = TitanJob(
        job_id=f"SVC-{name}-{int(time.time())}",
        filename=pointer,
        job_type="SERVICE",
        port=port,
        is_archive=True,
    )
    client.submit_job(job)
    # submit_job() wraps a single job into a DAG, so the scheduler's ID is DAG-prefixed.
    return f"DAG-{job.id}"


def settle(client, job_id, budget=READY_BUDGET):
    """Poll until the deploy job leaves the in-flight states. Returns (status, elapsed)."""
    start = time.time()
    terminal = ("COMPLETED", "FAILED", "DEAD", "CANCELLED")
    last = "UNKNOWN"
    while time.time() - start < budget:
        last = (client.get_job_status(job_id) or "UNKNOWN").strip().upper()
        if any(t in last for t in terminal):
            return last, time.time() - start
        time.sleep(0.25)
    return last, time.time() - start


def main():
    client = TitanClient()
    print(f"\n{'=' * 68}\n TEST 1: deploy completes only once the port is listening\n{'=' * 68}")
    print(f"  Service sleeps {BOOT_DELAY}s before binding on port {SLOW_PORT}.")

    # Watch the port in the background so we know the exact moment it starts listening,
    # independently of how coarsely we poll job status.
    port_open_at = {}

    def watch():
        t0 = time.time()
        while time.time() - t0 < READY_BUDGET:
            if port_open("127.0.0.1", SLOW_PORT, timeout=0.25):
                port_open_at["t"] = time.time()
                return
            time.sleep(0.05)

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    t_submit = time.time()
    job_id = deploy(client, "svc_slowboot", SLOW_PORT, BOOT_DELAY)
    status, elapsed = settle(client, job_id)
    t_complete = time.time()
    watcher.join(timeout=2)

    bound_at = port_open_at.get("t")
    print(f"  Job {job_id} -> {status} after {elapsed:.1f}s")
    if bound_at:
        print(f"  port bound at  +{bound_at - t_submit:.1f}s")
        print(f"  job completed  +{t_complete - t_submit:.1f}s")

    check("deploy job reached COMPLETED", "COMPLETED" in status, status)
    check("port was observed listening", bound_at is not None)
    check("job completed AFTER the port was listening (the invariant)",
          bound_at is not None and t_complete >= bound_at,
          f"gap={t_complete - bound_at:.1f}s" if bound_at else "port never opened")
    check("port is listening the instant the job completes",
          port_open("127.0.0.1", SLOW_PORT))

    print(f"\n{'=' * 68}\n TEST 2: the service is discoverable by address\n{'=' * 68}")
    addr = client.get_service_address(job_id)
    print(f"  get_service_address({job_id}) -> {addr}")
    check("address resolves", addr is not None)
    if addr:
        check("resolved port matches the deployed port", addr[1] == SLOW_PORT, f"got {addr[1]}")
        check("resolved address is actually reachable", port_open(addr[0], addr[1]))
    url = client.get_service_url(job_id)
    check("get_service_url returns a usable URL", bool(url) and str(SLOW_PORT) in str(url), str(url))
    check("service appears in list_services()", job_id in client.list_services())

    print(f"\n{'=' * 68}\n TEST 3: a service that never binds fails the deploy\n{'=' * 68}")
    never_id = deploy(client, "svc_neverbind", NEVER_PORT, 1, bind=False)
    status2, elapsed2 = settle(client, never_id, budget=120)
    print(f"  Job {never_id} -> {status2} after {elapsed2:.1f}s")
    check("non-binding service does NOT report COMPLETED",
          "COMPLETED" not in status2, status2)
    check("failure surfaces within a bounded window", elapsed2 < 120, f"elapsed={elapsed2:.1f}s")
    check("no discovery record for a service that never became ready",
          client.get_service_address(never_id) is None)

    print(f"\n{'=' * 68}\n TEST 4: teardown clears the discovery record\n{'=' * 68}")
    client.stop_service(job_id)
    time.sleep(2)
    check("address is gone after stop_service", client.get_service_address(job_id) is None)
    check("service removed from list_services()", job_id not in client.list_services())

    for d in ("svc_slowboot", "svc_neverbind"):
        shutil.rmtree(d, ignore_errors=True)

    print(f"\n{'=' * 68}\n RESULT: {len(PASSED)} passed, {len(FAILED)} failed\n{'=' * 68}")
    for f in FAILED:
        print(f"  FAILED: {f}")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
