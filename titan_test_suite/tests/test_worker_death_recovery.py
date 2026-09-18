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
A worker that dies mid-job must not strand the work it was holding.

The bug this pins: a job entered runningJobs at dispatch and left only when its completion
callback arrived. If the worker died first, that callback never came and nothing removed the
entry — the job stayed RUNNING forever, the Master's running count stayed permanently above the
fleet's occupied slots, the span never closed, and any child waiting on that job blocked for good.
Observed live: one lost ephemeral worker stranded four jobs for eleven hours and left a dependent
sink node waiting on a parent that could never complete.

The test kills a worker with SIGKILL while it holds work — no clean shutdown, no goodbye — and
asserts the cluster recovers by itself.

    python3 titan_test_suite/tests/test_worker_death_recovery.py
"""

import glob
import json
import os
import signal
import subprocess
import sys
import time

from titan_sdk import TitanClient, TitanJob

c = TitanClient()
R = []

VICTIM_PORT = int(os.environ.get("TITAN_VICTIM_PORT", "8099"))
MASTER_HOST = os.environ.get("TITAN_MASTER_HOST", "127.0.0.1")
MASTER_PORT = os.environ.get("TITAN_MASTER_PORT", "9090")
# Long enough that jobs are still running when the worker is killed.
JOB_SECONDS = 45
# The reconciliation grace window is 30s; the heartbeat path is faster. Allow for both.
RECOVERY_BUDGET = 90


def check(name, ok, detail=""):
    R.append((name, ok))
    print(f"  {'[PASS]' if ok else '[FAIL]'} {name}" + (f" - {detail}" if detail else ""))


def stats(arg=""):
    return json.loads(c._send_request(0x09, arg))


def sample():
    q = stats("metrics").get("queue", [])
    for pt in reversed(q):
        if len(pt) >= 8:
            return {"running": pt[3], "workers": pt[5], "slots": pt[6], "occupied": pt[7]}
    return {"running": 0, "workers": 0, "slots": 0, "occupied": 0}


def find_jar():
    for pat in ("target/titan-orchestrator-*.jar", "*.jar"):
        for j in sorted(glob.glob(pat)):
            if "original" not in j:
                return j
    return None


def script(name, body):
    os.makedirs("/tmp/wdr", exist_ok=True)
    path = f"/tmp/wdr/{name}.py"
    with open(path, "w") as f:
        f.write(body)
    c.deploy_script(path)
    return path


def main():
    jar = find_jar()
    if not jar:
        print("  no JAR found — build first (mvn package)")
        return 2

    tag = int(time.time()) % 100000
    long_job = script("wdr_long", f"import time\ntime.sleep({JOB_SECONDS})\nprint('done')\n")
    quick = script("wdr_quick", "print('ok')\n")

    # ------------------------------------------------------------------
    print(f"\n=== starting a sacrificial ephemeral worker on :{VICTIM_PORT} ===")
    proc = subprocess.Popen(
        ["java", "-cp", jar, "titan.TitanWorker", str(VICTIM_PORT),
         MASTER_HOST, MASTER_PORT, "GENERAL", "false"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(8)
    before = sample()
    check("the victim worker registered", before["workers"] >= 2,
          f"{before['workers']} workers, {before['slots']} slots")
    if before["workers"] < 2:
        proc.kill()
        return 1

    # ------------------------------------------------------------------
    print("\n=== filling the pool, with a sink that depends on every job ===")
    n = before["slots"]
    parents = [f"wdr{tag}-p{i}" for i in range(n)]
    jobs = [TitanJob(job_id=p, filename=long_job) for p in parents]
    jobs.append(TitanJob(job_id=f"wdr{tag}-sink", filename=quick, parents=parents))
    c.submit_dag(f"worker-death-{tag}", jobs)
    time.sleep(8)

    spans = stats(f"timeline:wdr{tag}:80").get("spans", [])
    on_victim = [s for s in spans if s["worker"] == str(VICTIM_PORT) and not s.get("ended_at")]
    check("work landed on the victim worker", len(on_victim) > 0, f"{len(on_victim)} jobs")
    mid = sample()
    check("running matches occupied while everything is healthy",
          mid["running"] == mid["occupied"], f"running {mid['running']}, occupied {mid['occupied']}")
    if not on_victim:
        proc.kill()
        return 1
    victim_ids = {s["id"] for s in on_victim}

    # ------------------------------------------------------------------
    print(f"\n=== SIGKILL the worker holding {len(victim_ids)} running job(s) ===")
    try:
        os.kill(proc.pid, signal.SIGKILL)
    except OSError:
        pass
    subprocess.run(f"pkill -9 -f 'titan.TitanWorker {VICTIM_PORT}'", shell=True)

    # Wait for the Master to actually notice the death first. Checking running<=occupied straight
    # away passes trivially: the sampler has not yet dropped the dead worker's slots, so both
    # numbers still agree and nothing has been proven.
    t0 = time.time()
    noticed_at = None
    while time.time() - t0 < RECOVERY_BUDGET:
        if sample()["workers"] < before["workers"]:
            noticed_at = time.time() - t0
            break
        time.sleep(1)
    check("the Master detected the dead worker", noticed_at is not None,
          f"still {sample()['workers']} workers after {RECOVERY_BUDGET}s")
    if noticed_at is not None:
        print(f"    death detected {noticed_at:.0f}s after the kill")

    # Only now is the divergence meaningful: the worker is gone, so any job still counted running
    # on it is stranded. Confirm the gap closes, and that it was actually observed to be open.
    saw_gap = False
    recovered_at = None
    t1 = time.time()
    while time.time() - t1 < RECOVERY_BUDGET:
        s = sample()
        if s["running"] > s["occupied"]:
            saw_gap = True
        if s["running"] <= s["occupied"] and time.time() - t1 > 2:
            recovered_at = time.time() - t1
            break
        time.sleep(1)
    after = sample()

    check("the Master stopped counting jobs no worker is running",
          recovered_at is not None,
          f"running {after['running']} vs occupied {after['occupied']}")
    if recovered_at is not None:
        print(f"    accounting consistent {recovered_at:.0f}s after detection"
              + (" (a gap was observed and closed)" if saw_gap else " (no gap ever opened)"))
        check("recovery completed within the heartbeat window",
              recovered_at <= RECOVERY_BUDGET, f"{recovered_at:.0f}s")

    # ------------------------------------------------------------------
    print("\n=== the stranded work is retried, not silently dropped ===")
    deadline = time.time() + 240
    sink = f"DAG-wdr{tag}-sink"
    st = ""
    while time.time() < deadline:
        st = (c.get_job_status(sink) or "").upper()
        if any(x in st for x in ("COMPLETED", "FAILED", "DEAD")):
            break
        time.sleep(3)
    check("the dependent sink node completed instead of blocking forever",
          "COMPLETED" in st, st or "TIMEOUT")

    spans = stats(f"timeline:wdr{tag}:120").get("spans", [])
    dead_spans = [s for s in spans if s["id"] in victim_ids and s["worker"] == str(VICTIM_PORT)]
    check("every span on the dead worker was closed", all(s.get("ended_at") for s in dead_spans),
          f"{sum(1 for s in dead_spans if not s.get('ended_at'))} still open")
    check("closed spans say the worker died, not that the script failed",
          any("died while this job was running" in (s.get("reason") or "")
              or "left the fleet" in (s.get("reason") or "") for s in dead_spans),
          str([(s.get("reason") or "")[:60] for s in dead_spans][:1]))

    retried = [s for s in spans if s["id"] in victim_ids and s["attempt"] > 1]
    check("each stranded job was re-dispatched as a new attempt",
          len(retried) >= len(victim_ids), f"{len(retried)} of {len(victim_ids)}")
    check("the retries ran on a surviving worker",
          all(s["worker"] != str(VICTIM_PORT) for s in retried),
          str({s["worker"] for s in retried}))
    check("the retries succeeded", all(s["status"] == "COMPLETED" for s in retried),
          str({s["status"] for s in retried}))

    final = sample()
    check("running never exceeds occupied once settled",
          final["running"] <= final["occupied"] or final["running"] == 0,
          f"running {final['running']}, occupied {final['occupied']}")

    bad = [n for n, ok in R if not ok]
    print(f"\n{'=' * 66}\n RESULT: {len(R) - len(bad)}/{len(R)} passed\n{'=' * 66}")
    for b in bad:
        print("  FAILED:", b)
    return 1 if bad else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        subprocess.run(f"pkill -9 -f 'titan.TitanWorker {VICTIM_PORT}'", shell=True)
