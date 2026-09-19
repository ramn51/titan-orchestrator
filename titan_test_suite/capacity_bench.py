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
Titan capacity benchmark.

A single end-to-end throughput number is close to useless for a three-stage pipeline, because it
only ever reports the slowest stage and says nothing about the other two. A system that submits at
2/s and dispatches at 500/s looks identical from the outside to one that submits at 500/s and
dispatches at 2/s, and the fix for each is the opposite of the fix for the other.

So this measures each stage on its own terms:

    client      how fast work can be handed to the Master        submits/sec
    scheduler   how fast the Master can place work on workers    dispatches/sec (serialized)
    worker      how fast a worker can turn a placement into a    jobs/sec/worker
                finished process
    envelope    which of the three binds, for a given job shape, and what the fleet must
                look like to move the binding point

Every stage reports the quantity that is invariant for it, not a number that silently depends on
the other two. The scheduler is measured with surplus slots so workers cannot be the limit. The
worker is measured with a pre-filled queue so the client cannot be the limit.

Usage:
    python titan_test_suite/capacity_bench.py [stage] [--json]

    stage: client | scheduler | worker | envelope | all      (default: all)

Prerequisites:
    - A Master on :9090 and at least one worker registered.
    - For `scheduler`, more workers is better: the measurement wants surplus capacity.
"""

import json
import os
import statistics as st
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from titan_sdk.titan_sdk import TitanClient, TitanJob

OP_STATS = 0x09
OP_SUBMIT_DAG = 0x04   # OP_REGISTER is 0x02; using it here measured a rejection, not a submit

RESULTS = {}
JSON_ONLY = "--json" in sys.argv


# ── output ────────────────────────────────────────────────────────────────────

def head(t):
    if not JSON_ONLY:
        print("\n" + "=" * 68 + f"\n  {t}\n" + "=" * 68)


def line(k, v, unit="", note=""):
    if not JSON_ONLY:
        print(f"  {k:<36} {v:>14} {unit:<12}{note}")


def note(t):
    if not JSON_ONLY:
        print(f"    {t}")


# ── helpers ───────────────────────────────────────────────────────────────────

def metrics(c):
    return json.loads(c._send_request(OP_STATS, "metrics"))


def fleet(c):
    """(workers, total slots) as the Master currently sees them."""
    q = metrics(c)["queue"][-1]
    return q[5], q[6]


def script(body):
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False)
    f.write(body)
    f.close()
    return f.name


def drain(c, timeout=300):
    """Wait until the queue is empty and nothing is running. Returns seconds waited.

    Only safe once work is known to have registered. The queue series is sampled about once a
    second, so immediately after a submit it still reports the PREVIOUS second's empty queue, and
    a naive wait returns instantly. Use await_jobs() for anything being timed.
    """
    t0 = time.time()
    while time.time() - t0 < timeout:
        q = metrics(c)["queue"][-1]
        if q[1] == 0 and q[3] == 0:
            return time.time() - t0
        time.sleep(0.25)
    return time.time() - t0


TERMINAL = {"COMPLETED", "FAILED", "DEAD", "CANCELLED", "ERROR"}


def await_jobs(c, job_ids, timeout=300):
    """Block until every named job reaches a terminal state. Returns (seconds, statuses).

    Polls job status directly rather than the sampled queue gauge, so it cannot mistake a stale
    metrics sample for completion. This is the only correct way to time an execution window.
    """
    t0 = time.time()
    remaining = set(job_ids)
    seen = {}
    while remaining and time.time() - t0 < timeout:
        for jid in list(remaining):
            pref = jid if jid.startswith("DAG-") else f"DAG-{jid}"
            stt = c.get_job_status(pref)
            if stt and stt.upper() in TERMINAL:
                seen[jid] = stt.upper()
                remaining.discard(jid)
        if remaining:
            time.sleep(0.2)
    for jid in remaining:
        seen[jid] = "TIMEOUT"
    return time.time() - t0, seen


def phase_split(c, jobs):
    """Per-job cost of each dispatch phase, from the Master's own counters.

    The first 10% of samples are dropped: JIT warm-up and first-connect effects land there and
    they are not representative of steady state.
    """
    ph = metrics(c).get("dispatch_phases", [])
    if not ph:
        return None
    ph = ph[max(1, len(ph) // 10):]
    names = ["route", "select", "record", "store", "send"]
    tot = [sum(p[i] for p in ph) for i in range(1, 6)]
    return {
        "per_job_ms": {n: round(v / jobs, 4) for n, v in zip(names, tot)},
        "share_pct": {n: round(100 * v / (sum(tot) or 1), 1) for n, v in zip(names, tot)},
        "loop_per_job_ms": round(sum(tot) / jobs, 4),
        "samples": len(ph),
    }


# ── 1. client ─────────────────────────────────────────────────────────────────

def bench_client(n=25, threads=4):
    """How fast can work be handed to the Master?

    Split deliberately into the RPC and everything else. The RPC is the engine's actual admission
    cost; the remainder is SDK bookkeeping, and conflating them has hidden a 600x difference
    between the two before.
    """
    head("CLIENT — how fast work can be handed to the Master")
    c = TitanClient()
    noop = script("pass\n")
    c.deploy_script(noop)
    tag = int(time.time()) % 100000

    rpc, full = [], []
    for i in range(n):
        jobs = [TitanJob(job_id=f"cb{tag}-{i}", filename=noop)]
        payload = " ; ".join(j.to_string() for j in jobs)
        t0 = time.time()
        c._send_request(OP_SUBMIT_DAG, payload)
        t1 = time.time()
        c._write_dag_manifest(f"capbench-{tag}-{i}", jobs, payload)
        t2 = time.time()
        if i >= max(2, n // 10):
            rpc.append((t1 - t0) * 1000)
            full.append((t2 - t0) * 1000)

    manifest_entries = 0
    try:
        with open(".titan_dag_manifest.json") as f:
            manifest_entries = len(json.load(f))
    except (OSError, ValueError):
        pass

    line("submit RPC to Master", f"{st.mean(rpc):.2f}", "ms", "the engine's admission cost")
    line("full submit_dag", f"{st.mean(full):.2f}", "ms", "including SDK bookkeeping")
    line("SDK overhead share", f"{100*(st.mean(full)-st.mean(rpc))/st.mean(full):.1f}", "%")
    line("manifest entries", f"{manifest_entries:,}", "", "SDK cost scales with this")
    line("single-client rate", f"{1000/st.mean(full):.1f}", "submits/s")

    # Concurrency: does the Master's accept path scale, or is the client serialized elsewhere?
    barrier = threading.Barrier(threads)
    counts = [0] * threads
    stop = time.time() + 6

    def worker(idx):
        # Build the payload with TitanJob so the wire format is the real one. A hand-written
        # string is easy to get wrong, and a malformed payload is REJECTED fast, which measures
        # the error path at thousands per second and reads like excellent throughput.
        cc = TitanClient()
        template = TitanJob(job_id="x", filename=noop).to_string()
        barrier.wait()
        k = 0
        while time.time() < stop:
            body = template.replace("x|", f"conc{tag}-{idx}-{k}|", 1)
            r = cc._send_request(OP_SUBMIT_DAG, body)
            if r is None or str(r).startswith(("SERVER_ERROR", "ERROR")):
                break                      # do not report a rejection rate as a throughput
            k += 1
        counts[idx] = k

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    conc_rate = sum(counts) / 6.0
    line(f"RPC rate, {threads} clients", f"{conc_rate:.0f}", "req/s", "Master accept path")
    line("scaling vs single client", f"{conc_rate/(1000/st.mean(rpc)):.2f}", "x",
         "1.0 means no benefit from concurrency")

    RESULTS["client"] = {
        "submit_rpc_ms": round(st.mean(rpc), 3),
        "full_submit_ms": round(st.mean(full), 3),
        "manifest_entries": manifest_entries,
        "single_client_submits_per_s": round(1000 / st.mean(full), 2),
        "concurrent_rpc_per_s": round(conc_rate, 1),
        "concurrency_scaling": round(conc_rate / (1000 / st.mean(rpc)), 2),
    }
    drain(c)
    return RESULTS["client"]


# ── 2. scheduler ──────────────────────────────────────────────────────────────

def bench_scheduler(n=1200):
    """How fast can the Master place work, with workers deliberately not the constraint?

    The dispatch loop is serialized, so its per-job cost is a hard fleet-wide ceiling: no amount of
    added workers can dispatch two jobs at the same instant. That makes 1000/loop_per_job_ms the
    single most important capacity number in the system.
    """
    head("SCHEDULER — the serialized dispatch ceiling")
    c = TitanClient()
    w, slots = fleet(c)
    noop = script("pass\n")
    c.deploy_script(noop)
    tag = int(time.time()) % 100000

    line("fleet during measurement", f"{w} workers / {slots} slots")
    if slots < 8:
        note("few slots: the loop may be waiting on capacity rather than running flat out.")
        note("Re-run with more workers for a cleaner ceiling.")

    t0 = time.time()
    per = 300
    for b in range(0, n, per):
        c.submit_dag(f"schedcap-{tag}-{b}",
                     [TitanJob(job_id=f"sc{tag}-{b+i}", filename=noop)
                      for i in range(min(per, n - b))])
    drain(c)
    wall = time.time() - t0
    time.sleep(1.5)

    ps = phase_split(c, n)
    if not ps:
        note("no dispatch phase samples; is this Master built with phase instrumentation?")
        return None

    for k in ("route", "select", "record", "store", "send"):
        line(f"  {k}", f"{ps['per_job_ms'][k]:.4f}", "ms/job", f"{ps['share_pct'][k]}% of the loop")
    line("dispatch loop total", f"{ps['loop_per_job_ms']:.4f}", "ms/job")
    ceiling = 1000 / ps["loop_per_job_ms"]
    line("SERIALIZED CEILING", f"{ceiling:.0f}", "jobs/s", "fleet-wide, cannot be exceeded")
    line("observed end-to-end", f"{n/wall:.1f}", "jobs/s", "bounded by slots, not the loop")

    RESULTS["scheduler"] = {
        "workers": w, "slots": slots, "jobs": n, "wall_s": round(wall, 2),
        "phase_per_job_ms": ps["per_job_ms"], "phase_share_pct": ps["share_pct"],
        "loop_per_job_ms": ps["loop_per_job_ms"],
        "serialized_ceiling_jobs_per_s": round(ceiling),
        "observed_end_to_end_jobs_per_s": round(n / wall, 1),
    }
    return RESULTS["scheduler"]


# ── 3. worker ─────────────────────────────────────────────────────────────────

def bench_worker(n=240):
    """What does one worker cost per job, and what is its ceiling?

    Measured two ways so the fixed overhead separates from the work itself:
      - no-op jobs      -> the floor: process spawn plus protocol, per job
      - known-sleep jobs -> confirms slots really do overlap, and how efficiently

    The queue is filled before timing starts, so the client cannot be the limit.
    """
    head("WORKER — per-job overhead and per-worker ceiling")
    c = TitanClient()
    w, slots = fleet(c)
    per_worker_slots = (slots // w) if w else 0
    line("fleet during measurement", f"{w} workers / {slots} slots",
         "", f"{per_worker_slots} slots/worker")

    noop = script("pass\n")
    c.deploy_script(noop)
    tag = int(time.time()) % 100000

    # -- floor: no-op jobs. Timed from first submit to last completion, by job status.
    drain(c)
    ids = [f"wn{tag}-{i}" for i in range(n)]
    t0 = time.time()
    c.submit_dag(f"wcap-noop-{tag}",
                 [TitanJob(job_id=j, filename=noop) for j in ids])
    submit_s = time.time() - t0
    exec_s, statuses = await_jobs(c, ids)
    done = sum(1 for v in statuses.values() if v == "COMPLETED")
    rate = done / exec_s if exec_s else 0
    overhead = (slots / rate * 1000) if rate else 0
    line("no-op jobs completed", f"{done}/{n}", "", f"submitted in {submit_s:.1f}s")
    line("fleet no-op rate", f"{rate:.1f}", "jobs/s", "all workers together")
    line("per-worker rate", f"{rate/w if w else 0:.1f}", "jobs/s/worker")
    line("per-job overhead", f"{overhead:.1f}", "ms", "spawn + protocol, per slot")

    # -- overlap: jobs of a known duration must run concurrently, not in series
    dur = 1.0
    sleeper = script(f"import time; time.sleep({dur})\n")
    c.deploy_script(sleeper)
    m = slots * 2
    drain(c)
    sids = [f"ws{tag}-{i}" for i in range(m)]
    t0 = time.time()
    c.submit_dag(f"wcap-sleep-{tag}",
                 [TitanJob(job_id=j, filename=sleeper) for j in sids])
    sleep_wall, sstat = await_jobs(c, sids)
    sleep_wall = time.time() - t0
    sdone = sum(1 for v in sstat.values() if v == "COMPLETED")
    serial = m * dur
    ideal = (m / slots) * dur if slots else 0
    eff = (ideal / sleep_wall * 100) if sleep_wall else 0
    line(f"{m} x {dur}s jobs, wall", f"{sleep_wall:.1f}", "s",
         f"{sdone}/{m} done; serial {serial:.0f}s, ideal {ideal:.0f}s")
    line("slot efficiency", f"{eff:.0f}", "%", "ideal / actual, 100% = perfect overlap")
    line("effective parallelism", f"{serial/sleep_wall if sleep_wall else 0:.1f}", "x",
         f"against {slots} slots")

    RESULTS["worker"] = {
        "workers": w, "slots": slots, "slots_per_worker": per_worker_slots,
        "noop_fleet_jobs_per_s": round(rate, 1),
        "noop_per_worker_jobs_per_s": round(rate / w, 1) if w else 0,
        "per_job_overhead_ms": round(overhead, 1),
        "sleep_wall_s": round(sleep_wall, 2),
        "slot_efficiency_pct": round(eff),
        "effective_parallelism": round(serial / sleep_wall, 2) if sleep_wall else 0,
    }
    return RESULTS["worker"]


# ── 4. envelope ───────────────────────────────────────────────────────────────

def bench_envelope():
    """Combine the three stages into a capacity statement.

    Throughput is min(client, scheduler, worker-fleet). Which one binds depends entirely on job
    duration, so a single "jobs per second" claim is meaningless without stating it.
    """
    head("ENVELOPE — which stage binds, and what the fleet must be")
    cl = RESULTS.get("client")
    sc = RESULTS.get("scheduler")
    wk = RESULTS.get("worker")
    if not (cl and sc and wk):
        note("run all stages first (default), the envelope is derived from them.")
        return None

    ceiling = sc["serialized_ceiling_jobs_per_s"]
    overhead_s = wk["per_job_overhead_ms"] / 1000.0
    spw = wk["slots_per_worker"] or 4

    # Engine admission and SDK bookkeeping are different limits with different fixes, so the
    # envelope must not blend them. The engine's number is what the Master can accept; the SDK's
    # is a client-side cost that scales with local manifest size and is independently fixable.
    admission = cl["concurrent_rpc_per_s"]
    line("engine admission", f"{admission:.0f}", "req/s", "Master accept path, concurrent clients")
    line("scheduler ceiling", f"{ceiling}", "jobs/s", "serialized dispatch, fleet-wide")
    line("worker per-job overhead", f"{wk['per_job_overhead_ms']:.1f}", "ms", "the per-slot floor")
    line("SDK single-client rate", f"{cl['single_client_submits_per_s']:.1f}", "submits/s",
         "client-side, scales with manifest size")
    note("")
    note(f"The binding engine limit is min(admission {admission:.0f}, dispatch {ceiling}) "
         f"= {min(admission, ceiling):.0f} jobs/s,")
    note("then capped by whether the fleet has the slots to keep that many jobs in flight.")

    if not JSON_ONLY:
        print(f"\n  {'job duration':>13} {'slots for ceiling':>18} {'workers':>9} "
              f"{'binds at 1 worker':>19}")
        print("  " + "-" * 64)
    rows = []
    for d in (0.05, 0.1, 0.5, 1.0, 5.0, 30.0):
        eff = d + overhead_s                 # a slot is busy for the work plus the fixed overhead
        slots_needed = ceiling * eff
        workers = slots_needed / spw
        one_worker = spw / eff               # what a single worker sustains at this duration
        engine_limit = min(admission, ceiling)
        binds = "worker" if one_worker < engine_limit else "scheduler"
        rows.append({"job_s": d, "slots_for_ceiling": round(slots_needed),
                     "workers_for_ceiling": round(workers),
                     "one_worker_jobs_per_s": round(one_worker, 1),
                     "binding_stage_at_1_worker": binds})
        if not JSON_ONLY:
            print(f"  {d:>12}s {round(slots_needed):>18} {round(workers):>9} "
                  f"{binds:>19}  ({one_worker:.1f} jobs/s)")

    RESULTS["envelope"] = {
        "engine_admission_req_per_s": admission,
        "scheduler_ceiling_jobs_per_s": ceiling,
        "binding_engine_limit_jobs_per_s": round(min(admission, ceiling)),
        "sdk_single_client_submits_per_s": cl["single_client_submits_per_s"],
        "worker_overhead_ms": wk["per_job_overhead_ms"],
        "slots_per_worker": spw,
        "by_job_duration": rows,
    }
    return RESULTS["envelope"]


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    stage = args[0] if args else "all"
    t0 = time.time()
    if stage in ("client", "all"):
        bench_client()
    if stage in ("scheduler", "all"):
        bench_scheduler()
    if stage in ("worker", "all"):
        bench_worker()
    if stage in ("envelope", "all"):
        bench_envelope()

    if JSON_ONLY:
        print(json.dumps(RESULTS, indent=2))
    else:
        head("SUMMARY")
        if "scheduler" in RESULTS:
            line("scheduler ceiling", f"{RESULTS['scheduler']['serialized_ceiling_jobs_per_s']}",
                 "jobs/s")
        if "worker" in RESULTS:
            line("per worker, no-op", f"{RESULTS['worker']['noop_per_worker_jobs_per_s']}",
                 "jobs/s")
            line("per-job overhead", f"{RESULTS['worker']['per_job_overhead_ms']}", "ms")
        if "client" in RESULTS:
            line("client submit rate", f"{RESULTS['client']['single_client_submits_per_s']}",
                 "submits/s")
        line("total benchmark time", f"{time.time()-t0:.0f}", "s")


if __name__ == "__main__":
    main()
