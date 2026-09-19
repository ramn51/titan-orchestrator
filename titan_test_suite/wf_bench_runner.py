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
Run a published WfCommons instance on Titan and derive numbers from it.

The point of using a published instance is that it carries its own ground truth, so every number
below is absolute rather than a comparison against another system:

  CORRECTNESS   the instance states exactly which tasks exist and which depends on which, so
                "every task ran once" and "no child started before its parent finished" are
                checkable facts, not opinions. The dependency check is the strong one: it is
                verified against every edge in the real workflow.

  PERFORMANCE   the instance defines its own floors. The critical path is the wall time with
                infinite slots; total-work/slots is the wall time with no dependencies at all.
                The larger of the two is the best any scheduler could do on this fleet, so
                floor/actual is a scheduling efficiency that needs no second tool to interpret.

  OVERHEAD      dependency-release latency (parent ends -> child starts) isolates the scheduler,
                and span-minus-intended isolates the worker. Those two separate "our scheduler is
                slow" from "our process spawn is slow", which a single throughput number cannot.

Usage:
    python titan_test_suite/wf_bench_runner.py <instance.json> [--scale F] [--mode sleep|cpu]
                                               [--limit N] [--json]

Prerequisites: a Master and at least one worker. More slots gives a lower floor, not a better
score; efficiency is measured against whatever fleet is actually present.
"""

import argparse
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from wfformat_to_titan import load_wfformat, limit_tasks, generate, check_acyclic  # noqa: E402
from titan_sdk.titan_sdk import TitanClient, TitanJob                              # noqa: E402

OP_STATS = 0x09
TERMINAL = {"COMPLETED", "FAILED", "DEAD", "CANCELLED", "ERROR"}


def fleet(c):
    q = json.loads(c._send_request(OP_STATS, "metrics"))["queue"][-1]
    return q[5], q[6]


def bulk_status(c, ids):
    """Statuses for many jobs in a few round trips, chunked to stay well under the frame cap.

    One RPC per job does not scale: a 700-task workflow meant 700 round trips per poll cycle, and
    the harness became slower than the system it was measuring.
    """
    out = {}
    ids = list(ids)
    for i in range(0, len(ids), 120):
        chunk = ids[i:i + 120]
        raw = c._send_request(OP_STATS, "status:" + ",".join(
            j if j.startswith("DAG-") else f"DAG-{j}" for j in chunk))
        if not raw:
            continue
        start = raw.find("{")
        if start == -1:
            continue
        try:
            got = json.loads(raw[start:])
        except json.JSONDecodeError:
            continue
        for j in chunk:
            v = got.get(j) or got.get(f"DAG-{j}")
            if v:
                out[j] = str(v).upper()
    return out


def await_all(c, ids, timeout):
    """Poll until every job is terminal, in bulk. Statuses, not the sampled queue gauge."""
    t0, remaining, seen = time.time(), set(ids), {}
    while remaining and time.time() - t0 < timeout:
        got = bulk_status(c, remaining)
        for jid, st in got.items():
            if st in TERMINAL:
                seen[jid] = st
                remaining.discard(jid)
        if remaining:
            time.sleep(0.5)
    for jid in remaining:
        seen[jid] = "TIMEOUT"
    return seen


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("instance")
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--mode", choices=("sleep", "cpu"), default="sleep")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--min-s", type=float, default=0.05)
    ap.add_argument("--timeout", type=float, default=900)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    inst = load_wfformat(a.instance)
    if check_acyclic(inst["tasks"]):
        print("instance contains a cycle", file=sys.stderr)
        sys.exit(2)
    if a.limit:
        inst["tasks"] = limit_tasks(inst["tasks"], a.limit)

    import tempfile
    outdir = tempfile.mkdtemp(prefix="wfrun_")
    jobs, stats = generate(inst, outdir, a.scale, a.min_s, 0.0, a.mode)

    c = TitanClient()
    workers, slots = fleet(c)
    stamp = int(time.time()) % 1000000
    pipeline = f"wfb-{inst['name']}-{stamp}"
    tjobs = [TitanJob(job_id=f"{j['job_id']}-{stamp}", filename=j["filename"],
                      parents=[f"{p}-{stamp}" for p in j["parents"]]) for j in jobs]
    intended = {f"{j['job_id']}-{stamp}": j["secs"] for j in jobs}
    parents = {f"{j['job_id']}-{stamp}": [f"{p}-{stamp}" for p in j["parents"]] for j in jobs}

    if not a.json:
        print(f"\n  {inst['name']}  ({stats['tasks']} tasks, {stats['edges']} edges, "
              f"depth {stats['depth']}, widest {stats['max_width']})")
        print(f"  fleet {workers} workers / {slots} slots · scale {a.scale} · mode {a.mode}")

    t0 = time.time()
    if a.json:
        # The SDK prints progress to stdout, which would land in the middle of the JSON document
        # and make the output unparseable. Capture it for the duration of the call.
        import io, contextlib
        with contextlib.redirect_stdout(io.StringIO()):
            c.submit_dag(pipeline, tjobs)
    else:
        c.submit_dag(pipeline, tjobs)
    submit_s = time.time() - t0
    statuses = await_all(c, list(intended), a.timeout)
    wall_client = time.time() - t0

    # Spans are the authoritative record of what actually ran.
    #
    # Read from the PERSISTED span log, not the in-memory ring. The ring holds the most recent
    # ~5,000 spans, so a larger run silently returns only its tail: the earliest starts are missing,
    # the measured window collapses, and efficiency comes out far too high. A 9,805-task run
    # reported 63.4s against a true 125.9s that way.
    time.sleep(1.5)
    spans = {}
    now = int(time.time() * 1000)
    frm = now - 6 * 3600 * 1000
    raw = c._send_request(OP_STATS, f"history:{frm}:{now}::{max(len(intended) * 2, 2000)}")
    try:
        payload = json.loads(raw[raw.find("{"):]) if raw and "{" in raw else {}
    except json.JSONDecodeError:
        payload = {}
    for s in payload.get("spans", []):
        base = s["id"][4:] if s["id"].startswith("DAG-") else s["id"]
        if base in intended:
            spans[base] = s
    if len(spans) < len(intended):                 # fall back to the ring if disk is disabled
        for s in json.loads(c._send_request(OP_STATS, "timeline::6000"))["spans"]:
            base = s["id"][4:] if s["id"].startswith("DAG-") else s["id"]
            if base in intended and base not in spans:
                spans[base] = s
    if len(spans) < len(intended) and not a.json:
        print(f"\n  NOTE: only {len(spans)} of {len(intended)} spans were retrievable; "
              f"wall clock and efficiency below are computed from those and may be optimistic.")

    completed = sum(1 for v in statuses.values() if v == "COMPLETED")
    # -- correctness
    order_checked = order_violations = 0
    for jid, ps in parents.items():
        if jid not in spans:
            continue
        for p in ps:
            if p in spans:
                order_checked += 1
                if spans[jid]["started_at"] < spans[p]["ended_at"]:
                    order_violations += 1

    # -- performance, against floors the instance defines
    if spans:
        st = min(s["started_at"] for s in spans.values())
        en = max(s["ended_at"] or s["started_at"] for s in spans.values())
        wall = (en - st) / 1000.0
    else:
        wall = wall_client
    total_work = sum(intended.values())
    crit = stats["critical_path_seconds"]

    # Use the slot count that was ACTUALLY in effect, taken from peak observed concurrency, not
    # the count configured at submit time. The autoscaler adds workers mid-run, so the configured
    # figure understates the fleet, which inflates the floor and can push "efficiency" above 100%.
    events = []
    for sp in spans.values():
        events.append((sp["started_at"], 1))
        events.append((sp["ended_at"], -1))
    events.sort()
    cur = observed_peak = 0
    for _, d in events:
        cur += d
        observed_peak = max(observed_peak, cur)
    effective_slots = max(observed_peak, slots)

    slot_floor = total_work / effective_slots if effective_slots else total_work
    floor = max(crit, slot_floor)
    eff = (floor / wall * 100) if wall else 0
    mean_conc = (sum(sp["duration_ms"] for sp in spans.values()) / 1000.0 / wall) if wall else 0

    # -- overhead: scheduler vs worker
    gaps = []
    for jid, ps in parents.items():
        if jid not in spans:
            continue
        ready = [spans[p]["ended_at"] for p in ps if p in spans]
        if ready:
            g = spans[jid]["started_at"] - max(ready)
            if g >= 0:
                gaps.append(g)
    gaps.sort()
    over = sorted(spans[j]["duration_ms"] / 1000.0 - intended[j] for j in intended if j in spans)

    def pct(v, q):
        return v[min(len(v) - 1, int(len(v) * q))] if v else 0

    res = {
        "instance": inst["name"], "pipeline": pipeline,
        "tasks": stats["tasks"], "edges": stats["edges"], "depth": stats["depth"],
        "max_width": stats["max_width"], "scale": a.scale, "mode": a.mode,
        "workers": workers, "slots": slots,
        "submit_s": round(submit_s, 2),
        "completed": completed, "spans": len(spans),
        "all_completed": completed == len(intended),
        "dependency_edges_checked": order_checked,
        "order_violations": order_violations,
        "wall_s": round(wall, 2),
        "total_work_s": round(total_work, 1),
        "critical_path_s": round(crit, 1),
        "slot_floor_s": round(slot_floor, 1),
        "configured_slots": slots,
        "observed_peak_concurrency": observed_peak,
        "effective_slots": effective_slots,
        "mean_concurrency": round(mean_conc, 2),
        "floor_s": round(floor, 1),
        "achieved_speedup": round(total_work / wall, 1) if wall else 0,
        "max_speedup": stats["max_speedup"],
        "scheduling_efficiency_pct": round(eff),
        "dep_release_p50_ms": pct(gaps, 0.50), "dep_release_p90_ms": pct(gaps, 0.90),
        "dep_release_max_ms": gaps[-1] if gaps else 0, "dep_release_n": len(gaps),
        "task_overhead_p50_ms": round(pct(over, 0.50) * 1000),
        "task_overhead_p90_ms": round(pct(over, 0.90) * 1000),
    }

    if a.json:
        print(json.dumps(res, indent=2))
    else:
        print(f"\n  CORRECTNESS")
        print(f"    tasks completed            {completed}/{len(intended)}")
        print(f"    spans produced             {len(spans)}")
        print(f"    dependency edges checked   {order_checked}")
        print(f"    ORDER VIOLATIONS           {order_violations}   (must be 0)")
        print(f"\n  PERFORMANCE (floors come from the instance)")
        print(f"    wall clock                 {wall:.1f}s")
        print(f"    total task work            {total_work:.1f}s")
        print(f"    critical path              {crit:.1f}s")
        print(f"    slot floor ({effective_slots} slots)     {slot_floor:.1f}s"
              + (f"   [configured {slots}, observed peak {observed_peak}]" if observed_peak != slots else ""))
        print(f"    mean concurrency           {mean_conc:.1f} of {effective_slots}")
        print(f"    achieved speed-up          {res['achieved_speedup']}x  of {stats['max_speedup']}x max")
        print(f"    SCHEDULING EFFICIENCY      {eff:.0f}%")
        print(f"\n  OVERHEAD")
        print(f"    dependency release         p50 {res['dep_release_p50_ms']}ms  "
              f"p90 {res['dep_release_p90_ms']}ms  max {res['dep_release_max_ms']}ms  (n={len(gaps)})")
        print(f"    per-task (worker)          p50 {res['task_overhead_p50_ms']}ms  "
              f"p90 {res['task_overhead_p90_ms']}ms")
    return 0 if (res["all_completed"] and order_violations == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
