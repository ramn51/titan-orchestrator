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
Does parallel execution actually run in parallel, and is every delay explained?

The claim under test is narrow and falsifiable:

    A job waits ONLY because no slot is free. There is no per-job scheduling tax.

So the test measures, from real spans:

  1. sub-capacity     fewer jobs than slots  -> all start together, queue wait ~ 0
  2. over-capacity    more jobs than slots   -> the first `slots` start immediately, the rest
                                               wait one wave, and concurrency never exceeds slots
  3. dependency chain                        -> the gap between a parent finishing and its child
                                               starting IS the dispatch latency; it must be small
                                               and must not grow with depth
  4. overhead budget                         -> total queue wait, minus the part arithmetic says
                                               saturation must cause, is the scheduler's own tax

Run against a live cluster (./titan-dev.sh up):
    python3 titan_test_suite/tests/test_parallelism.py
"""

import json
import os
import sys
import time

from titan_sdk import TitanClient, TitanJob

c = TitanClient()
R = []

# A job long enough that dispatch latency is a small fraction of it, short enough to iterate.
DUR = 3.0
# Dispatch is push-based over a socket, so a healthy gap is tens of milliseconds. The ceiling is
# deliberately loose: this asserts "no structural stall", not a latency SLO on a laptop.
DISPATCH_CEILING_MS = 1500


def check(name, ok, detail=""):
    R.append((name, ok))
    print(f"  {'[PASS]' if ok else '[FAIL]'} {name}" + (f" - {detail}" if detail else ""))


def stats(arg=""):
    return json.loads(c._send_request(0x09, arg))


def slots():
    """Total concurrent job slots, from the queue series.

    NOT from summing worker_load: that map keeps a departed worker's series for a grace period,
    so summing it counts slots belonging to nodes that no longer exist — it read 20 slots against
    a real 8 after the autoscaler reclaimed three ephemerals. The queue series' slot field is
    sampled straight from the registry each tick, so it is the authoritative capacity.
    """
    q = stats("metrics").get("queue", [])
    for pt in reversed(q):
        if len(pt) >= 7:
            return pt[6]
    return 0


def script(name, body):
    os.makedirs("/tmp/par", exist_ok=True)
    path = f"/tmp/par/{name}.py"
    with open(path, "w") as f:
        f.write(body)
    c.deploy_script(path)
    return path


def settle(job_id, budget=300):
    t0 = time.time()
    while time.time() - t0 < budget:
        st = (c.get_job_status(job_id) or "").upper()
        if any(t in st for t in ("COMPLETED", "FAILED", "DEAD", "CANCELLED")):
            return st
        time.sleep(0.25)
    return "TIMEOUT"


def spans_for(prefix, expect, budget=300):
    """Spans whose id contains `prefix`, once `expect` of them have an end time."""
    t0 = time.time()
    last = []
    while time.time() - t0 < budget:
        sp = stats(f"timeline:{prefix}:2000").get("spans", [])
        last = sp
        if len([s for s in sp if s.get("ended_at")]) >= expect:
            return sp
        time.sleep(0.5)
    return last


def peak_concurrency(sp):
    ev = []
    for s in sp:
        if not s.get("ended_at"):
            continue
        ev.append((s["started_at"], 1))
        ev.append((s["ended_at"], -1))
    ev.sort(key=lambda x: (x[0], x[1]))
    cur = peak = 0
    for _, d in ev:
        cur += d
        peak = max(peak, cur)
    return peak


def main():
    tag = int(time.time()) % 100000
    N = slots()
    if N < 2:
        print(f"  need at least 2 job slots to measure parallelism, found {N}")
        return 2
    print(f"\n=== cluster offers {N} concurrent job slots ===")

    work = script("par_work", f"import time\ntime.sleep({DUR})\nprint('done')\n")

    # ------------------------------------------------------------------
    # 1. sub-capacity: fewer jobs than slots. Nothing may wait.
    # ------------------------------------------------------------------
    sub = max(2, N - 1)
    print(f"\n=== 1. {sub} independent jobs into {N} free slots — nothing should wait ===")
    pre = f"par{tag}a-"
    t0 = time.time()
    c.submit_dag(f"par-sub-{tag}", [TitanJob(job_id=f"{pre}{i}", filename=work) for i in range(sub)])
    settle(f"DAG-{pre}{sub - 1}", 180)
    sp = spans_for(pre, sub, 120)
    wall = time.time() - t0

    check("every job produced a span", len(sp) >= sub, f"{len(sp)}/{sub}")
    if len(sp) >= sub:
        waits = sorted(s["queue_wait_ms"] for s in sp)
        skew = max(s["started_at"] for s in sp) - min(s["started_at"] for s in sp)
        peak = peak_concurrency(sp)
        busy = sum(s["duration_ms"] for s in sp if s.get("ended_at"))
        print(f"    queue waits: {waits}")
        print(f"    start skew {skew}ms · peak concurrency {peak}/{sub} · wall {wall:.1f}s")
        check("no job waited longer than the dispatch ceiling",
              waits[-1] <= DISPATCH_CEILING_MS, f"max {waits[-1]}ms")
        check("all jobs started within one dispatch window of each other",
              skew <= DISPATCH_CEILING_MS, f"{skew}ms skew")
        check("they ran concurrently, not serially", peak == sub, f"peak {peak} of {sub}")
        check("wall clock is one job's duration, not the sum",
              wall < DUR * sub * 0.75, f"{wall:.1f}s vs serial {DUR * sub:.1f}s")
        check("parallelism factor reaches the job count",
              busy / max(wall * 1000, 1) > sub * 0.6,
              f"{busy / max(wall * 1000, 1):.2f}x")

    # ------------------------------------------------------------------
    # 2. over-capacity: waiting is REQUIRED, and must be exactly one wave's worth.
    # ------------------------------------------------------------------
    over = N * 3
    print(f"\n=== 2. {over} jobs into {N} slots — waiting is required, and must be wave-shaped ===")
    pre = f"par{tag}b-"
    cap_before = N
    t0 = time.time()
    c.submit_dag(f"par-over-{tag}", [TitanJob(job_id=f"{pre}{i}", filename=work) for i in range(over)])
    settle(f"DAG-{pre}{over - 1}", 300)
    sp = spans_for(pre, over, 240)
    wall = time.time() - t0

    check("every over-capacity job produced a span", len(sp) >= over, f"{len(sp)}/{over}")
    if len(sp) >= over:
        sp = sorted(sp, key=lambda s: s["started_at"])
        waits = [s["queue_wait_ms"] for s in sp]
        peak = peak_concurrency(sp)
        immediate = [w for w in waits if w <= DISPATCH_CEILING_MS]
        spawn_spans = [sp2 for sp2 in stats("timeline:WRK-:50").get("spans", [])
                       if sp2.get("ended_at") and sp2["ended_at"] >= int(t0 * 1000)]
        print(f"    queue waits, dispatch order: {waits}")
        print(f"    peak concurrency {peak}/{N} slots · wall {wall:.1f}s "
              f"(serial would be {DUR * over:.0f}s)")

        # Capacity is not fixed while the autoscaler is enabled: a saturated pool is exactly the
        # condition that spawns another worker, so the slot count read before the burst can be
        # smaller than the one that was in force during it. Compare against the largest capacity
        # the cluster actually reported over the window, from the sampled queue series.
        seen_slots = [pt[6] for pt in stats("metrics").get("queue", []) if len(pt) >= 7]
        cap_max = max(seen_slots + [N])
        if cap_max > N:
            print(f"    capacity grew mid-test: {N} -> {cap_max} slots (autoscaler)")
        check("concurrency never exceeded the slot count (no overcommit)",
              peak <= cap_max, f"peak {peak} vs {cap_max} slots at their widest")
        check("the pool was actually filled", peak >= N, f"peak {peak}, opened with {N}")
        # Capacity is NOT stable across this burst and cannot be made so: saturation is the
        # scaler's trigger, so the load required to test queueing is the same load that adds and
        # reclaims workers underneath it. A fixed "wave = slots" model is therefore wrong — the
        # pre-read slot count can be stale by a sample, and the pool genuinely changes mid-run.
        #
        # What IS invariant regardless of capacity changes is the SHAPE: jobs are admitted in
        # groups, and each group waits about one job-duration longer than the group before it.
        # That step function is the signature of "waiting only because a slot was busy". A
        # per-job scheduling tax would instead show a continuous upward drift within a group.
        groups = []
        for w in waits:
            if groups and w - groups[-1][-1] <= DISPATCH_CEILING_MS:
                groups[-1].append(w)
            else:
                groups.append([w])
        widths = [len(g) for g in groups]
        steps = [groups[i + 1][0] - groups[i][0] for i in range(len(groups) - 1)]
        print(f"    admitted in {len(groups)} group(s) of {widths}")
        print(f"    step between groups: {steps} ms (one job = {int(DUR * 1000)}ms)")

        check("jobs were admitted in groups, not one at a time",
              max(widths) > 1, f"widest group {max(widths)}")
        check("the first group waited essentially nothing",
              groups[0][0] <= DISPATCH_CEILING_MS, f"{groups[0][0]}ms")
        # The step-function shape is only well defined while capacity holds still. If the scaler
        # added or reclaimed a worker during the burst, the groups mix two pool sizes and the
        # shape says nothing — so the measurement is reported and these assertions are skipped
        # OUT LOUD, rather than loosened until they can never fail.
        seen = [pt[6] for pt in stats("metrics").get("queue", []) if len(pt) >= 7]
        cap_changed = bool(seen) and (max(seen) != cap_before or min(seen) != cap_before)
        if cap_changed:
            print(f"    SKIPPED the wave-shape assertions: capacity moved during the burst "
                  f"({cap_before} -> {min(seen)}..{max(seen)} slots). The shape is only defined "
                  f"at constant capacity — re-run with the scaler at rest to assert it.")
        else:
            check("within a group, waits do not drift — no per-job tax",
                  all(g[-1] - g[0] <= DISPATCH_CEILING_MS for g in groups),
                  f"worst intra-group spread {max(g[-1] - g[0] for g in groups)}ms")
        if steps and not cap_changed:
            # Each step should be about one job duration: a slot frees, the next job takes it.
            # A scale-up occupies the loop for its spawn, so allow that on top.
            spawn_spans = [sp2 for sp2 in stats("timeline:WRK-:50").get("spans", [])
                           if sp2.get("ended_at") and sp2["ended_at"] >= int(t0 * 1000)]
            spawn_cost = sum(sp2["duration_ms"] for sp2 in spawn_spans)
            if spawn_spans:
                print(f"    {len(spawn_spans)} scale-up(s) cost the dispatch loop {spawn_cost}ms "
                      f"({', '.join(str(x['duration_ms']) + 'ms' for x in spawn_spans)})")
            allowance = DUR * 1000 + DISPATCH_CEILING_MS + spawn_cost
            worst = max(steps)
            check("each group starts about one job-duration after the previous one",
                  worst <= allowance,
                  f"worst step {worst}ms vs allowance {allowance:.0f}ms")
            check("no step is shorter than a job — groups are gated on completion, not a timer",
                  min(steps) >= DUR * 1000 * 0.5, f"shortest step {min(steps)}ms")

        check("throughput beat serial execution substantially",
              (DUR * over) / max(wall, 0.1) > 2.0,
              f"{(DUR * over) / max(wall, 0.1):.1f}x serial")

    # ------------------------------------------------------------------
    # 3. dependency chain: the parent-end to child-start gap IS dispatch latency.
    # ------------------------------------------------------------------
    depth = 6
    print(f"\n=== 3. a {depth}-deep chain into an idle pool — gap after each parent is pure latency ===")
    quick = script("par_quick", "print('ok')\n")
    pre = f"par{tag}c-"
    chain = []
    prev = None
    for i in range(depth):
        jid = f"{pre}{i}"
        chain.append(TitanJob(job_id=jid, filename=quick, parents=[prev] if prev else []))
        prev = jid
    c.submit_dag(f"par-chain-{tag}", chain)
    settle(f"DAG-{prev}", 240)
    sp = spans_for(pre, depth, 120)

    check("every chain node produced a span", len(sp) >= depth, f"{len(sp)}/{depth}")
    if len(sp) >= depth:
        by = {s["id"]: s for s in sp}
        gaps = []
        for i in range(1, depth):
            child, parent = by.get(f"DAG-{pre}{i}"), by.get(f"DAG-{pre}{i - 1}")
            if child and parent and parent.get("ended_at"):
                gaps.append(child["started_at"] - parent["ended_at"])
        print(f"    parent-end -> child-start gaps: {gaps}")
        check("a child never starts before its parent finished", all(g >= -50 for g in gaps),
              str(gaps))
        check("dependency release is prompt at every level",
              gaps and max(gaps) <= DISPATCH_CEILING_MS, f"worst {max(gaps) if gaps else '?'}ms")
        if len(gaps) >= 4:
            early, late = sum(gaps[:2]) / 2.0, sum(gaps[-2:]) / 2.0
            check("release latency does not grow with depth",
                  late <= max(early * 3, early + 500), f"first {early:.0f}ms vs last {late:.0f}ms")
        avg = sum(gaps) / max(len(gaps), 1)
        print(f"    mean dependency-release latency: {avg:.0f}ms")

    # ------------------------------------------------------------------
    # 4. what the whole run cost in scheduling, versus in work
    # ------------------------------------------------------------------
    print("\n=== 4. scheduling overhead across everything just run ===")
    all_sp = stats(f"timeline:par{tag}:2000").get("spans", [])
    done = [s for s in all_sp if s.get("ended_at")]
    if done:
        busy = sum(s["duration_ms"] for s in done)
        qsum = sum(s["queue_wait_ms"] for s in done)
        ratio = qsum / max(busy, 1)
        print(f"    {len(done)} spans · {busy}ms executing · {qsum}ms queued · "
              f"queue/exec {ratio:.2f}")
        m = stats("metrics").get("dispatch", [])
        if m:
            recent = [pt[1] for pt in m[-60:]]
            print(f"    dispatch loop latency, last 60 samples: "
                  f"min {min(recent)}ms max {max(recent)}ms")
            check("dispatch loop latency stayed bounded", max(recent) < 5000,
                  f"max {max(recent)}ms")
        check("queue time is attributable, not unbounded", ratio < 4, f"{ratio:.2f}")

    bad = [n for n, ok in R if not ok]
    print(f"\n{'=' * 66}\n RESULT: {len(R) - len(bad)}/{len(R)} passed · {N} slots\n{'=' * 66}")
    for b in bad:
        print("  FAILED:", b)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
