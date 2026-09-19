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
Slot occupancy over time, from the persisted span log.

Efficiency alone cannot tell you *why* a run was slow. A run can be slow because the fleet sat
idle waiting on a critical path, or because the fleet was fully busy and the work simply cost more
than expected. Those demand opposite fixes, and a single percentage conflates them. Occupancy over
time separates them: it shows how many tasks were executing at each moment.

It reads `titan_spans/spans-*.jsonl` rather than the Master's in-memory ring. The ring holds only
the most recent few thousand spans, so any larger run returns just its tail, and a window measured
from that is far too short. That mistake inflated one run's reported efficiency from 40% to 98%.

Two derived quantities matter more than the raw curve:

  observed peak concurrency   the real slot count in effect, which is not the configured one:
                              the autoscaler adds workers mid-run, so "20 slots" may have been
                              12 for the first half.

  mean concurrency            total execution-seconds / wall-seconds. Compare against the peak:
                              close together means the fleet stayed saturated and the cost is in
                              the tasks; far apart means the graph starved it and the cost is in
                              the dependencies.

Usage:
    python titan_test_suite/wf_occupancy.py <pipeline-or-id-fragment> [--bucket-ms 250] [--json]
"""

import argparse
import glob
import json
import os
import sys

SPAN_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "titan_spans")


def load_spans(fragment, span_dir=SPAN_DIR):
    """Every completed span whose id contains `fragment`, from every retained day file."""
    out = []
    for path in sorted(glob.glob(os.path.join(span_dir, "spans-*.jsonl"))):
        with open(path, errors="replace") as fh:
            for line in fh:
                try:
                    s = json.loads(line)
                except ValueError:
                    continue
                if fragment in s.get("id", "") and s.get("started_at") and s.get("ended_at"):
                    out.append(s)
    return out


def occupancy(spans, bucket_ms=250):
    """Concurrency sampled at a fixed interval, plus the exact peak via a sweep line."""
    t0 = min(s["started_at"] for s in spans)
    t1 = max(s["ended_at"] for s in spans)
    n = max(1, int((t1 - t0) / bucket_ms) + 1)

    # Time-weighted series: each bucket holds the AVERAGE number of tasks running during it,
    # computed from how long each task actually overlapped that bucket.
    #
    # Counting a task once per overlapped bucket is wrong whenever tasks are short relative to the
    # bucket. With ~114ms tasks in 500ms buckets it reported a permanently saturated fleet while
    # the true mean concurrency was 12.7 of 21, because a task present for a fifth of a bucket was
    # counted as present for all of it.
    weighted = [0.0] * n
    for s in spans:
        st, en = s["started_at"], s["ended_at"]
        a = max(0, int((st - t0) / bucket_ms))
        b = min(n - 1, int((en - t0) / bucket_ms))
        for i in range(a, b + 1):
            lo = t0 + i * bucket_ms
            hi = lo + bucket_ms
            overlap = min(en, hi) - max(st, lo)
            if overlap > 0:
                weighted[i] += overlap / bucket_ms
    series = [round(v, 2) for v in weighted]

    # Exact peak. The sampled series can overcount at boundaries, so the true peak comes from
    # ordered start/end events instead.
    events = []
    for s in spans:
        events.append((s["started_at"], 1))
        events.append((s["ended_at"], -1))
    events.sort()
    cur = peak = 0
    for _, delta in events:
        cur += delta
        peak = max(peak, cur)

    wall = (t1 - t0) / 1000.0
    exec_s = sum(s["duration_ms"] for s in spans) / 1000.0
    return {
        "t0": t0, "wall_s": round(wall, 2), "buckets": n, "bucket_ms": bucket_ms,
        "series": series,
        "peak_concurrency": peak,
        "mean_concurrency": round(exec_s / wall, 2) if wall else 0,
        "exec_seconds": round(exec_s, 1),
        "saturation_pct": round(100 * (exec_s / wall) / peak) if wall and peak else 0,
    }


def per_worker(spans):
    """Peak concurrency per worker, which should not exceed its configured slot count."""
    out = {}
    for wk in sorted({s.get("worker") for s in spans if s.get("worker")}):
        ev = []
        for s in spans:
            if s.get("worker") == wk:
                ev.append((s["started_at"], 1))
                ev.append((s["ended_at"], -1))
        ev.sort()
        cur = peak = 0
        for _, d in ev:
            cur += d
            peak = max(peak, cur)
        out[wk] = {"tasks": sum(1 for s in spans if s.get("worker") == wk), "peak": peak}
    return out


def sparkline(series, peak):
    blocks = "▁▂▃▄▅▆▇█"
    return "".join(blocks[min(7, int(v / peak * 7.99))] if v > 0.05 else " " for v in series)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("fragment", help="pipeline name or id fragment, e.g. a run stamp")
    ap.add_argument("--bucket-ms", type=int, default=250)
    ap.add_argument("--intended", type=float, default=None,
                    help="total intended task seconds, to attribute the wall clock")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    spans = load_spans(a.fragment)
    if not spans:
        print(f"no completed spans matched '{a.fragment}' in {SPAN_DIR}", file=sys.stderr)
        return 1

    occ = occupancy(spans, a.bucket_ms)
    workers = per_worker(spans)

    if a.json:
        print(json.dumps({"fragment": a.fragment, "spans": len(spans),
                          "workers": workers, **occ}, indent=2))
        return 0

    print(f"\n  spans                 {len(spans)}")
    print(f"  wall clock            {occ['wall_s']}s")
    print(f"  execution-seconds     {occ['exec_seconds']}s")
    print(f"  peak concurrency      {occ['peak_concurrency']}   <- the slot count actually in effect")
    print(f"  mean concurrency      {occ['mean_concurrency']}")
    print(f"  saturation            {occ['saturation_pct']}%   (mean / peak)")
    print("\n  per worker:")
    for wk, v in workers.items():
        print(f"    {wk}: {v['tasks']:>5} tasks, peak {v['peak']} concurrent")
    print(f"\n  occupancy, each character = {a.bucket_ms}ms, height = tasks running:")
    line = sparkline(occ["series"], occ["peak_concurrency"])
    for i in range(0, len(line), 100):
        print("    " + line[i:i + 100])

    # Attribution. Wall time is exec-seconds divided by mean concurrency, so a long run has
    # exactly two causes: the work cost more slot-time than the tasks themselves needed, or the
    # fleet did not stay as busy as it could have. Splitting them says which to go fix.
    if a.intended is not None and occ["mean_concurrency"]:
        intended, peak = a.intended, occ["peak_concurrency"]
        actual = occ["exec_seconds"]
        overhead = actual - intended
        ideal = intended / peak if peak else 0
        with_overhead = actual / peak if peak else 0
        print(f"\n  where the {occ['wall_s']}s went:")
        print(f"    {ideal:>7.1f}s  the work itself, spread across {peak} slots")
        print(f"    {with_overhead - ideal:>+7.1f}s  per-task overhead "
              f"({overhead:.0f}s of slot time over {len(spans)} tasks "
              f"= {1000*overhead/len(spans):.0f}ms each)")
        print(f"    {occ['wall_s'] - with_overhead:>+7.1f}s  fleet not staying full "
              f"(mean {occ['mean_concurrency']} of peak {peak})")

    if occ["saturation_pct"] >= 80:
        print("\n  Fleet stayed saturated. A slow run here is the tasks costing more than expected,")
        print("  not the scheduler starving them. Look at per-task overhead.")
    elif occ["saturation_pct"] >= 50:
        print("\n  Fleet was mostly busy but never consistently full. Both per-task overhead and")
        print("  graph shape are contributing; the split above says in what proportion.")
    else:
        print("\n  Fleet was often idle. The graph's dependencies, not the scheduler, set the pace.")
        print("  Look at the critical path.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
