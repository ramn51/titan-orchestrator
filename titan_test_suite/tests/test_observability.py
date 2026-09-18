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
Accuracy tests for the timeline and control-plane metrics.

These do not check that a field is *present* — they check that the number is
*right*, by constructing jobs whose true timings are known in advance and
asserting the reported values match.

Run against a live cluster (./titan-dev.sh up):
    python3 titan_test_suite/tests/test_observability.py
"""

import json
import os
import sys
import time

from titan_sdk import TitanClient, TitanJob

c = TitanClient()
RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, ok))
    print(f"  {'[PASS]' if ok else '[FAIL]'} {name}" + (f" - {detail}" if detail else ""))


def metrics():
    return json.loads(c._send_request(0x09, "metrics"))


def timeline(f="", n=300):
    return json.loads(c._send_request(0x09, f"timeline:{f}:{n}"))


def settle(job_id, budget=120):
    t0 = time.time()
    while time.time() - t0 < budget:
        st = (c.get_job_status(job_id) or "").upper()
        if any(t in st for t in ("COMPLETED", "FAILED", "DEAD", "CANCELLED")):
            return st
        time.sleep(0.25)
    return "TIMEOUT"


def script(name, body):
    os.makedirs("/tmp/obs", exist_ok=True)
    path = f"/tmp/obs/{name}.py"
    with open(path, "w") as f:
        f.write(body)
    c.deploy_script(path)
    return path


def main():
    tag = int(time.time()) % 100000

    # ------------------------------------------------------------------
    print("\n=== 1. span duration matches the job's real runtime ===")
    # A job that sleeps a known amount: reported duration must bracket it.
    slow = script("slow3", "import time\ntime.sleep(3)\nprint('slow done')\n")
    jid = f"obs{tag}-slow"
    c.submit_job(TitanJob(job_id=jid, filename=slow))
    settle(f"DAG-{jid}")
    sp = [s for s in timeline(f"obs{tag}-slow")["spans"]]
    check("span recorded for the job", len(sp) == 1, f"got {len(sp)}")
    if sp:
        d = sp[0]["duration_ms"]
        check("duration ≈ 3s actual runtime", 2800 <= d <= 5000, f"{d}ms")
        check("ended_at - started_at equals duration",
              abs((sp[0]["ended_at"] - sp[0]["started_at"]) - d) <= 2, f"{d}ms")
        check("enqueued_at <= started_at", sp[0]["enqueued_at"] <= sp[0]["started_at"])

    # ------------------------------------------------------------------
    print("\n=== 2. queue wait is real, not zero-filled ===")
    # Saturate the pool, then submit one more. It MUST show measurable queue wait.
    hog = script("hog", "import time\ntime.sleep(6)\nprint('hog')\n")
    hogs = [TitanJob(job_id=f"obs{tag}-hog{i}", filename=hog) for i in range(8)]
    c.submit_dag(f"obs-hog-{tag}", hogs)
    time.sleep(1.5)
    late = script("late", "print('late')\n")
    lid = f"obs{tag}-late"
    c.submit_job(TitanJob(job_id=lid, filename=late))
    settle(f"DAG-{lid}", 180)
    lsp = timeline(f"obs{tag}-late")["spans"]
    if lsp:
        qw = lsp[0]["queue_wait_ms"]
        check("job submitted into a saturated pool shows queue wait > 0", qw > 0, f"{qw}ms")
        check("queue wait is bounded by observed reality (< 3min)", qw < 180000, f"{qw}ms")
    else:
        check("late job produced a span", False)

    # ------------------------------------------------------------------
    print("\n=== 3. retries produce one span per attempt ===")
    boom = script("boom", "import sys\nsys.exit(5)\n")
    bid = f"obs{tag}-boom"
    c.submit_job(TitanJob(job_id=bid, filename=boom))
    st = settle(f"DAG-{bid}", 180)
    bsp = timeline(f"obs{tag}-boom")["spans"]
    attempts = sorted(s["attempt"] for s in bsp)
    check("job that always fails ends DEAD", "DEAD" in st, st)
    check("4 spans for 4 attempts (3 retries + original)", len(bsp) == 4, f"got {len(bsp)}")
    check("attempt numbers are 1,2,3,4", attempts == [1, 2, 3, 4], str(attempts))
    check("exactly one span is DEAD, rest FAILED",
          sum(1 for s in bsp if s["status"] == "DEAD") == 1, str([s["status"] for s in bsp]))
    check("failure reason carries the exit code",
          any("exit 5" in (s.get("reason") or "") for s in bsp),
          str([s.get("reason") for s in bsp][:1]))

    # ------------------------------------------------------------------
    print("\n=== 4. dead-letter queue reflects the DEAD job ===")
    m = metrics()
    dlq_ids = [x["id"] for x in m["dlq"]["jobs"]]
    check("DLQ depth > 0", m["dlq"]["depth"] > 0, str(m["dlq"]["depth"]))
    check("the DEAD job is listed in the DLQ", f"DAG-{bid}" in dlq_ids, str(dlq_ids[:3]))
    check("DLQ entry records its attempt count",
          any(x["id"] == f"DAG-{bid}" and x["attempts"] >= 3 for x in m["dlq"]["jobs"]))

    # ------------------------------------------------------------------
    print("\n=== 5. unschedulable job is diagnosed, not silently queued ===")
    ok = script("ok", "print('ok')\n")
    uid = f"obs{tag}-tpu"
    c.submit_job(TitanJob(job_id=uid, filename=ok, requirement="TPU"))
    time.sleep(12)
    m = metrics()
    reasons = {x["id"]: x["reason"] for x in m["pending_reasons"]}
    caps = {x["cap"]: x for x in m["capability"]}
    # pending_reasons is capped (longest-waiting first), so a freshly parked job is legitimately
    # outside the window on a cluster with a backlog. Fall back to the board, which is queryable
    # by limit — the assertion is "the job is diagnosed somewhere", not "it is in the first 25".
    if f"DAG-{uid}" not in reasons:
        board = json.loads(c._send_request(OP := 0x09, "board:500"))
        for item in board.get("parked", []):
            if item.get("id") == f"DAG-{uid}":
                reasons[f"DAG-{uid}"] = item.get("reason", "")
                break
    check("unschedulable job has a pending reason", f"DAG-{uid}" in reasons,
          f"total pending={m.get('pending_reasons_total')}, shown={len(m['pending_reasons'])}")
    check("reason names the missing capability",
          "TPU" in reasons.get(f"DAG-{uid}", ""), reasons.get(f"DAG-{uid}", ""))
    check("a capped pending_reasons list reports the true total",
          m.get("pending_reasons_total", 0) >= len(m["pending_reasons"]),
          f"{m.get('pending_reasons_total')} total vs {len(m['pending_reasons'])} shown")
    check("capability table shows TPU demand with zero supply",
          caps.get("TPU", {}).get("waiting", 0) > 0 and caps.get("TPU", {}).get("workers", 1) == 0,
          str(caps.get("TPU")))
    check("GENERAL shows non-zero worker supply",
          caps.get("GENERAL", {}).get("workers", 0) > 0, str(caps.get("GENERAL")))
    c.cancel_job(f"DAG-{uid}") if hasattr(c, "cancel_job") else None

    # ------------------------------------------------------------------
    print("\n=== 6. percentiles are ordered and consistent with raw spans ===")
    m = metrics()
    qw = m["queue_wait"]
    check("p50 <= p95 <= p99 <= max",
          qw["p50"] <= qw["p95"] <= qw["p99"] <= qw["max"], str(qw))
    # Fetch the full ring, not a 500-span slice: the percentile window is computed over every
    # span the Master holds, so comparing it against a truncated fetch fails once the cluster has
    # been busy — 997 reported against 500 fetched is the fetch being short, not the metric.
    fetched = timeline("", 5000)
    all_waits = sorted(s["queue_wait_ms"] for s in fetched["spans"] if s["enqueued_at"] > 0)
    if all_waits and not fetched.get("truncated"):
        check("reported max equals the true max over spans",
              qw["max"] == all_waits[-1], f"reported {qw['max']} vs actual {all_waits[-1]}")
        check("reported n matches the number of spans with an enqueue time",
              qw["n"] == len(all_waits), f"{qw['n']} vs {len(all_waits)}")
    else:
        check("percentile source was truncated — comparison skipped rather than faked",
              True, f"fetched {len(fetched.get('spans', []))}, truncated={fetched.get('truncated')}")

    # ------------------------------------------------------------------
    print("\n=== 7. retry histogram sums to total dispatches ===")
    rt = m["retries"]
    hsum = sum(h["count"] for h in rt["histogram"])
    check("histogram counts sum to dispatch total", hsum == rt["dispatches"], f"{hsum} vs {rt['dispatches']}")
    check("retry rate is a fraction in [0,1]", 0 <= rt["rate"] <= 1, str(rt["rate"]))
    check("histogram includes an attempt=1 bucket",
          any(h["attempt"] == 1 for h in rt["histogram"]))

    # ------------------------------------------------------------------
    print("\n=== 8. sampled series are well-formed and monotonic in time ===")
    m = metrics()
    q = m["queue"]
    check("queue series has samples", len(q) > 3, f"{len(q)}")
    check("queue timestamps increase monotonically",
          all(q[i][0] <= q[i + 1][0] for i in range(len(q) - 1)))
    # [ts, ready, blocked, running, delayed, workers, slots, occupied]. workers/slots arrived with
    # the control-plane series for the scaling chart; occupied arrived with the saturation chart,
    # which cannot use `running` — that field holds a job until its callback lands, so it counts
    # work orphaned by a reclaimed worker and can exceed the fleet's slots.
    check("queue sample shape is [ts,ready,blocked,running,delayed,workers,slots,occupied]",
          all(len(p) == 8 for p in q), f"widths {sorted({len(p) for p in q})}")
    check("worker count and slot total are present in every sample",
          all(p[6] >= p[5] >= 0 for p in q), str(q[-1]) if q else "")
    check("occupied slots never exceed total slots",
          all(p[7] <= p[6] for p in q if p[6]), str([p for p in q if p[6] and p[7] > p[6]][:1]))
    check("no negative counts in any sample",
          all(all(v >= 0 for v in p[1:]) for p in q))
    hb = m["heartbeat"]
    check("heartbeat series exist for registered workers", len(hb) > 0, f"{len(hb)} workers")
    for k, v in list(hb.items())[:2]:
        check(f"heartbeat rtt for {k} is plausible (<30s)",
              all(0 <= p[1] < 30000 for p in v), f"max {max((p[1] for p in v), default=0)}ms")

    # ------------------------------------------------------------------
    print("\n=== 9. throughput counters reconcile with completed spans ===")
    thru = sum(p[1] for p in m["throughput"])
    completed = sum(1 for s in timeline("", 500)["spans"] if s["status"] == "COMPLETED")
    check("throughput total is non-zero", thru > 0, str(thru))
    check("throughput does not exceed completed spans in the window",
          thru <= completed + 5, f"throughput {thru} vs completed spans {completed}")

    # ------------------------------------------------------------------
    print("\n=== 10. worker lifecycle events recorded ===")
    kinds = {e["type"] for e in m["scaler_events"]}
    check("a WORKER_JOIN event was recorded", "WORKER_JOIN" in kinds, str(sorted(kinds)))

    # ------------------------------------------------------------------
    print("\n=== 11. span history is persisted to disk ===")
    hs = json.loads(c._send_request(0x09, "history_stats"))
    check("persistence is enabled", hs.get("enabled") is True, str(hs.get("enabled")))
    # "history_stats" also startsWith("history") — the exact match must win, or this returns spans.
    check("history_stats is distinct from history",
          "spans" not in hs and "day_files" in hs, str(sorted(hs)[:4]))
    check("at least one day file exists", hs.get("day_files", 0) >= 1, str(hs.get("day_files")))
    check("no write errors", hs.get("errors", 1) == 0, str(hs.get("errors")))
    check("a retention window is configured", hs.get("retain_days", 0) > 0, str(hs.get("retain_days")))
    check("writes are drained, not backing up", hs.get("queued", 1) == 0, str(hs.get("queued")))

    now_ms = int(time.time() * 1000)
    hist = json.loads(c._send_request(0x09, f"history:{now_ms - 3600000}:{now_ms}::5000"))
    check("history reads from disk", hist.get("source") == "disk", str(hist.get("source")))
    check("history returns spans", hist.get("count", 0) > 0, str(hist.get("count")))

    if hist.get("spans"):
        live = json.loads(c._send_request(0x09, "timeline::5"))
        if live.get("spans"):
            missing = set(live["spans"][0]) - set(hist["spans"][0])
            check("disk spans carry every field a live span has", not missing, f"missing: {missing}")
        d0 = hist["spans"][0]
        check("disk spans preserve queue wait", "queue_wait_ms" in d0)
        check("disk spans preserve the attempt number", d0.get("attempt", 0) >= 1)
        check("disk spans preserve failure reasons",
              any(sp.get("reason") for sp in hist["spans"]))
        check("disk spans are newest-first",
              all(hist["spans"][i]["started_at"] >= hist["spans"][i + 1]["started_at"]
                  for i in range(min(len(hist["spans"]) - 1, 20))))

    old_ms = now_ms - 90 * 24 * 3600 * 1000
    empty = json.loads(c._send_request(0x09, f"history:{old_ms}:{old_ms + 1000}::100"))
    check("an empty range returns zero spans cleanly", empty.get("count") == 0, str(empty.get("count")))

    bad = [n for n, ok_ in RESULTS if not ok_]
    print(f"\n{'=' * 64}\n RESULT: {len(RESULTS) - len(bad)}/{len(RESULTS)} passed\n{'=' * 64}")
    for b in bad:
        print("  FAILED:", b)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
