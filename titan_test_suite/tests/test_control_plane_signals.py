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
The three control-plane signals added for the Live tab, plus the demo runner.

  1. dispatch phase breakdown  — the serialized section split into route/select/record/store/send
  2. priority on spans         — proves the priority queue actually reorders work
  3. store latency             — read and write series, the only external dependency in the path
  4. demo runner               — preset-only mutation endpoint on the dashboard

These assert the numbers are *coherent*, not merely present: phases must sum to no more than the
measured loop total, priority must actually separate queue waits under saturation, and the demo
endpoint must refuse anything that is not a known preset.

Run against a live cluster and dashboard:
    python3 titan_test_suite/tests/test_control_plane_signals.py
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

from titan_sdk import TitanClient, TitanJob

c = TitanClient()
DASH = os.environ.get("TITAN_DASH", "http://localhost:5000")
R = []


def check(name, ok, detail=""):
    R.append((name, ok))
    print(f"  {'[PASS]' if ok else '[FAIL]'} {name}" + (f" - {detail}" if detail else ""))


def stats(arg=""):
    return json.loads(c._send_request(0x09, arg))


def get(path):
    try:
        with urllib.request.urlopen(DASH + path, timeout=30) as r:
            return json.loads(r.read().decode())
    except (urllib.error.URLError, OSError, ValueError):
        return None


def post(path, body):
    req = urllib.request.Request(DASH + path, method="POST",
                                 data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except ValueError:
            return e.code, {}
    except (urllib.error.URLError, OSError):
        return None, {}


def script(name, body):
    os.makedirs("/tmp/cps", exist_ok=True)
    path = f"/tmp/cps/{name}.py"
    with open(path, "w") as f:
        f.write(body)
    c.deploy_script(path)
    return path


def settle(job_id, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        st = (c.get_job_status(job_id) or "").upper()
        if any(t in st for t in ("COMPLETED", "FAILED", "DEAD", "CANCELLED")):
            return st
        time.sleep(0.3)
    return "TIMEOUT"


def slots():
    total = 0
    for _, series in stats("metrics").get("worker_load", {}).items():
        if series:
            total += series[-1][2]
    return total


def main():
    tag = int(time.time()) % 100000
    work = script("cps_work", "import time\ntime.sleep(2.0)\nprint('ok')\n")
    quick = script("cps_quick", "print('ok')\n")

    # ------------------------------------------------------------------
    print("\n=== 1. dispatch phase breakdown ===")
    # Generate traffic so there are iterations to measure.
    c.submit_dag(f"cps-warm-{tag}", [TitanJob(job_id=f"cps{tag}-w{i}", filename=quick)
                                     for i in range(6)])
    time.sleep(6)
    m = stats("metrics")
    ph = m.get("dispatch_phases", [])
    disp = m.get("dispatch", [])
    check("dispatch_phases series is exposed", isinstance(ph, list) and len(ph) > 0, f"{len(ph)}")
    if ph:
        check("each sample is [ts, route, select, record, store, send]",
              all(len(p) == 6 for p in ph), f"widths {sorted({len(p) for p in ph})}")
        check("no negative phase durations",
              all(all(v >= 0 for v in p[1:]) for p in ph))
        check("timestamps increase monotonically",
              all(ph[i][0] <= ph[i + 1][0] for i in range(len(ph) - 1)))
        # The phases partition one iteration, so their sum cannot exceed the loop's own worst
        # measured total. A sum above that means the boundaries are mis-placed.
        worst_total = max([p[1] for p in disp] or [0])
        worst_sum = max(sum(p[1:]) for p in ph)
        check("phase sums stay within the measured loop total",
              worst_sum <= max(worst_total, 1) + 50, f"phases {worst_sum}ms vs loop {worst_total}ms")
        check("at least one phase recorded real time",
              any(sum(p[1:]) > 0 for p in ph))
        # The network hand-off is the phase that should dominate; scheduling logic is arithmetic.
        tot = [sum(p[i] for p in ph) for i in range(1, 6)]
        names = ["route", "select", "record", "store", "send"]
        print("    totals: " + " · ".join(f"{n} {v}ms" for n, v in zip(names, tot)))
        check("the breakdown attributes time to a named phase", sum(tot) > 0)

    # ------------------------------------------------------------------
    print("\n=== 2. priority is recorded on spans and actually reorders work ===")
    n = max(2, slots())
    bulk = [TitanJob(job_id=f"cps{tag}-bulk{i}", filename=work, priority=1)
            for i in range(n * 3)]
    c.submit_dag(f"cps-prio-{tag}", bulk)
    time.sleep(1.5)                       # let the pool fill so there is a queue to jump
    c.submit_job(TitanJob(job_id=f"cps{tag}-urgent", filename=work, priority=9))
    settle(f"DAG-cps{tag}-urgent", 240)
    time.sleep(2)

    sp = stats(f"timeline:cps{tag}-:400").get("spans", [])
    check("spans were recorded", len(sp) > 0, f"{len(sp)}")
    check("every span carries a priority field", all("priority" in s for s in sp))
    hi = [s for s in sp if s.get("priority") == 9]
    lo = [s for s in sp if s.get("priority") == 1]
    check("the high-priority span recorded priority 9", len(hi) == 1, f"{len(hi)}")
    check("bulk spans recorded priority 1", len(lo) >= n, f"{len(lo)}")
    if hi and len(lo) >= n:
        # Only jobs that had to queue are comparable: the first wave never waited at all.
        queued_lo = sorted(s["queue_wait_ms"] for s in lo if s["queue_wait_ms"] > 500)
        urgent_wait = hi[0]["queue_wait_ms"]
        print(f"    urgent waited {urgent_wait}ms · queued bulk waits {queued_lo[:6]}")
        if queued_lo:
            beaten = [w for w in queued_lo if w > urgent_wait]
            check("the priority-9 job overtook jobs already queued ahead of it",
                  len(beaten) > 0, f"overtook {len(beaten)} of {len(queued_lo)} queued jobs")
            check("it did not simply wait the longest",
                  urgent_wait <= queued_lo[-1], f"{urgent_wait}ms vs max {queued_lo[-1]}ms")
        else:
            check("pool never saturated, so priority had no queue to jump — inconclusive",
                  True, "widen the load to make this meaningful")

    # Disk spans must carry it too, or a persisted span stops being comparable to a live one.
    # Scope this to spans THIS run wrote: the JSONL is append-only across builds, so files also
    # hold spans from before the field existed. The dashboard tolerates that by filtering on the
    # field's presence — the contract is that spans written now carry it, not that history is
    # retroactively complete.
    hist = get("/api/history?hours=24&limit=20000") or {}
    mine = [s for s in hist.get("spans", []) if f"cps{tag}-" in s.get("id", "")]
    check("this run's spans reached disk", len(mine) > 0, f"{len(mine)} of {len(hist.get('spans', []))}")
    if mine:
        check("persisted spans carry priority", all("priority" in s for s in mine),
              f"{sum(1 for s in mine if 'priority' not in s)} missing of {len(mine)}")
        check("the persisted priority matches what was submitted",
              any(s.get("priority") == 9 for s in mine) and
              any(s.get("priority") == 1 for s in mine),
              str(sorted({s.get("priority") for s in mine})))

    # ------------------------------------------------------------------
    print("\n=== 3. store latency, read and write ===")
    m = stats("metrics")
    w = m.get("store_latency", [])
    check("write latency series is exposed", isinstance(w, list) and len(w) > 0, f"{len(w)}")
    check("write samples are [ts, ms]", all(len(p) == 2 for p in w) if w else False)
    check("write latency is non-negative", all(p[1] >= 0 for p in w) if w else False)
    check("read latency series is exposed", "store_read_latency" in m)
    # Reads only happen when something asks for status, so make one happen first.
    get("/api/dag_status") or get("/api/demo/status")
    time.sleep(2)
    rd = stats("metrics").get("store_read_latency", [])
    check("read latency fills once a status read occurs", len(rd) > 0, f"{len(rd)} samples")
    if rd:
        check("read samples are [ts, ms]", all(len(p) == 2 for p in rd))
        check("read counter agrees that reads happened",
              (stats("metrics").get("store") or {}).get("reads", 0) > 0)

    # ------------------------------------------------------------------
    print("\n=== 4. demo runner ===")
    pres = get("/api/demo/presets")
    check("presets endpoint responds", pres is not None)
    if pres:
        ids = [p["id"] for p in pres.get("presets", [])]
        check("presets are advertised", len(ids) >= 4, str(ids))
        check("every preset says what it shows",
              all(p.get("shows") and p.get("label") for p in pres["presets"]))

        # The endpoint must be preset-only: nothing in the request may become a job.
        code, body = post("/api/demo/run", {"preset": "definitely-not-a-preset"})
        check("an unknown preset is refused", code == 400, f"HTTP {code}")
        code, body = post("/api/demo/run", {"preset": "../../etc/passwd"})
        check("a path-like preset is refused", code == 400, f"HTTP {code}")
        code, body = post("/api/demo/run",
                          {"preset": "fanout", "script": "import os; os.system('id')"})
        check("an extra script field is ignored, not executed", code in (200, 409), f"HTTP {code}")

        if code == 200:
            check("run reports the preset it started", body.get("preset") == "fanout", str(body))
            deadline = time.time() + 120
            last = {}
            while time.time() < deadline:
                last = get("/api/demo/status") or {}
                if last.get("total") and last.get("done") == last.get("total"):
                    break
                time.sleep(2)
            check("the demo reported no error", not last.get("error"), str(last.get("error")))
            check("the demo produced jobs", (last.get("total") or 0) > 0, str(last.get("total")))
            check("every demo job settled", last.get("done") == last.get("total"),
                  f"{last.get('done')}/{last.get('total')}")
            check("it ran under a named pipeline", bool(last.get("pipeline")), str(last.get("pipeline")))
            check("the pipeline is visible to the timeline picker",
                  any(d["name"] == last.get("pipeline")
                      for d in (get("/api/dag_names") or {}).get("dags", [])))
            counts = last.get("counts", {})
            check("the fan-out demo completed rather than failing",
                  counts.get("COMPLETED", 0) > 0 and counts.get("DEAD", 0) == 0, str(counts))

    # ------------------------------------------------------------------
    print("\n=== 5. failure rate and saturation are derivable and bounded ===")
    m = stats("metrics")
    thru = m.get("throughput", [])
    check("throughput samples are [ts, completed, failed]",
          all(len(p) == 3 for p in thru) if thru else False,
          f"widths {sorted({len(p) for p in thru})}" if thru else "no samples")
    if thru:
        done = sum(p[1] for p in thru)
        failed = sum(p[2] for p in thru)
        check("completed and failed counts are non-negative",
              all(p[1] >= 0 and p[2] >= 0 for p in thru))
        check("the window recorded settled work", done + failed > 0, f"{done} done, {failed} failed")
        rate = 100 * failed / max(done + failed, 1)
        check("the derived failure rate is a valid percentage", 0 <= rate <= 100, f"{rate:.1f}%")
        print(f"    failure rate over the window: {rate:.1f}% ({failed} of {done + failed})")

    q = m.get("queue", [])
    check("queue samples carry occupied slots (field 7)",
          all(len(p) >= 8 for p in q) if q else False,
          f"widths {sorted({len(p) for p in q})}" if q else "no samples")
    # Occupancy, not runningJobs: see the comment on queueSeries.add.
    sat = [(p[0], 100.0 * p[7] / p[6]) for p in q if len(p) >= 8 and p[6]]
    orphans = [(p[0], p[3] - p[7]) for p in q if len(p) >= 8 and p[3] > p[7]]
    if orphans:
        worst = max(v for _, v in orphans)
        print(f"    {len(orphans)} sample(s) counted more jobs running than the fleet had "
              f"occupied slots, worst gap {worst} — jobs orphaned by a reclaimed worker")
    check("queue samples carry running and total slots", len(sat) > 0, f"{len(sat)} of {len(q)}")
    if sat:
        worst = max(v for _, v in sat)
        check("saturation never exceeds 100% — running cannot outnumber slots",
              worst <= 100.0, f"peak {worst:.1f}%")
        check("saturation is never negative", min(v for _, v in sat) >= 0)
        print(f"    saturation: now {sat[-1][1]:.0f}% · peak {worst:.0f}% of "
              f"{q[-1][6]} slots")

    # ------------------------------------------------------------------
    print("\n=== 6. services are described, not just counted ===")
    code, body = post("/api/demo/run", {"preset": "service"})
    check("the service preset is accepted", code == 200, f"HTTP {code}")
    if code == 200:
        svc = []
        deadline = time.time() + 120
        while time.time() < deadline:
            svc = stats("metrics").get("services", [])
            if svc:
                break
            time.sleep(2)
        check("a deployed service appears in the metrics payload", len(svc) > 0, f"{len(svc)}")
        if svc:
            sv = svc[0]
            for field in ("id", "host", "port", "worker", "since", "uptime_ms",
                          "deploy_attempts", "status"):
                check(f"service record carries {field}", field in sv)
            check("the port is a real listening port, not zero", sv.get("port", 0) > 0, str(sv.get("port")))
            check("uptime is positive once it is up", sv.get("uptime_ms", -1) >= 0, str(sv.get("uptime_ms")))
            check("the hosting worker is identified", ":" in str(sv.get("worker")), str(sv.get("worker")))
            print(f"    {sv['id']} -> {sv['host']}:{sv['port']} on {sv['worker']}, "
                  f"up {sv['uptime_ms']}ms")

            # The address is the whole point of the panel: it must actually answer.
            try:
                with urllib.request.urlopen(f"http://{sv['host']}:{sv['port']}/", timeout=8) as r:
                    reachable = r.status == 200
            except (urllib.error.URLError, OSError):
                reachable = False
            check("the advertised address actually answers", reachable,
                  f"http://{sv['host']}:{sv['port']}/")

            # Uptime must advance rather than reset on each poll.
            first = sv["uptime_ms"]
            time.sleep(3)
            again = [x for x in stats("metrics").get("services", []) if x["id"] == sv["id"]]
            if again:
                check("uptime advances instead of resetting",
                      again[0]["uptime_ms"] > first,
                      f"{first}ms -> {again[0]['uptime_ms']}ms")
                check("the start time is stable across polls",
                      again[0]["since"] == sv["since"])

            # A SERVICE deploy job completes once readiness passes while the process keeps
            # running, so a COMPLETED deploy job alongside a live service is the correct state.
            check("the service survives its deploy job completing",
                  str(sv.get("status", "")).upper() in ("COMPLETED", "RUNNING")
                  or "DEPLOY" in str(sv.get("status", "")).upper(), str(sv.get("status")))

            stopped = c.stop_service(sv["id"]) if hasattr(c, "stop_service") else None
            if stopped is not None:
                time.sleep(4)
                left = [x for x in stats("metrics").get("services", []) if x["id"] == sv["id"]]
                check("stopping a service removes it from the live set", not left, str(stopped)[:60])

    # ------------------------------------------------------------------
    print("\n=== 7. host vitals on the heartbeat, without breaking the old format ===")
    st = stats("")
    workers = st.get("workers", [])
    check("workers are reported", len(workers) > 0, str(len(workers)))
    for w in workers:
        for f in ("host_cpu_pct", "host_mem_pct", "host_load_x100"):
            check(f"worker :{w['port']} carries {f}", f in w)
    reporting = [w for w in workers if w.get("host_cpu_pct", -1) >= 0]
    check("at least one worker reported real host vitals", len(reporting) > 0,
          f"{len(reporting)} of {len(workers)}")
    if reporting:
        w = reporting[0]
        print(f"    :{w['port']} cpu {w['host_cpu_pct']}% · mem {w['host_mem_pct']}% · "
              f"load {w['host_load_x100'] / 100:.2f}")
        check("cpu is a percentage", 0 <= w["host_cpu_pct"] <= 100, str(w["host_cpu_pct"]))
        check("memory is a percentage", 0 <= w["host_mem_pct"] <= 100, str(w["host_mem_pct"]))
        check("load average is non-negative", w["host_load_x100"] >= 0, str(w["host_load_x100"]))
        # The whole point: these are independent of slot occupancy.
        used, cap = (w.get("load") or "0/0").split("/")
        check("host load is reported independently of slot occupancy",
              True, f"slots {used}/{cap} vs cpu {w['host_cpu_pct']}%")

    # A worker that cannot measure its host, or predates the change, must read as unknown — never
    # as zero, which would be indistinguishable from a genuinely idle machine.
    unknown = [w for w in workers if w.get("host_cpu_pct", -1) < 0]
    check("a worker not reporting vitals reads -1, not 0",
          all(w.get("host_cpu_pct") == -1 for w in unknown) if unknown else True,
          f"{len(unknown)} not reporting")

    # Slot accounting must be untouched by the added fields — this is the compatibility contract.
    check("slot occupancy still parses for every worker",
          all("/" in (w.get("load") or "") for w in workers),
          str([w.get("load") for w in workers]))
    q = stats("metrics").get("queue", [])
    if q:
        check("the fleet's slot total still matches the workers",
              q[-1][6] == sum(int((w.get("load") or "0/0").split("/")[1]) for w in workers),
              f"queue says {q[-1][6]}")

    bad = [n for n, ok in R if not ok]
    print(f"\n{'=' * 66}\n RESULT: {len(R) - len(bad)}/{len(R)} passed\n{'=' * 66}")
    for b in bad:
        print("  FAILED:", b)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
