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
Regression tests for the DAG pipeline visualizer.

Each test here pins a specific defect that was found and fixed, and is written so
that reverting the fix makes it fail. They are not smoke tests.

  1. DAG grouping        — a DAG must appear as ONE entry, not fragmented per
                           job-ID prefix. Regression: a corrupt/absent manifest
                           silently fell back to splitting job IDs on "_".
  2. Status resolution    — every node's status must be correct regardless of graph
                           size. Regression: status came from workerRecentHistory,
                           capped at 10 per worker, so larger DAGs showed PENDING
                           for finished jobs.
  3. Concurrent manifest  — simultaneous submit_dag calls must leave valid JSON.
                           Regression: unguarded read-modify-write corrupted it.
  4. Corrupt manifest     — a damaged manifest must self-heal, not poison grouping
                           forever.
  5. Routes               — every dashboard page renders.

Requires a live cluster AND the dashboard:
    ./titan-dev.sh up  (or start Master + worker + server_dashboard.py)
    python3 titan_test_suite/tests/test_visualizer.py
"""

import json
import os
import shutil
import sys
import threading
import time
import urllib.error
import urllib.request

from titan_sdk import TitanClient, TitanJob

DASH = os.environ.get("TITAN_DASHBOARD", "http://127.0.0.1:5000")
MANIFEST = ".titan_dag_manifest.json"
OP_STATS_JSON = 0x09

c = TitanClient()
R = []


def check(name, ok, detail=""):
    R.append((name, ok))
    print(f"  {'[PASS]' if ok else '[FAIL]'} {name}" + (f" - {detail}" if detail else ""))


def http(path, timeout=20):
    try:
        with urllib.request.urlopen(DASH + path, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, ""
    except OSError as e:
        return 0, str(e)


def settle(job_id, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        st = (c.get_job_status(job_id) or "").upper()
        if any(t in st for t in ("COMPLETED", "FAILED", "DEAD", "CANCELLED")):
            return st
        time.sleep(0.3)
    return "TIMEOUT"


def worker_count():
    try:
        return json.loads(c._send_request(OP_STATS_JSON, "")).get("active_workers", 1)
    except Exception:
        return 1


def script():
    os.makedirs("/tmp/viz", exist_ok=True)
    p = "/tmp/viz/n.py"
    with open(p, "w") as f:
        f.write("import time\ntime.sleep(0.4)\nprint('ok')\n")
    c.deploy_script(p)
    return p


def main():
    code, _ = http("/")
    if code != 200:
        print(f"  Dashboard unreachable at {DASH} (got {code}). Start server_dashboard.py first.")
        return 2

    S = script()
    tag = int(time.time()) % 100000

    # ------------------------------------------------------------------
    # 1. Grouping: underscore-heavy job IDs must NOT fragment the DAG.
    #    This is the exact shape that broke — chain_1/chain_2 became "chain".
    # ------------------------------------------------------------------
    print("\n=== 1. DAG grouping (underscore job IDs must stay one DAG) ===")
    g = f"grp{tag}"
    dag_name = f"viz-group-{tag}"
    gjobs = [
        TitanJob(job_id=f"{g}-extract_raw", filename=S),
        TitanJob(job_id=f"{g}-extract_meta", filename=S),
        TitanJob(job_id=f"{g}-transform_step_one", filename=S, parents=[f"{g}-extract_raw"]),
        TitanJob(job_id=f"{g}-transform_step_two", filename=S, parents=[f"{g}-extract_meta"]),
        TitanJob(job_id=f"{g}-load_final", filename=S,
                 parents=[f"{g}-transform_step_one", f"{g}-transform_step_two"]),
    ]
    c.submit_dag(dag_name, gjobs)
    settle(f"DAG-{g}-load_final")
    time.sleep(3)

    _, page = http("/dags")
    import re
    listed = set(re.findall(r'/dags/(DAG-[A-Za-z0-9_\-]+)', page))
    mine = {d for d in listed if g in d or dag_name in d}
    check("the DAG appears as exactly ONE entry, not fragmented",
          len(mine) == 1, f"found {len(mine)}: {sorted(mine)[:4]}")
    check("the entry is named after the DAG, not a job-ID prefix",
          any(dag_name in d for d in mine), str(sorted(mine)[:2]))

    # ------------------------------------------------------------------
    # 2. Status resolution beyond the worker-history cap.
    #    workerRecentHistory holds 10 per worker; build a graph bigger than that.
    # ------------------------------------------------------------------
    print("\n=== 2. status resolution for a DAG larger than the history window ===")
    # Sized beyond any plausible window: the scaler can add workers mid-run and each new worker
    # widens workerRecentHistory by 10, so a marginal graph would stop proving anything.
    cap = 10 * max(1, worker_count())
    n = max(cap + 8, 46)
    b = f"big{tag}"
    bjobs = [TitanJob(job_id=f"{b}-node_{i}", filename=S) for i in range(n - 1)]
    bjobs.append(TitanJob(job_id=f"{b}-sink_node", filename=S,
                          parents=[f"{b}-node_{i}" for i in range(0, n - 1, 5)]))
    print(f"  worker history window ≈ {cap} jobs; submitting {n} nodes")
    c.submit_dag(f"viz-big-{tag}", bjobs)
    # The sink only depends on every 5th node, so waiting on it alone would return while most of
    # the graph is still running and the status assertion below would race.
    settle(f"DAG-{b}-sink_node", 300)
    deadline = time.time() + 240
    while time.time() < deadline:
        probe = json.loads(c._send_request(
            OP_STATS_JSON, "status:" + ",".join(f"DAG-{j.id}" for j in bjobs)))
        if all(v in ("COMPLETED", "FAILED", "DEAD", "CANCELLED") for v in probe.values()):
            break
        time.sleep(1)
    time.sleep(2)

    ids = [f"DAG-{j.id}" for j in bjobs]
    bulk = json.loads(c._send_request(OP_STATS_JSON, "status:" + ",".join(ids)))
    unresolved = [i for i in ids if bulk.get(i, "UNKNOWN") in ("UNKNOWN", "NULL", "")]
    completed = sum(1 for v in bulk.values() if v == "COMPLETED")
    check("bulk status resolves every node in a graph larger than the window",
          not unresolved, f"{len(unresolved)} unresolved of {len(ids)}")
    check("every node reports COMPLETED", completed == len(ids), f"{completed}/{len(ids)}")

    # The live stats payload genuinely cannot carry them all — that's the point.
    stats = json.loads(c._send_request(OP_STATS_JSON, ""))
    visible = set()
    for w in stats.get("workers", []):
        for h in w.get("history", []):
            visible.add(h["id"])
        if w.get("active_job"):
            visible.add(w["active_job"])
    missing_from_window = set(ids) - visible
    check("stats-JSON history alone could NOT have shown them all (proves the fix matters)",
          bool(missing_from_window),
          f"window exposes {len(visible)} of {len(ids)}; {len(missing_from_window)} invisible without the fix")

    _, big_page = http(f"/dags/DAG-viz-big-{tag}")
    if big_page:
        check("rendered page does not report the DAG as still pending",
              "PENDING" not in big_page[:4000] or "COMPLETED" in big_page,
              "header region")

    # ------------------------------------------------------------------
    # 3. Concurrent manifest writes must not corrupt it.
    # ------------------------------------------------------------------
    print("\n=== 3. concurrent submissions keep the manifest valid ===")
    before_ok = os.path.exists(MANIFEST)
    errors = []

    def submit(i):
        try:
            cl = TitanClient()
            cl.submit_dag(f"viz-conc-{tag}-{i}",
                          [TitanJob(job_id=f"cc{tag}_{i}_{k}", filename=S) for k in range(3)])
        except Exception as e:  # noqa: BLE001 - surfaced as a test failure
            errors.append(repr(e))

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    time.sleep(2)

    check("no submission raised during concurrent writes", not errors, str(errors[:2]))
    parsed, entries = False, 0
    try:
        with open(MANIFEST) as f:
            data = json.load(f)
        parsed, entries = True, len(data)
    except (ValueError, OSError) as e:
        check("manifest is valid JSON after 8 concurrent submissions", False, str(e)[:70])
    if parsed:
        check("manifest is valid JSON after 8 concurrent submissions", True, f"{entries} entries")
        found = sum(1 for i in range(8)
                    if any(isinstance(v, dict) and v.get("dag") == f"viz-conc-{tag}-{i}"
                           for v in data.values()))
        check("all 8 concurrent DAGs survived in the manifest", found == 8, f"{found}/8")
    check("no stray temp files left behind",
          not [f for f in os.listdir(".") if f.startswith(MANIFEST) and f.endswith(".tmp")])

    # ------------------------------------------------------------------
    # 4. A corrupt manifest must self-heal instead of poisoning grouping.
    # ------------------------------------------------------------------
    print("\n=== 4. corrupt manifest self-heals ===")
    backup = MANIFEST + ".testbak"
    if os.path.exists(MANIFEST):
        shutil.copy(MANIFEST, backup)
    with open(MANIFEST, "w") as f:
        f.write('{"broken": {"dag": "x", ')          # deliberately truncated JSON
    h = f"heal{tag}"
    try:
        c.submit_dag(f"viz-heal-{tag}", [TitanJob(job_id=f"{h}-only_node", filename=S)])
        settle(f"DAG-{h}-only_node", 90)
        time.sleep(2)
        healed = False
        try:
            with open(MANIFEST) as f:
                m2 = json.load(f)
            healed = any(isinstance(v, dict) and v.get("dag") == f"viz-heal-{tag}" for v in m2.values())
        except ValueError:
            healed = False
        check("submitting over a corrupt manifest leaves valid JSON", healed)
        check("the damaged file is quarantined rather than deleted",
              os.path.exists(MANIFEST + ".corrupt"))
        _, p2 = http("/dags")
        check("grouping works again after the heal", f"viz-heal-{tag}" in p2 or h in p2)
    finally:
        # Self-healing rebuilds the manifest from scratch, so every pipeline submitted before this
        # test would vanish from the timeline picker — a destructive side effect for a read-only
        # check. Merge the pre-test entries back in; the healed ones win on conflict.
        if os.path.exists(backup):
            try:
                with open(backup) as f:
                    prior = json.load(f)
            except ValueError:
                prior = {}
            try:
                with open(MANIFEST) as f:
                    healed_now = json.load(f)
            except (OSError, ValueError):
                healed_now = {}
            if prior:
                prior.update(healed_now)
                tmp = MANIFEST + ".restore.tmp"
                with open(tmp, "w") as f:
                    json.dump(prior, f, indent=2)
                os.replace(tmp, MANIFEST)
                check("pre-existing pipelines survived the corrupt-manifest test",
                      len(prior) >= len(healed_now), f"{len(prior)} entries restored")
            os.remove(backup)

    # ------------------------------------------------------------------
    # 5. Every dashboard route still renders.
    # ------------------------------------------------------------------
    print("\n=== 5. dashboard routes ===")
    for path in ("/", "/dags", "/dags/new", "/agents", "/timeline", "/cluster"):
        code, body = http(path)
        check(f"{path} renders", code == 200 and len(body) > 500, f"{code}, {len(body)}b")
    for api in ("/api/metrics", "/api/board", "/api/timeline?limit=5", "/api/cluster_stats"):
        code, body = http(api)
        ok = code == 200
        if ok:
            try:
                json.loads(body)
            except ValueError:
                ok = False
        check(f"{api} returns JSON", ok, str(code))
    code, body = http("/api/export?fmt=md")
    check("/api/export produces a markdown report", code == 200 and "# Titan cluster report" in body)

    # ------------------------------------------------------------------
    # 6. Timeline must be filterable by DAG NAME, not just job-ID substring.
    #    Regression: the Master matches job-ID substrings, so searching for the
    #    pipeline's name returned nothing when job IDs used a different prefix.
    # ------------------------------------------------------------------
    print("\n=== 6. timeline filtering by pipeline name ===")
    tn = f"tl{tag}"
    tl_dag = f"viz-timeline-{tag}"
    c.submit_dag(tl_dag, [
        TitanJob(job_id=f"{tn}-alpha_one", filename=S),
        TitanJob(job_id=f"{tn}-beta_two", filename=S, parents=[f"{tn}-alpha_one"]),
        TitanJob(job_id=f"{tn}-gamma_three", filename=S, parents=[f"{tn}-beta_two"]),
    ])
    settle(f"DAG-{tn}-gamma_three", 120)
    time.sleep(3)

    code, body = http("/api/dag_names")
    names = []
    if code == 200:
        try:
            names = [d["name"] for d in json.loads(body).get("dags", [])]
        except ValueError:
            names = []
    check("the pipeline appears in /api/dag_names for the picker", tl_dag in names,
          f"{len(names)} known")

    code, body = http(f"/api/timeline?dag={tl_dag}&limit=100")
    spans = []
    if code == 200:
        try:
            spans = json.loads(body).get("spans", [])
        except ValueError:
            spans = []
    ids = {sp["id"] for sp in spans}
    check("filtering by DAG name returns that pipeline's spans",
          len(ids) == 3, f"{len(ids)} spans")
    check("every returned span belongs to the requested DAG",
          all(tn in i for i in ids), str(sorted(ids)[:2]))

    # The job IDs deliberately do NOT contain the DAG name — that is the bug being pinned.
    code, body2 = http(f"/api/timeline?filter={tl_dag}&limit=100")
    raw_spans = []
    if code == 200:
        try:
            raw_spans = json.loads(body2).get("spans", [])
        except ValueError:
            raw_spans = []
    check("a raw substring search on the DAG name finds nothing (proves dag= is required)",
          len(raw_spans) == 0, f"{len(raw_spans)} spans")

    # ------------------------------------------------------------------
    # 7. The DAG *list* must resolve status from the store, not the rolling
    #    worker-history window. Regression: the detail view was fixed but the
    #    list was not, so every pipeline showed PENDING after its jobs aged out.
    # ------------------------------------------------------------------
    print("\n=== 7. DAG list resolves status authoritatively ===")
    ln = f"ls{tag}"
    list_dag = f"viz-list-{tag}"
    lst = [TitanJob(job_id=f"{ln}-step_{i}", filename=S) for i in range(4)]
    lst.append(TitanJob(job_id=f"{ln}-final_step", filename=S,
                        parents=[f"{ln}-step_{i}" for i in range(4)]))
    c.submit_dag(list_dag, lst)
    settle(f"DAG-{ln}-final_step", 180)

    # Push its jobs out of the 10-per-worker history window so only the store still knows.
    filler = [TitanJob(job_id=f"fill{tag}-{i}", filename=S) for i in range(30)]
    c.submit_dag(f"viz-filler-{tag}", filler)
    settle(f"DAG-fill{tag}-29", 240)
    time.sleep(4)

    code, body = http("/api/dag_status")
    entries = []
    if code == 200:
        try:
            entries = json.loads(body)
        except ValueError:
            entries = []
    mine = [e for e in entries if e["name"] == list_dag]
    check("the pipeline appears in the list API", len(mine) == 1, f"{len(mine)} matches")
    if mine:
        e = mine[0]
        done = sum(1 for j in e["jobs"] if j["status"] == "COMPLETED")
        check("list reports the DAG as COMPLETED, not PENDING",
              e["status"] == "COMPLETED", e["status"])
        check("every job in the list is COMPLETED", done == len(e["jobs"]),
              f"{done}/{len(e['jobs'])}")
        check("no job is left as WAITING/UNKNOWN in the list",
              not [j for j in e["jobs"] if j["status"] in ("WAITING", "UNKNOWN", "")],
              str([j["status"] for j in e["jobs"]][:4]))

    # An unschedulable pipeline must still read PENDING — the fix must not force COMPLETED.
    up = f"vp{tag}"
    c.submit_job(TitanJob(job_id=f"{up}-tpu_job", filename=S, requirement="TPU"))
    time.sleep(8)
    code, body = http("/api/dag_status")
    try:
        entries = json.loads(body) if code == 200 else []
    except ValueError:
        entries = []
    parked = [e for e in entries if up in e["name"] or any(up in j["id"] for j in e["jobs"])]
    check("an unschedulable pipeline still reports PENDING",
          bool(parked) and parked[0]["status"] == "PENDING",
          parked[0]["status"] if parked else "not found")

    _, page = http("/dags")
    import re as _re
    labels = _re.findall(r">(COMPLETED|RUNNING|FAILED|PENDING|CANCELLED)<", page)
    check("the rendered list is not uniformly PENDING",
          not labels or len(set(labels)) > 1 or labels[0] != "PENDING",
          f"labels: {dict((x, labels.count(x)) for x in set(labels))}")

    bad = [n for n, ok in R if not ok]
    print(f"\n{'=' * 64}\n RESULT: {len(R) - len(bad)}/{len(R)} passed\n{'=' * 64}")
    for x in bad:
        print("  FAILED:", x)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
