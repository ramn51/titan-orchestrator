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
Run published WfCommons workflow instances on Titan.

Why this exists: benchmarks we invent ourselves prove nothing to anyone else. WfCommons publishes
DAG instances derived from real production scientific workflows (Montage, Epigenomics, CyberShake,
BLAST, Cycles, SoyKB) in a documented JSON schema called WfFormat, and ships translators for
Airflow, Nextflow, Pegasus, Dask, Parsl and others. Running the same instance here produces a
number that is directly comparable to systems that have published against it.

This is the missing translator: WfFormat in, Titan DAG out.

    https://wfcommons.org/            instances and the WfBench generator
    https://github.com/wfcommons/WfFormat    the schema and its validator

What it does NOT do: reproduce the original computation. A published instance records a task graph
and how long each task took on the machine that ran it, not the science. Each task becomes a
generated Python script that occupies a slot for the recorded duration, either sleeping or burning
CPU. That is the right model for measuring a scheduler, because what is under test is placement,
dependency release and recovery, not arithmetic.

Usage
-----
    # inspect an instance without touching the cluster
    python titan_test_suite/wfformat_to_titan.py montage.json

    # compress a multi-hour workflow into something runnable, then submit
    python titan_test_suite/wfformat_to_titan.py montage.json --scale 0.01 --submit

    # burn real CPU instead of sleeping, so worker saturation is exercised
    python titan_test_suite/wfformat_to_titan.py montage.json --scale 0.01 --mode cpu --submit

    # cap total tasks while keeping the DAG connected, for a smoke run
    python titan_test_suite/wfformat_to_titan.py montage.json --limit 200 --submit

Options
-------
    --scale F     multiply every recorded runtime by F (default 1.0)
    --min-s S     floor on generated task duration, seconds (default 0.05)
    --max-s S     ceiling on generated task duration, seconds (default 0)  0 = no ceiling
    --mode M      sleep | cpu   how a task occupies its slot (default sleep)
    --limit N     keep at most N tasks, breadth-first from the roots, edges preserved
    --name NAME   pipeline name to submit under (default: the instance name)
    --out DIR     write the generated scripts here instead of a temp dir
    --submit      actually submit to the Master; without it, this only reports
    --json        machine-readable summary
"""

import argparse
import json
import os
import sys
import tempfile
import time
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Characters that would corrupt Titan's pipe-delimited wire payload. Job IDs are embedded as
# `id|file|[p1,p2]|prio|delay|req` and DAGs are joined with " ; ", so none of these can survive
# in an identifier. WfFormat task IDs routinely contain '.' and '-' and sometimes worse.
_UNSAFE = str.maketrans({c: "_" for c in "|;,[]() \t\n\r"})


# ── parsing ───────────────────────────────────────────────────────────────────

def _norm_task(raw, runtimes):
    """One WfFormat task in the shape this tool uses, regardless of schema version."""
    tid = raw.get("id") or raw.get("name")
    name = raw.get("name") or tid
    parents = list(raw.get("parents") or [])
    # runtimeInSeconds is 1.6; `runtime` is the pre-1.5 spelling and is also seconds.
    rt = raw.get("runtimeInSeconds", raw.get("runtime"))
    if rt is None:
        rt = runtimes.get(tid)
    return {
        "id": tid,
        "name": name,
        "parents": parents,
        "runtime_s": float(rt) if rt is not None else None,
        "cores": raw.get("coreCount"),
        "memory_bytes": raw.get("memoryInBytes"),
    }


def load_wfformat(path):
    """Read a WfFormat instance into a normalized task list.

    Handles the layouts that appear across the published corpus:

      1.6+   workflow.specification.tasks   (graph)
             workflow.execution.tasks       (runtimes, keyed by the same id)
      1.3-1.5  workflow.tasks               (graph and runtime together)
      <=1.2    workflow.jobs                (same, older noun)

    Raises ValueError with a specific reason rather than a KeyError, because a silently
    mis-parsed instance would produce a benchmark that measures nothing.
    """
    with open(path) as f:
        doc = json.load(f)

    wf = doc.get("workflow")
    if not isinstance(wf, dict):
        raise ValueError("no top-level 'workflow' object: this is not a WfFormat instance")

    spec = wf.get("specification") if isinstance(wf.get("specification"), dict) else None

    # execution tasks carry the runtimes in 1.6; index them by id
    runtimes = {}
    execution = wf.get("execution")
    if isinstance(execution, dict):
        for t in execution.get("tasks") or []:
            if t.get("id") is not None and t.get("runtimeInSeconds") is not None:
                runtimes[t["id"]] = t["runtimeInSeconds"]

    if spec and spec.get("tasks"):
        raw_tasks, layout = spec["tasks"], "1.6 specification/execution"
    elif wf.get("tasks"):
        raw_tasks, layout = wf["tasks"], "flat workflow.tasks"
    elif wf.get("jobs"):
        raw_tasks, layout = wf["jobs"], "legacy workflow.jobs"
    else:
        raise ValueError("no tasks found under workflow.specification.tasks, "
                         "workflow.tasks or workflow.jobs")

    tasks = [_norm_task(t, runtimes) for t in raw_tasks]
    if not tasks:
        raise ValueError("instance parsed but contains zero tasks")

    known = {t["id"] for t in tasks}
    # Some instances list parents by name rather than id; accept either.
    by_name = {t["name"]: t["id"] for t in tasks}
    for t in tasks:
        fixed = []
        for p in t["parents"]:
            if p in known:
                fixed.append(p)
            elif p in by_name:
                fixed.append(by_name[p])
            # a parent naming nothing in this instance is dropped, and counted below
        t["parents"] = [p for p in fixed if p != t["id"]]   # never depend on yourself

    return {
        "name": doc.get("name") or os.path.splitext(os.path.basename(path))[0],
        "schema": doc.get("schemaVersion", "unknown"),
        "layout": layout,
        "tasks": tasks,
        "dropped_parent_refs": sum(
            len([p for p in (t.get("parents") or [])]) for t in raw_tasks if isinstance(t, dict)
        ) - sum(len(t["parents"]) for t in tasks),
    }


# ── graph shaping ─────────────────────────────────────────────────────────────

def check_acyclic(tasks):
    """Kahn's algorithm. Returns the ids that could not be ordered, which is the cycle."""
    incoming = {t["id"]: set(t["parents"]) for t in tasks}
    ready = deque(i for i, ps in incoming.items() if not ps)
    children = {}
    for t in tasks:
        for p in t["parents"]:
            children.setdefault(p, []).append(t["id"])
    ordered = 0
    while ready:
        n = ready.popleft()
        ordered += 1
        for ch in children.get(n, []):
            incoming[ch].discard(n)
            if not incoming[ch]:
                ready.append(ch)
    return [i for i, ps in incoming.items() if ps] if ordered != len(tasks) else []


def limit_tasks(tasks, limit):
    """Keep at most `limit` tasks, breadth-first from the roots, and re-point severed edges.

    Truncating a DAG by slicing the list would leave tasks whose parents are gone, which Titan
    would park forever. Walking from the roots keeps the prefix connected, and any parent that
    did not survive is dropped from its child rather than left dangling.
    """
    if not limit or limit >= len(tasks):
        return tasks
    by_id = {t["id"]: t for t in tasks}
    children = {}
    for t in tasks:
        for p in t["parents"]:
            children.setdefault(p, []).append(t["id"])
    keep, q = [], deque(t["id"] for t in tasks if not t["parents"])
    seen = set(q)
    while q and len(keep) < limit:
        n = q.popleft()
        keep.append(n)
        for ch in children.get(n, []):
            if ch not in seen:
                seen.add(ch)
                q.append(ch)
    kept = set(keep)
    out = []
    for i in keep:
        t = dict(by_id[i])
        t["parents"] = [p for p in t["parents"] if p in kept]
        out.append(t)
    return out


# ── script generation ─────────────────────────────────────────────────────────

_SLEEP_TPL = '''"""Generated from a WfCommons instance. Occupies a slot for the recorded duration."""
import time
time.sleep({secs!r})
print("{tid} ok {secs}s")
'''

_CPU_TPL = '''"""Generated from a WfCommons instance. Burns CPU for the recorded duration."""
import time
end = time.time() + {secs!r}
x = 0
while time.time() < end:
    for _ in range(20000):
        x = (x * 1103515245 + 12345) & 0x7FFFFFFF
print("{tid} ok {secs}s", x & 1)
'''


def generate(inst, outdir, scale=1.0, min_s=0.05, max_s=0.0, mode="sleep"):
    """Write one script per task. Returns (jobs, stats) where jobs is ready for TitanJob."""
    os.makedirs(outdir, exist_ok=True)
    tpl = _CPU_TPL if mode == "cpu" else _SLEEP_TPL

    # Titan job IDs must be wire-safe and unique. Sanitizing can collide, so a suffix is added
    # on collision and the mapping is kept, since parents are expressed in original ids.
    idmap, used = {}, set()
    for t in inst["tasks"]:
        safe = t["id"].translate(_UNSAFE)[:80] or "task"
        cand, k = safe, 1
        while cand in used:
            cand = f"{safe}_{k}"
            k += 1
        used.add(cand)
        idmap[t["id"]] = cand

    jobs, missing_rt, total_s = [], 0, 0.0
    for t in inst["tasks"]:
        rt = t["runtime_s"]
        if rt is None:
            rt, missing_rt = min_s, missing_rt + 1
        secs = rt * scale
        if max_s:
            secs = min(secs, max_s)
        secs = round(max(secs, min_s), 3)
        total_s += secs
        jid = idmap[t["id"]]
        path = os.path.join(outdir, f"{jid}.py")
        with open(path, "w") as f:
            f.write(tpl.format(secs=secs, tid=jid))
        jobs.append({"job_id": jid, "filename": path,
                     "parents": [idmap[p] for p in t["parents"]],
                     "orig_id": t["id"], "secs": secs})

    depths = _depths(jobs)
    widths = {}
    for j in jobs:
        widths[depths[j["job_id"]]] = widths.get(depths[j["job_id"]], 0) + 1
    crit = _critical_path(jobs, depths)
    return jobs, {
        "tasks": len(jobs),
        "edges": sum(len(j["parents"]) for j in jobs),
        "roots": sum(1 for j in jobs if not j["parents"]),
        "depth": (max(depths.values()) + 1) if depths else 0,
        "max_width": max(widths.values()) if widths else 0,
        "total_task_seconds": round(total_s, 2),
        "critical_path_seconds": round(crit, 2),
        "max_speedup": round(total_s / crit, 1) if crit else 0,
        "tasks_missing_runtime": missing_rt,
    }


def _depths(jobs):
    by = {j["job_id"]: j for j in jobs}
    memo = {}

    def d(i, stack=()):
        if i in memo:
            return memo[i]
        if i in stack:                       # defensive: cycles are rejected earlier
            return 0
        ps = by[i]["parents"]
        v = 0 if not ps else 1 + max(d(p, stack + (i,)) for p in ps if p in by)
        memo[i] = v
        return v

    return {j["job_id"]: d(j["job_id"]) for j in jobs}


def _critical_path(jobs, depths):
    """Longest chain by summed duration: the floor on wall time with unlimited slots."""
    by = {j["job_id"]: j for j in jobs}
    memo = {}

    def f(i):
        if i in memo:
            return memo[i]
        ps = [p for p in by[i]["parents"] if p in by]
        v = by[i]["secs"] + (max(f(p) for p in ps) if ps else 0)
        memo[i] = v
        return v

    return max((f(j["job_id"]) for j in jobs), default=0)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Convert a WfCommons WfFormat instance into a Titan DAG")
    ap.add_argument("instance")
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--min-s", type=float, default=0.05)
    ap.add_argument("--max-s", type=float, default=0.0)
    ap.add_argument("--mode", choices=("sleep", "cpu"), default="sleep")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--name", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--submit", action="store_true")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    inst = load_wfformat(a.instance)
    cyc = check_acyclic(inst["tasks"])
    if cyc:
        print(f"ERROR: instance contains a cycle involving {len(cyc)} tasks, e.g. {cyc[:5]}",
              file=sys.stderr)
        sys.exit(2)
    if a.limit:
        inst["tasks"] = limit_tasks(inst["tasks"], a.limit)

    outdir = a.out or tempfile.mkdtemp(prefix="wfbench_")
    jobs, stats = generate(inst, outdir, a.scale, a.min_s, a.max_s, a.mode)
    pipeline = a.name or f"wf-{inst['name']}"

    summary = {"instance": inst["name"], "schema": inst["schema"], "layout": inst["layout"],
               "pipeline": pipeline, "scripts": outdir, "mode": a.mode, "scale": a.scale, **stats}

    if a.json:
        print(json.dumps(summary, indent=2))
    else:
        print(f"\n  instance            {inst['name']}")
        print(f"  schema / layout     {inst['schema']}  ({inst['layout']})")
        print(f"  tasks / edges       {stats['tasks']} / {stats['edges']}")
        print(f"  roots / depth       {stats['roots']} / {stats['depth']}")
        print(f"  widest level        {stats['max_width']} tasks")
        print(f"  total task time     {stats['total_task_seconds']}s "
              f"(after scale={a.scale}, mode={a.mode})")
        print(f"  critical path       {stats['critical_path_seconds']}s")
        print(f"  max speed-up        {stats['max_speedup']}x  "
              f"(total / critical path: the most parallelism can buy)")
        if stats["tasks_missing_runtime"]:
            print(f"  NOTE                {stats['tasks_missing_runtime']} tasks had no recorded "
                  f"runtime; floored to {a.min_s}s")
        if inst["dropped_parent_refs"] > 0:
            print(f"  NOTE                {inst['dropped_parent_refs']} parent refs pointed "
                  f"outside the instance and were dropped")
        print(f"  scripts             {outdir}")

    if not a.submit:
        if not a.json:
            print("\n  (dry run: pass --submit to send this to the Master)")
        return

    from titan_sdk.titan_sdk import TitanClient, TitanJob
    c = TitanClient()
    stamp = int(time.time()) % 100000
    tjobs = [TitanJob(job_id=f"{j['job_id']}-{stamp}", filename=j["filename"],
                      parents=[f"{p}-{stamp}" for p in j["parents"]])
             for j in jobs]
    t0 = time.time()
    c.submit_dag(f"{pipeline}-{stamp}", tjobs)
    if not a.json:
        print(f"\n  submitted {len(tjobs)} tasks as '{pipeline}-{stamp}' in {time.time()-t0:.1f}s")
        print(f"  critical path is {stats['critical_path_seconds']}s, so anything close to that "
              f"means the scheduler is not the limit")


if __name__ == "__main__":
    main()
