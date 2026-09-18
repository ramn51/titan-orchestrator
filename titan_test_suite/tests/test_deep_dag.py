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
A deep, structurally varied DAG — the case wide parallel load never exercises.

24 nodes across 9 dependency levels, combining a long chain, fan-out, fan-in, a
diamond, a cross-branch join, a disconnected island, a delayed node and a
priority node in ONE submission.

Asserts execution correctness (every node runs exactly once, after its parents,
and the chain observes real depth) so it doubles as a regression check that the
scheduler still handles graphs and not just batches.

    python3 titan_test_suite/tests/test_deep_dag.py
"""

import json
import os
import sys
import time

from titan_sdk import TitanClient, TitanJob

c = TitanClient()
R = []


def check(name, ok, detail=""):
    R.append((name, ok))
    print(f"  {'[PASS]' if ok else '[FAIL]'} {name}" + (f" - {detail}" if detail else ""))


def settle(job_id, budget=300):
    t0 = time.time()
    while time.time() - t0 < budget:
        st = (c.get_job_status(job_id) or "").upper()
        if any(t in st for t in ("COMPLETED", "FAILED", "DEAD", "CANCELLED")):
            return st
        time.sleep(0.3)
    return "TIMEOUT"


def main():
    tag = int(time.time()) % 100000
    os.makedirs("/tmp/deep", exist_ok=True)
    body = "import time,sys\ntime.sleep(0.8)\nprint('node ok')\n"
    with open("/tmp/deep/n.py", "w") as f:
        f.write(body)
    c.deploy_script("/tmp/deep/n.py")
    S = "/tmp/deep/n.py"

    P = f"d{tag}"
    def J(name, parents=None, **kw):
        return TitanJob(job_id=f"{P}-{name}", filename=S,
                        parents=[f"{P}-{p}" for p in (parents or [])], **kw)

    # L0 roots ─ L1 fan-out ─ L2 fan-in ─ L3 diamond split ─ L4 diamond join
    # ─ L5 cross-branch ─ L6/L7/L8 deep chain ─ plus island, delayed, priority
    jobs = [
        J("ingest_a"), J("ingest_b"), J("ingest_c"),                          # L0
        J("split_1", ["ingest_a"]), J("split_2", ["ingest_a"]),               # L1 fan-out from one root
        J("split_3", ["ingest_b"]), J("split_4", ["ingest_b"]),
        J("merge_left", ["split_1", "split_2"]),                              # L2 fan-in
        J("merge_right", ["split_3", "split_4", "ingest_c"]),                 # L2 fan-in (3 parents)
        J("diamond_top", ["merge_left", "merge_right"]),                      # L3 reconvergence
        J("diamond_l", ["diamond_top"]), J("diamond_r", ["diamond_top"]),     # L4 split
        J("diamond_join", ["diamond_l", "diamond_r"]),                        # L5 join
        J("cross", ["diamond_l", "merge_right"]),                             # L5 cross-branch
        J("chain_1", ["diamond_join"]),                                       # L6
        J("chain_2", ["chain_1"]),                                            # L7
        J("chain_3", ["chain_2"]),                                            # L8
        J("chain_4", ["chain_3"]),                                            # L9
        J("final", ["chain_4", "cross"]),                                     # L10 terminal fan-in
        J("island_a"), J("island_b", ["island_a"]),                           # disconnected pair
        J("urgent", priority=9),                                              # priority, no deps
        J("later", delay=6000),                                               # delayed root
    ]

    expected_depth = {
        "ingest_a": 0, "ingest_b": 0, "ingest_c": 0, "island_a": 0, "urgent": 0, "later": 0,
        "split_1": 1, "split_2": 1, "split_3": 1, "split_4": 1, "island_b": 1,
        "merge_left": 2, "merge_right": 2,
        "diamond_top": 3, "diamond_l": 4, "diamond_r": 4,
        "diamond_join": 5, "cross": 5,
        "chain_1": 6, "chain_2": 7, "chain_3": 8, "chain_4": 9, "final": 10,
    }

    print(f"\n=== submitting {len(jobs)} nodes, max depth 10, in ONE submit_dag ===")
    t0 = time.time()
    c.submit_dag(f"deep-{tag}", jobs)
    st = settle(f"DAG-{P}-final", 300)
    wall = time.time() - t0
    print(f"  terminal node -> {st} after {wall:.1f}s")

    check("terminal node COMPLETED", "COMPLETED" in st, st)
    check("island tail COMPLETED", "COMPLETED" in settle(f"DAG-{P}-island_b", 60))
    check("delayed node COMPLETED", "COMPLETED" in settle(f"DAG-{P}-later", 90))

    spans = json.loads(c._send_request(0x09, f"timeline:{P}-:200"))["spans"]
    by = {}
    for s in spans:
        by.setdefault(s["id"], []).append(s)

    print(f"\n=== structure ({len(by)} distinct nodes recorded) ===")
    check("every declared node produced a span", len(by) == len(jobs), f"{len(by)} vs {len(jobs)}")
    dupes = [k for k, v in by.items() if len([x for x in v if x["status"] == "COMPLETED"]) > 1]
    check("no node executed twice (diamonds reconverge, not duplicate)", not dupes, str(dupes[:3]))

    # parent-before-child, the core invariant of a DAG scheduler
    starts = {k: min(x["started_at"] for x in v) for k, v in by.items()}
    ends = {k: max(x["ended_at"] for x in v if x["ended_at"]) for k, v in by.items() if any(x["ended_at"] for x in v)}
    violations = []
    for s in spans:
        for p in s["parents"]:
            if p in ends and s["started_at"] < ends[p] - 50:   # 50ms clock tolerance
                violations.append(f"{s['id']} started before parent {p} finished")
    check("no child started before a parent finished", not violations, str(violations[:2]))

    # observed depth must match declared depth
    origin = min(starts.values())
    ordered = sorted(((k, starts[k] - origin) for k in starts), key=lambda x: x[1])
    print("  execution order (ms from first start):")
    for k, off in ordered:
        short = k.replace(f"DAG-{P}-", "")
        print(f"    +{off:>6}ms  depth {expected_depth.get(short, '?'):>2}  {short}")

    deep_nodes = [k for k in starts if k.replace(f"DAG-{P}-", "") in
                  ("chain_4", "final")]
    root_end = max(ends[k] for k in ends if k.replace(f"DAG-{P}-", "") in ("ingest_a", "ingest_b", "ingest_c"))
    check("depth-10 terminal started well after the roots finished",
          all(starts[k] > root_end for k in deep_nodes),
          f"roots done at +{root_end - origin}ms")
    check("wall clock reflects real depth, not parallel breadth (>6s for 10 levels)",
          wall > 6, f"{wall:.1f}s")

    # the island must not be gated on the main graph
    isl = starts.get(f"DAG-{P}-island_a")
    check("disconnected island started immediately, not after the main graph",
          isl is not None and (isl - origin) < 3000, f"+{(isl - origin) if isl else '?'}ms")

    bad = [n for n, ok in R if not ok]
    print(f"\n{'=' * 62}\n RESULT: {len(R) - len(bad)}/{len(R)} passed · wall {wall:.1f}s\n{'=' * 62}")
    for b in bad:
        print("  FAILED:", b)
    print(f"\n  View it: http://localhost:5000/dags   (DAG deep-{tag})")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
