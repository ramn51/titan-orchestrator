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
Mid-DAG resume: re-run from the failure point, not from the beginning.

The gap this covers: today a failed pipeline can be REDEPLOYED (everything re-runs) or a single job
can be REPLAYED in isolation (its children do not follow). Neither lets you fix one job and
continue. For a graph whose completed prefix took hours, that difference is the whole feature.

The shape under test:

        A ──▶ B ──▶ C ──▶ D
              │
              └──▶ E ──▶ F

B fails on the first run, so C, D, E and F never run. B is then fixed and the DAG resumed.

What must be true afterwards, and all of it is checked against the persisted span record rather
than against a status field:

  1. A is NOT re-executed. It has exactly one span across both runs.
  2. B, C, D, E and F all complete.
  3. The jobs that ran on resume are exactly B and its descendants: no more, no fewer.
  4. Every job reaches COMPLETED, so the pipeline finishes.

Point 3 is the one that separates resume from redeploy, and point 1 from a full re-run. Point 2
exercises the case that caused a silent hang this morning: a SKIPPED parent has to unlock its
children exactly as a freshly completed one does.
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from titan_sdk.titan_sdk import TitanClient, TitanJob   # noqa: E402

OP_STATS = 0x09
TERMINAL = {"COMPLETED", "FAILED", "DEAD", "CANCELLED", "ERROR"}

PASS = FAIL = 0


def check(what, ok, detail=""):
    global PASS, FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {what}" + (f" - {detail}" if detail else ""))
    if ok:
        PASS += 1
    else:
        FAIL += 1


def statuses(c, ids):
    """Bulk status, so a wide graph does not cost one round trip per job."""
    out = {}
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        raw = c._send_request(OP_STATS, "status:" + ",".join(f"DAG-{j}" for j in chunk))
        if not raw or "{" not in raw:
            continue
        try:
            got = json.loads(raw[raw.find("{"):])
        except json.JSONDecodeError:
            continue
        for j in chunk:
            v = got.get(f"DAG-{j}") or got.get(j)
            if v:
                out[j] = str(v).upper()
    return out


def settle(c, ids, timeout=120):
    """Wait until every job is terminal, or the timeout expires."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = statuses(c, ids)
        if len(st) == len(ids) and all(v in TERMINAL for v in st.values()):
            return st
        time.sleep(0.5)
    return statuses(c, ids)


def spans_settled(c, ids, timeout=30):
    """Span counts, once the Master's background writer has caught up.

    Spans are persisted from a queue, so a job can report COMPLETED before its span is on disk.
    Reading immediately after a status settles undercounts, which looks exactly like "the job never
    ran". Poll until two consecutive reads agree and nothing is left queued.
    """
    t0 = time.time()
    prev = None
    while time.time() - t0 < timeout:
        cur = spans_for(c, ids)
        pending = 0
        raw = c._send_request(OP_STATS, "history_stats")
        if raw and "{" in raw:
            try:
                pending = json.loads(raw[raw.find("{"):]).get("queued", 0)
            except json.JSONDecodeError:
                pass
        if cur == prev and pending == 0:
            return cur
        prev = cur
        time.sleep(0.6)
    return spans_for(c, ids)


def spans_for(c, ids):
    """Span count per job id, read from the persisted log via the Master's history view.

    Counting spans is what makes 're-ran' observable: a status field only shows the latest
    outcome, while a second span is proof the job executed twice.
    """
    now = int(time.time() * 1000)
    raw = c._send_request(OP_STATS, f"history:{now - 3600 * 1000}:{now}::4000")
    counts = {j: 0 for j in ids}
    if raw and "{" in raw:
        try:
            for sp in json.loads(raw[raw.find("{"):]).get("spans", []):
                base = sp["id"][4:] if sp["id"].startswith("DAG-") else sp["id"]
                if base in counts:
                    counts[base] += 1
        except json.JSONDecodeError:
            pass
    return counts


def main():
    c = TitanClient()
    tag = int(time.time()) % 1000000
    work = os.path.abspath(f"perm_files/resume_ok_{tag}.py")
    gate = os.path.abspath(f"perm_files/resume_gate_{tag}.py")
    sentinel = os.path.abspath(f"perm_files/.resume_fixed_{tag}")

    open(work, "w").write("import time; time.sleep(0.2); print('ok')\n")
    # Fails until the sentinel exists. Standing in for "the script was broken, then fixed".
    open(gate, "w").write(
        "import os, sys, time\n"
        f"time.sleep(0.2)\n"
        f"if not os.path.exists({sentinel!r}):\n"
        "    sys.stderr.write('upstream not ready\\n'); sys.exit(2)\n"
        "print('ok')\n")
    for f in (work, gate):
        c.deploy_script(f)

    A, B, C, D, E, F = (f"rs{tag}-{n}" for n in "ABCDEF")
    ids = [A, B, C, D, E, F]

    def build():
        return [
            TitanJob(job_id=A, filename=work),
            TitanJob(job_id=B, filename=gate, parents=[A]),
            TitanJob(job_id=C, filename=work, parents=[B]),
            TitanJob(job_id=D, filename=work, parents=[C]),
            TitanJob(job_id=E, filename=work, parents=[B]),
            TitanJob(job_id=F, filename=work, parents=[E]),
        ]

    print("\n=== run 1: B fails, so C, D, E, F never get to run ===")
    c.submit_dag(f"resume-{tag}", build())
    st = settle(c, ids)
    check("A completed", st.get(A) == "COMPLETED", st.get(A))
    check("B did not complete", st.get(B) not in (None, "COMPLETED"), st.get(B))
    downstream = {j: st.get(j) for j in (C, D, E, F)}
    check("no descendant of B completed",
          all(v != "COMPLETED" for v in downstream.values()), str(downstream))
    before = spans_settled(c, ids)
    check("A produced exactly one span", before.get(A) == 1, f"{before.get(A)}")

    print("\n=== fix B, then RESUME rather than redeploy ===")
    open(sentinel, "w").write("fixed\n")
    t0 = time.time()
    c.submit_dag(f"resume-{tag}", build(), resume=True)
    st2 = settle(c, ids)
    print(f"    resume settled in {time.time()-t0:.1f}s")

    check("every job now COMPLETED",
          all(st2.get(j) == "COMPLETED" for j in ids),
          str({j: st2.get(j) for j in ids}))

    after = spans_settled(c, ids)
    check("A was SKIPPED, not re-executed",
          after.get(A) == before.get(A) == 1,
          f"spans before {before.get(A)} after {after.get(A)}")
    reran = [j for j in ids if after.get(j, 0) > before.get(j, 0)]
    expected = [B, C, D, E, F]
    check("exactly B and its descendants re-ran",
          sorted(reran) == sorted(expected),
          f"re-ran {sorted(x[-1] for x in reran)}, expected {sorted(x[-1] for x in expected)}")
    check("children of the skipped job were unlocked",
          after.get(C, 0) >= 1 and after.get(E, 0) >= 1,
          f"C={after.get(C)} E={after.get(E)}")

    print("\n=== resuming an already-complete DAG is a no-op ===")
    third = dict(after)
    c.submit_dag(f"resume-{tag}", build(), resume=True)
    time.sleep(3)
    final = spans_settled(c, ids)
    check("nothing re-ran when there was nothing to do",
          all(final.get(j, 0) == third.get(j, 0) for j in ids),
          str({j[-1]: (third.get(j), final.get(j)) for j in ids if final.get(j) != third.get(j)}))


    # ------------------------------------------------------------------
    # FAN-IN: a join whose parents are split between "skipped" and "re-run".
    #
    # This is the shape that produced a silent permanent hang this morning. A skipped parent never
    # emits a completion event, so if the join only learns about parents through unlockChildren it
    # will wait forever on work that is already done. It has to be satisfied at admission instead.
    print("\n=== fan-in: one parent skipped, one re-run, join must still fire ===")
    P, Q, J = (f"fi{tag}-{n}" for n in "PQJ")
    fids = [P, Q, J]
    gate2 = os.path.abspath(f"perm_files/resume_gate2_{tag}.py")
    sent2 = os.path.abspath(f"perm_files/.resume_fixed2_{tag}")
    open(gate2, "w").write(
        "import os, sys, time\n"
        "time.sleep(0.2)\n"
        f"if not os.path.exists({sent2!r}):\n"
        "    sys.stderr.write('not ready\\n'); sys.exit(2)\n"
        "print('ok')\n")
    c.deploy_script(gate2)

    def build_fanin():
        return [
            TitanJob(job_id=P, filename=work),                    # succeeds first time
            TitanJob(job_id=Q, filename=gate2),                   # fails first time
            TitanJob(job_id=J, filename=work, parents=[P, Q]),    # the join
        ]

    c.submit_dag(f"resume-fanin-{tag}", build_fanin())
    fst = settle(c, fids)
    check("fan-in: P completed, Q did not",
          fst.get(P) == "COMPLETED" and fst.get(Q) != "COMPLETED", f"P={fst.get(P)} Q={fst.get(Q)}")
    check("fan-in: the join did not run", fst.get(J) != "COMPLETED", fst.get(J))
    fbefore = spans_settled(c, fids)

    open(sent2, "w").write("fixed\n")
    c.submit_dag(f"resume-fanin-{tag}", build_fanin(), resume=True)
    fst2 = settle(c, fids)
    fafter = spans_settled(c, fids)
    check("fan-in: P was skipped", fafter.get(P) == fbefore.get(P) == 1,
          f"{fbefore.get(P)} -> {fafter.get(P)}")
    check("fan-in: the join FIRED on a skipped parent plus a re-run parent",
          fst2.get(J) == "COMPLETED" and fafter.get(J, 0) == 1,
          f"J={fst2.get(J)} spans={fafter.get(J)}")

    # ------------------------------------------------------------------
    # RESUME ACROSS A MASTER RESTART.
    #
    # executionHistory is in-memory, so after a restart the only record that a job ever succeeded
    # is in TitanStore. That fallback path in isAlreadyCompleted is otherwise never executed, and
    # resuming after a crash is the main reason the feature exists.
    print("\n=== resume across a Master restart (store fallback) ===")
    import subprocess
    jar = "target/titan-orchestrator-1.0-SNAPSHOT.jar"
    R1, R2 = (f"mr{tag}-{n}" for n in "12")
    rids = [R1, R2]
    gate3 = os.path.abspath(f"perm_files/resume_gate3_{tag}.py")
    sent3 = os.path.abspath(f"perm_files/.resume_fixed3_{tag}")
    open(gate3, "w").write(
        "import os, sys, time\n"
        "time.sleep(0.2)\n"
        f"if not os.path.exists({sent3!r}):\n"
        "    sys.stderr.write('not ready\\n'); sys.exit(2)\n"
        "print('ok')\n")
    c.deploy_script(gate3)

    def build_restart():
        return [TitanJob(job_id=R1, filename=work),
                TitanJob(job_id=R2, filename=gate3, parents=[R1])]

    c.submit_dag(f"resume-restart-{tag}", build_restart())
    rst = settle(c, rids)
    check("restart case: first job completed before the crash", rst.get(R1) == "COMPLETED", rst.get(R1))
    rbefore = spans_settled(c, rids)

    subprocess.run(["pkill", "-f", "titan.TitanMaster"], capture_output=True)
    time.sleep(3)
    subprocess.Popen(["java", "-cp", jar, "titan.TitanMaster"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # wait for the Master to accept connections and for workers to re-register
    for _ in range(40):
        try:
            if json.loads(c._send_request(OP_STATS, "metrics"))["queue"][-1][5] > 0:
                break
        except Exception:
            pass
        time.sleep(1)
    time.sleep(4)

    open(sent3, "w").write("fixed\n")
    c.submit_dag(f"resume-restart-{tag}", build_restart(), resume=True)
    rst2 = settle(c, rids)
    rafter = spans_settled(c, rids)
    check("restart case: both jobs COMPLETED after resume",
          all(rst2.get(j) == "COMPLETED" for j in rids), str(rst2))
    check("restart case: the pre-crash job was SKIPPED from the STORE, not re-run",
          rafter.get(R1) == rbefore.get(R1) == 1,
          f"spans {rbefore.get(R1)} -> {rafter.get(R1)}")

    for f in (gate2, sent2, gate3, sent3):
        try:
            os.remove(f)
        except OSError:
            pass

    for f in (work, gate, sentinel):
        try:
            os.remove(f)
        except OSError:
            pass

    print("\n" + "=" * 62)
    print(f" RESULT: {PASS}/{PASS + FAIL} passed")
    print("=" * 62)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
