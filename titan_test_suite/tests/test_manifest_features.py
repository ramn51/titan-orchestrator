"""Guards the two dashboard features that depend on the manifest's non-trivial record types.

Proves the manifest write path did not break the two features that read its
non-trivial record types: Agent Runs (accumulative `stages`) and the DAG visualizer.

Agent Runs is the risky one. Its record accumulates across submissions rather than being
overwritten, so a write path that only ever records "the latest" would silently show one stage.
"""
import os, sys, time, json, urllib.request
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from titan_sdk.titan_sdk import TitanClient, TitanJob

PASS = FAIL = 0
def check(what, ok, detail=""):
    global PASS, FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {what}" + (f" - {detail}" if detail else ""))
    if ok: PASS += 1
    else:  FAIL += 1

def get(path):
    with urllib.request.urlopen(f"http://127.0.0.1:5000{path}", timeout=180) as r:
        return r.status, r.read().decode("utf-8", "replace")

c = TitanClient()
noop = "/tmp/vm_noop.py"; open(noop,"w").write("print('ok')\n")
c.deploy_script(noop)
run_id = f"verify-agentrun-{int(time.time())}"
tag = int(time.time()) % 100000
STAGES = ["RESEARCH_plan", "RESEARCH_gather", "RESEARCH_write"]

print("\n=== a 3-stage agent run, submitted as 3 separate DAGs sharing one agent_run_id ===")
for i, stage in enumerate(STAGES):
    c.submit_dag(stage, [TitanJob(job_id=f"ar{tag}-{i}", filename=noop)], agent_run_id=run_id)
    time.sleep(0.4)

time.sleep(2.5)
man = json.load(open(".titan_dag_manifest.json"))
key = f"__agent_run__{run_id}"
entry = man.get(key)
check("the agent run record exists in the manifest", entry is not None, key)
if entry:
    got = entry.get("stages", [])
    check("ALL stages accumulated, not just the last one",
          got == STAGES, f"{got}")
    check("the record keeps its agent_run_id", entry.get("agent_run_id") == run_id)
    check("the record carries a run timestamp", bool(entry.get("run_ts")))

print("\n=== the per-job records the DAG visualizer needs ===")
for i, stage in enumerate(STAGES):
    jk = f"DAG-ar{tag}-{i}"
    e = man.get(jk)
    check(f"job record {i} maps to its pipeline name", isinstance(e, dict) and e.get("dag") == stage,
          f"{jk} -> {e.get('dag') if isinstance(e, dict) else e}")
    check(f"job record {i} carries agent_run_id", isinstance(e, dict) and e.get("agent_run_id") == run_id)

print("\n=== replay/redeploy records (payload round trip) ===")
check("a DAG payload was stored for redeploy",
      f"__payload__{STAGES[0]}" in man)
check("a per-job payload was stored for single-job replay",
      any(k.startswith("__job_payload__DAG-ar") for k in man))

print("\n=== the pages actually render ===")
try:
    s, body = get("/agents")
    check("Agent Runs page returns 200", s == 200, f"HTTP {s}")
    check("Agent Runs page shows this run", run_id in body or STAGES[0] in body,
          f"{len(body)} bytes")
    shown = sum(1 for st_ in STAGES if st_ in body)
    check("Agent Runs page shows every stage", shown == len(STAGES), f"{shown}/{len(STAGES)}")
except Exception as e:
    check("Agent Runs page reachable", False, str(e))

try:
    s, body = get("/dags")
    check("DAG visualizer index returns 200", s == 200, f"HTTP {s}")
    check("DAG visualizer lists pipelines", "DAG" in body or "dag" in body, f"{len(body)} bytes")
    s2, body2 = get(f"/dags/{STAGES[0]}")
    check("DAG visualizer opens a specific pipeline", s2 == 200, f"HTTP {s2}")
except Exception as e:
    check("DAG visualizer reachable", False, str(e))

try:
    s, body = get("/api/job_dag_map")
    m = json.loads(body)
    check("job_dag_map API still serves the mapping", s == 200 and bool(m.get("map")),
          f"{len(m.get('map', {}))} entries")
    check("this run appears in job_dag_map", any(f"ar{tag}" in k for k in m.get("map", {})))
except Exception as e:
    check("job_dag_map reachable", False, str(e))

print("\n" + "=" * 64)
print(f" RESULT: {PASS}/{PASS+FAIL} passed")
print("=" * 64)
sys.exit(1 if FAIL else 0)
