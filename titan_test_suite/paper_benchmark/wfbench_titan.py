"""Execute an official WfBench graph through Titan without changing the engine."""
from __future__ import annotations
import argparse,base64,json,sys,time,uuid
from pathlib import Path
REPO=Path(__file__).resolve().parents[2]; sys.path.insert(0,str(REPO))
import titan_sdk.titan_sdk as sdk
from titan_sdk.titan_sdk import TitanClient,TitanJob
from titan_test_suite.external_agent_benchmark.runner import wait_for
from .common import OUTLIER_POLICY,latency,load_graph,summarize,write_json

def enc(v): return base64.urlsafe_b64encode(json.dumps(v,separators=(",",":")).encode()).decode().rstrip("=")

def measured_spans(root: Path | None, expected: set[str]) -> list[dict]:
    if root is None: return []
    by_job: dict[str,list[dict]] = {}
    for path in sorted(root.glob("spans-*.jsonl")):
        for line in path.read_text(encoding="utf-8",errors="replace").splitlines():
            try: item=json.loads(line)
            except json.JSONDecodeError: continue
            job=str(item.get("job_id") or "")
            if job in expected: by_job.setdefault(job,[]).append(item)
    latest=[]
    for job,items in by_job.items():
        latest.append(max(items,key=lambda x:(int(x.get("attempt") or 0),int(x.get("ended_at") or 0))))
    return latest

def span_metrics(spans: list[dict]) -> dict:
    tasks=[]
    for span in spans:
        eligible=int(span.get("eligible_at") or span.get("queued_at") or 0)
        started=int(span.get("started_at") or 0); ended=int(span.get("ended_at") or 0)
        if not started or not ended: continue
        tasks.append({"job_id":span.get("job_id"),"worker":span.get("worker"),
                      "attempt":span.get("attempt"),"queue_ms":max(0,started-eligible) if eligible else None,
                      "execution_ms":max(0,ended-started),"started_at":started,"ended_at":ended})
    queue=[x["queue_ms"] for x in tasks if x["queue_ms"] is not None]
    execution=[x["execution_ms"] for x in tasks]
    return {"task_metrics":tasks,"queue_ms":latency(queue),"execution_ms":latency(execution)}

def main():
    p=argparse.ArgumentParser(); p.add_argument("--workflow",type=Path,required=True); p.add_argument("--host",default="127.0.0.1")
    p.add_argument("--port",type=int,default=9090); p.add_argument("--passes",type=int,default=5); p.add_argument("--warmups",type=int,default=1)
    p.add_argument("--timeout",type=float,default=900); p.add_argument("--output",type=Path,required=True)
    p.add_argument("--spans-dir",type=Path); a=p.parse_args()
    sdk.TITAN_HOST,sdk.TITAN_PORT=a.host,a.port; raw,tasks,parents=load_graph(a.workflow); rows=[]
    stage=str((Path(__file__).parent/"wfbench_stage.py").resolve())
    for ix in range(a.warmups+a.passes):
        token=uuid.uuid4().hex[:8]; idmap={n:f"wfb_{ix}_{token}_{i}" for i,n in enumerate(tasks)}; jobs=[]
        for n,t in tasks.items():
            payload=enc({"name":n,"command":t.get("command",{})})
            jobs.append(TitanJob(idmap[n],stage,args=payload,parents=[idmap[x] for x in parents[n]],requirement="GENERAL"))
        dag=f"PAPER_WFBENCH_{ix}_{token}"; start=time.time(); TitanClient().submit_dag(dag,jobs)
        client=TitanClient(); statuses={}
        for n,j in idmap.items(): statuses[n]=wait_for(client,"DAG-"+j,a.timeout)
        wall=time.time()-start; ok=all(str(v).upper()=="COMPLETED" for v in statuses.values())
        spans=measured_spans(a.spans_dir,{"DAG-"+value for value in idmap.values()})
        metrics=span_metrics(spans)
        rows.append({"pass":ix-a.warmups+1,"warmup":ix<a.warmups,"dag":dag,"wall_seconds":wall,
                     "throughput":len(jobs)/wall,"completed":sum(str(v).upper()=="COMPLETED" for v in statuses.values()),
                     "span_count":len(spans),**metrics,"valid":ok})
        write_json(a.output,{"rows":rows})
        if not ok: raise RuntimeError(f"WfBench pass failed: {statuses}")
    measured=[r for r in rows if not r["warmup"]]
    write_json(a.output,{"schema":"titan-executable-wfbench-v1","tasks":len(tasks),"passes":a.passes,"warmups":a.warmups,
      "outlier_policy":OUTLIER_POLICY,"wall_seconds":summarize([r["wall_seconds"] for r in measured]),
      "throughput":summarize([r["throughput"] for r in measured]),
      "queue_ms":latency([v["queue_ms"] for r in measured for v in r["task_metrics"] if v["queue_ms"] is not None]),
      "execution_ms":latency([v["execution_ms"] for r in measured for v in r["task_metrics"]]),
      "rows":rows,"source_workflow":raw})
if __name__=="__main__": main()
