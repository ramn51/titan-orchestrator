"""Execute an official WfBench graph through Titan without changing the engine."""
from __future__ import annotations
import argparse,base64,json,sys,time,uuid
from pathlib import Path
REPO=Path(__file__).resolve().parents[2]; sys.path.insert(0,str(REPO))
import titan_sdk.titan_sdk as sdk
from titan_sdk.titan_sdk import TitanClient,TitanJob
from titan_test_suite.external_agent_benchmark.runner import wait_for
from .common import OUTLIER_POLICY,load_graph,summarize,write_json

def enc(v): return base64.urlsafe_b64encode(json.dumps(v,separators=(",",":")).encode()).decode().rstrip("=")
def main():
    p=argparse.ArgumentParser(); p.add_argument("--workflow",type=Path,required=True); p.add_argument("--host",default="127.0.0.1")
    p.add_argument("--port",type=int,default=9090); p.add_argument("--passes",type=int,default=5); p.add_argument("--warmups",type=int,default=1)
    p.add_argument("--timeout",type=float,default=900); p.add_argument("--output",type=Path,required=True); a=p.parse_args()
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
        rows.append({"pass":ix-a.warmups+1,"warmup":ix<a.warmups,"dag":dag,"wall_seconds":wall,
                     "throughput":len(jobs)/wall,"completed":sum(str(v).upper()=="COMPLETED" for v in statuses.values()),"valid":ok})
        write_json(a.output,{"rows":rows})
        if not ok: raise RuntimeError(f"WfBench pass failed: {statuses}")
    measured=[r for r in rows if not r["warmup"]]
    write_json(a.output,{"schema":"titan-executable-wfbench-v1","tasks":len(tasks),"passes":a.passes,"warmups":a.warmups,
      "outlier_policy":OUTLIER_POLICY,"wall_seconds":summarize([r["wall_seconds"] for r in measured]),
      "throughput":summarize([r["throughput"] for r in measured]),"rows":rows,"source_workflow":raw})
if __name__=="__main__": main()
