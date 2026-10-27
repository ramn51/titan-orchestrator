"""Run matched Dask or Ray baselines with dependency-aware task latency."""
from __future__ import annotations
import argparse, base64, json, os, subprocess, sys, time
from pathlib import Path
from .common import OUTLIER_POLICY, latency, load_graph, summarize, write_json

def encode(value):
    return base64.urlsafe_b64encode(json.dumps(value,separators=(",",":")).encode()).decode().rstrip("=")

def work(node, ms, mode, batch_started, payload, *parents):
    eligible=max([float(p["finished"]) for p in parents],default=batch_started)
    started=time.time()
    if mode=="external":
        subprocess.run([sys.executable,"-c",f"import time; time.sleep({ms/1000.0!r})"],check=True)
    elif mode=="wfbench":
        cp=subprocess.run([sys.executable,payload["stage"],payload["encoded"]],capture_output=True,text=True)
        if cp.returncode:
            raise RuntimeError(f"WfBench task {node} failed: {cp.stderr[-1000:]}")
    else:
        time.sleep(ms/1000.0)
    finished=time.time()
    return {"node":node,"parent_count":len(parents),"host":os.uname().nodename,
            "eligible":eligible,"started":started,"finished":finished,
            # Worker clocks are NTP-synchronised but may still differ by a few
            # milliseconds.  A task cannot have negative queue time, so clamp
            # that small cross-host clock error at zero.
            "queue_ms":max(0.0,(started-eligible)*1000),"execution_ms":(finished-started)*1000,
            "batch_end_ms":(finished-batch_started)*1000}

def dask_once(address, parents, duration, mode, payloads):
    from dask.distributed import Client
    c=Client(address); futures={}; t0=time.time()
    pending=set(parents)
    while pending:
        ready=sorted(n for n in pending if all(p in futures for p in parents[n]))
        if not ready: raise RuntimeError("cycle or missing parent")
        for n in ready:
            deps=[futures[p] for p in parents[n]]
            futures[n]=c.submit(work,n,duration,mode,t0,payloads.get(n),*deps,pure=False)
            pending.remove(n)
    results=c.gather(list(futures.values())); elapsed=time.time()-t0; c.close()
    return results,elapsed

def ray_once(address, parents, duration, mode, payloads):
    import ray
    ray.init(address=address,ignore_reinit_error=True,logging_level="ERROR")
    remote=ray.remote(max_retries=1)(work); futures={}; pending=set(parents); t0=time.time()
    while pending:
        ready=sorted(n for n in pending if all(p in futures for p in parents[n]))
        if not ready: raise RuntimeError("cycle or missing parent")
        for n in ready:
            deps=[futures[p] for p in parents[n]]
            futures[n]=remote.remote(n,duration,mode,t0,payloads.get(n),*deps); pending.remove(n)
    results=ray.get(list(futures.values())); elapsed=time.time()-t0
    ray.shutdown(); return results,elapsed

def run_case(runtime,address,parents,duration,mode,passes,warmups,payloads=None):
    runner=dask_once if runtime=="dask" else ray_once; rows=[]
    payloads=payloads or {}
    for idx in range(warmups+passes):
        wall0=time.time(); results,wait=runner(address,parents,duration,mode,payloads); wall=time.time()-wall0
        hosts=sorted(set(r["host"] for r in results)); exec_ms=[r["execution_ms"] for r in results]
        rows.append({"pass":idx-warmups+1,"warmup":idx<warmups,"wall_seconds":wall,
                     "gather_seconds":wait,"throughput":len(results)/wall,"hosts":hosts,
                     "queue_ms":latency([r["queue_ms"] for r in results]),
                     "execution_ms":latency(exec_ms),
                     "end_to_end_ms":latency([r["batch_end_ms"] for r in results]),
                     "task_metrics":results,"valid":len(results)==len(parents)})
    measured=[r for r in rows if not r["warmup"]]
    return {"rows":rows,"wall_seconds":summarize([r["wall_seconds"] for r in measured]),
            "throughput":summarize([r["throughput"] for r in measured]),
            "queue_ms":latency([v["queue_ms"] for r in measured for v in r["task_metrics"]]),
            "execution_ms":latency([v["execution_ms"] for r in measured for v in r["task_metrics"]]),
            "end_to_end_ms":latency([v["batch_end_ms"] for r in measured for v in r["task_metrics"]]),
            "worker_hosts":sorted(set(h for r in measured for h in r["hosts"]))}

def main():
    p=argparse.ArgumentParser(); p.add_argument("--runtime",choices=["dask","ray"],required=True)
    p.add_argument("--mode",choices=["native","external","wfbench"],required=True); p.add_argument("--address",required=True)
    p.add_argument("--workflow",type=Path,required=True); p.add_argument("--output",type=Path,required=True)
    p.add_argument("--passes",type=int,default=5); p.add_argument("--warmups",type=int,default=1)
    p.add_argument("--workflow-ms",type=float,default=100); p.add_argument("--tasks",type=int,default=32)
    p.add_argument("--wfbench-stage",type=Path)
    p.add_argument("--suites",choices=["both","workflow","sweep"],default="both")
    p.add_argument("--durations",default="10,20,50,100,200,500,1000,2000"); a=p.parse_args()
    _,tasks,parents=load_graph(a.workflow)
    payloads={}
    if a.mode=="wfbench":
        if not a.wfbench_stage: p.error("--wfbench-stage is required in wfbench mode")
        payloads={n:{"stage":str(a.wfbench_stage),"encoded":encode({"name":n,"command":t.get("command",{})})}
                  for n,t in tasks.items()}
    result={"schema":"titan-peer-baseline-v1","runtime":a.runtime,"mode":a.mode,
            "configuration":{"passes":a.passes,"warmups":a.warmups,"workflow":str(a.workflow),
            "workflow_tasks":len(tasks),"workflow_duration_ms":a.workflow_ms,"duration_tasks":a.tasks},
            "outlier_policy":OUTLIER_POLICY,"workflow":None,"sweep":[]}
    if a.suites in {"both","workflow"}:
        result["workflow"]=run_case(a.runtime,a.address,parents,a.workflow_ms,a.mode,a.passes,a.warmups,payloads)
        write_json(a.output,result)
    bag={f"task_{i:03d}":[] for i in range(a.tasks)}
    if a.suites in {"both","sweep"}:
        for ms in [float(x) for x in a.durations.split(",")]:
            if a.mode=="wfbench": p.error("wfbench mode only supports --suites workflow")
            result["sweep"].append({"duration_ms":ms,**run_case(a.runtime,a.address,bag,ms,a.mode,a.passes,a.warmups)})
            write_json(a.output,result)
    write_json(a.output,result)
if __name__=="__main__": main()
