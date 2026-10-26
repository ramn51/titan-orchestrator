"""Run matched Dask or Ray baselines on a WfCommons graph and a duration sweep."""
from __future__ import annotations
import argparse, json, os, subprocess, sys, time
from pathlib import Path
from .common import OUTLIER_POLICY, latency, load_graph, summarize, write_json

def work(node, ms, mode, *parents):
    started=time.time()
    if mode=="external":
        subprocess.run([sys.executable,"-c",f"import time; time.sleep({ms/1000.0!r})"],check=True)
    else: time.sleep(ms/1000.0)
    return {"node":node,"parent_count":len(parents),"host":os.uname().nodename,
            "started":started,"finished":time.time()}

def dask_once(address, parents, duration, mode):
    from dask.distributed import Client
    c=Client(address); futures={}
    pending=set(parents)
    while pending:
        ready=sorted(n for n in pending if all(p in futures for p in parents[n]))
        if not ready: raise RuntimeError("cycle or missing parent")
        for n in ready:
            deps=[futures[p] for p in parents[n]]
            futures[n]=c.submit(work,n,duration,mode,*deps,pure=False)
            pending.remove(n)
    t0=time.time(); results=c.gather(list(futures.values())); elapsed=time.time()-t0; c.close()
    return results,elapsed

def ray_once(address, parents, duration, mode):
    import ray
    ray.init(address=address,ignore_reinit_error=True,logging_level="ERROR")
    remote=ray.remote(work); futures={}; pending=set(parents)
    while pending:
        ready=sorted(n for n in pending if all(p in futures for p in parents[n]))
        if not ready: raise RuntimeError("cycle or missing parent")
        for n in ready:
            deps=[futures[p] for p in parents[n]]
            futures[n]=remote.remote(n,duration,mode,*deps); pending.remove(n)
    t0=time.time(); results=ray.get(list(futures.values())); elapsed=time.time()-t0
    ray.shutdown(); return results,elapsed

def run_case(runtime,address,parents,duration,mode,passes,warmups):
    runner=dask_once if runtime=="dask" else ray_once; rows=[]
    for idx in range(warmups+passes):
        wall0=time.time(); results,wait=runner(address,parents,duration,mode); wall=time.time()-wall0
        hosts=sorted(set(r["host"] for r in results)); exec_ms=[(r["finished"]-r["started"])*1000 for r in results]
        rows.append({"pass":idx-warmups+1,"warmup":idx<warmups,"wall_seconds":wall,
                     "gather_seconds":wait,"throughput":len(results)/wall,"hosts":hosts,
                     "execution_ms":latency(exec_ms),"valid":len(results)==len(parents)})
    measured=[r for r in rows if not r["warmup"]]
    return {"rows":rows,"wall_seconds":summarize([r["wall_seconds"] for r in measured]),
            "throughput":summarize([r["throughput"] for r in measured]),
            "worker_hosts":sorted(set(h for r in measured for h in r["hosts"]))}

def main():
    p=argparse.ArgumentParser(); p.add_argument("--runtime",choices=["dask","ray"],required=True)
    p.add_argument("--mode",choices=["native","external"],required=True); p.add_argument("--address",required=True)
    p.add_argument("--workflow",type=Path,required=True); p.add_argument("--output",type=Path,required=True)
    p.add_argument("--passes",type=int,default=5); p.add_argument("--warmups",type=int,default=1)
    p.add_argument("--workflow-ms",type=float,default=100); p.add_argument("--tasks",type=int,default=32)
    p.add_argument("--suites",choices=["both","workflow","sweep"],default="both")
    p.add_argument("--durations",default="10,20,50,100,200,500,1000,2000"); a=p.parse_args()
    _,tasks,parents=load_graph(a.workflow)
    result={"schema":"titan-peer-baseline-v1","runtime":a.runtime,"mode":a.mode,
            "configuration":{"passes":a.passes,"warmups":a.warmups,"workflow":str(a.workflow),
            "workflow_tasks":len(tasks),"workflow_duration_ms":a.workflow_ms,"duration_tasks":a.tasks},
            "outlier_policy":OUTLIER_POLICY,"workflow":None,"sweep":[]}
    if a.suites in {"both","workflow"}:
        result["workflow"]=run_case(a.runtime,a.address,parents,a.workflow_ms,a.mode,a.passes,a.warmups)
        write_json(a.output,result)
    bag={f"task_{i:03d}":[] for i in range(a.tasks)}
    if a.suites in {"both","sweep"}:
        for ms in [float(x) for x in a.durations.split(",")]:
            result["sweep"].append({"duration_ms":ms,**run_case(a.runtime,a.address,bag,ms,a.mode,a.passes,a.warmups)})
            write_json(a.output,result)
    write_json(a.output,result)
if __name__=="__main__": main()
