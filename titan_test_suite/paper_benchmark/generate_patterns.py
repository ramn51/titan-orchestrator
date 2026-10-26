"""Generate identical JSON DAG definitions for Titan, Dask and Ray."""
from __future__ import annotations
import argparse, json
from pathlib import Path
from titan_test_suite.task_granularity_benchmark.runner import make_graph

def main():
    p=argparse.ArgumentParser(); p.add_argument("--output",type=Path,required=True)
    p.add_argument("--count",type=int,default=256); p.add_argument("--duration-ms",type=float,default=100)
    p.add_argument("--patterns",default="bag,chain,fanout,fanin,diamond,layered,stencil"); a=p.parse_args()
    a.output.mkdir(parents=True,exist_ok=True)
    for pattern in [x.strip() for x in a.patterns.split(",") if x.strip()]:
        specs=make_graph(pattern,a.count,a.duration_ms,pattern)
        value={"workflow":{"name":f"matched-{pattern}-{a.count}","tasks":[
            {"id":s.node_id,"parents":list(s.parents),"duration_ms":s.duration_ms} for s in specs]}}
        (a.output/f"{pattern}-{a.count}.json").write_text(json.dumps(value,indent=2)+"\n",encoding="utf-8")

if __name__=="__main__": main()
