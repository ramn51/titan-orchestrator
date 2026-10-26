"""Dependency-free Linux host CPU, memory and network sampler."""
from __future__ import annotations
import argparse, json, os, socket, time
from pathlib import Path

def cpu():
    fields=list(map(int,Path("/proc/stat").read_text().splitlines()[0].split()[1:])); return sum(fields),fields[3]+fields[4]
def memory():
    values={k.rstrip(":"):int(v.split()[0])*1024 for k,v in (line.split(None,1) for line in Path("/proc/meminfo").read_text().splitlines())}
    return values["MemTotal"],values.get("MemAvailable",values["MemFree"])
def network():
    rx=tx=0
    for line in Path("/proc/net/dev").read_text().splitlines()[2:]:
        name,data=line.split(":",1)
        if name.strip()=="lo": continue
        values=data.split(); rx+=int(values[0]); tx+=int(values[8])
    return rx,tx
def main():
    p=argparse.ArgumentParser(); p.add_argument("--output",type=Path,required=True); p.add_argument("--interval",type=float,default=1); a=p.parse_args()
    a.output.parent.mkdir(parents=True,exist_ok=True); prev_total,prev_idle=cpu(); prev_time=time.time()
    with a.output.open("a",encoding="utf-8",buffering=1) as out:
        while True:
            time.sleep(a.interval); now=time.time(); total,idle=cpu(); mem_total,mem_available=memory(); rx,tx=network()
            delta=max(1,total-prev_total); busy=100*(1-(idle-prev_idle)/delta)
            out.write(json.dumps({"epoch":now,"host":socket.gethostname(),"pid":os.getpid(),"cpu_busy_pct":busy,
                "memory_used_bytes":mem_total-mem_available,"memory_total_bytes":mem_total,"rx_bytes":rx,"tx_bytes":tx})+"\n")
            prev_total,prev_idle,prev_time=total,idle,now
if __name__=="__main__": main()
