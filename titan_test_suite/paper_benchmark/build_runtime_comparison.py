"""Build a self-contained Titan, Dask, and Ray comparison report."""
from __future__ import annotations

import argparse
import html
import json
import math
import statistics
from pathlib import Path

from .common import OUTLIER_POLICY, latency, summarize


RUNTIME_COLORS = {"Titan": "#58e0a1", "Dask": "#efb54a", "Ray": "#68a7ff"}


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def peer_duration(path: Path) -> list[dict]:
    data = load(path)
    return [{"duration_ms": row["duration_ms"],
             "throughput": row["throughput"]["median"],
             "queue_p50_ms": row["queue_ms"]["p50"],
             "queue_p95_ms": row["queue_ms"]["p95"],
             "queue_p99_ms": row["queue_ms"]["p99"],
             "execution_p50_ms": row["execution_ms"]["p50"],
             "execution_p95_ms": row["execution_ms"]["p95"],
             "execution_p99_ms": row["execution_ms"]["p99"],
             "variability": row["throughput"]}
            for row in data["sweep"]]


def titan_duration(summary: dict) -> list[dict]:
    return [{"duration_ms": row["requested_duration_ms"],
             "throughput": row["throughput"],
             "queue_p50_ms": row["queue_p50_ms"],
             "queue_p95_ms": row["queue_p95_ms"],
             "queue_p99_ms": row["queue_p99_ms"],
             "execution_p50_ms": row["execution_p50_ms"],
             "execution_p95_ms": row["execution_p95_ms"],
             "execution_p99_ms": row["execution_p99_ms"],
             "efficiency": row["efficiency"], "variability": row["variability"]["throughput"]}
            for row in summary["granularity_summary"]]


def peer_patterns(directory: Path) -> dict[str, dict]:
    result = {}
    for path in sorted(directory.glob("*.json")):
        workflow = load(path)["workflow"]
        result[path.stem] = {"wall": workflow["wall_seconds"],
                             "throughput": workflow["throughput"],
                             "queue_ms": workflow["queue_ms"],
                             "execution_ms": workflow["execution_ms"],
                             "workers": workflow["worker_hosts"]}
    return result


def titan_patterns(summary: dict) -> dict[str, dict]:
    grouped: dict[str, list[dict]] = {}
    for row in summary["results"]:
        if row["suite"] == "patterns":
            grouped.setdefault(row["label"], []).append(row)
    result = {}
    for name, rows in grouped.items():
        result[name] = {
            "wall": summarize([row["wall_seconds"] for row in rows]),
            "throughput": summarize([row["throughput_tasks_per_second"] for row in rows]),
            "queue_ms": latency([value for row in rows for value in row["queue_ms_samples"]]),
            "execution_ms": latency([value for row in rows for value in row["execution_ms_samples"]]),
            "workers": sorted({worker for row in rows for worker in row["workers"]}),
        }
    return result


def svg_lines(series: dict[str, list[dict]], field: str, title: str, y_label: str) -> str:
    width, height, left, right, top, bottom = 900, 330, 72, 24, 35, 60
    values = [float(row[field]) for rows in series.values() for row in rows if row.get(field) is not None]
    if not values:
        return "<p>No samples.</p>"
    maximum = max(values) * 1.08 or 1
    durations = sorted({float(row["duration_ms"]) for rows in series.values() for row in rows})
    logs = [math.log10(value) for value in durations]
    x = lambda value: left + (math.log10(value) - min(logs)) / max(.001, max(logs) - min(logs)) * (width-left-right)
    y = lambda value: top + (1 - value / maximum) * (height-top-bottom)
    grid = "".join(f'<line x1="{left}" x2="{width-right}" y1="{y(maximum*i/4):.1f}" y2="{y(maximum*i/4):.1f}"/>'
                   f'<text x="{left-10}" y="{y(maximum*i/4)+4:.1f}" text-anchor="end">{maximum*i/4:.1f}</text>'
                   for i in range(5))
    paths, marks = [], []
    for name, rows in series.items():
        color = RUNTIME_COLORS[name]
        points = " ".join(f'{x(float(row["duration_ms"])):.1f},{y(float(row[field])):.1f}' for row in rows)
        paths.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="3"/>')
        marks.extend(f'<circle cx="{x(float(row["duration_ms"])):.1f}" cy="{y(float(row[field])):.1f}" r="4" fill="#071013" stroke="{color}" stroke-width="3"><title>{name}: {row[field]:.3f}</title></circle>' for row in rows)
    xlabels = "".join(f'<text x="{x(value):.1f}" y="{height-28}" text-anchor="middle">{value:g} ms</text>' for value in durations)
    legend = "".join(f'<g transform="translate({left+index*150} 18)"><circle r="5" fill="{RUNTIME_COLORS[name]}"/><text x="10" y="4">{name}</text></g>' for index,name in enumerate(series))
    return f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}"><g class="legend">{legend}</g><g class="grid">{grid}</g>{"".join(paths)}{"".join(marks)}<g class="axis">{xlabels}<text x="{(left+width-right)/2}" y="{height-5}" text-anchor="middle">requested task duration · logarithmic scale</text><text transform="translate(16 {(top+height-bottom)/2}) rotate(-90)" text-anchor="middle">{html.escape(y_label)}</text></g></svg>'


def svg_grouped(patterns: dict[str, dict[str, dict]]) -> str:
    names = [name for name in ("bag", "chain", "diamond", "fanin", "fanout", "layered", "stencil")
             if all(name in values for values in patterns.values())]
    width, height, left, right, top, bottom = 950, 340, 72, 20, 35, 72
    maximum = max(patterns[r][n]["wall"]["median"] for r in patterns for n in names) * 1.1
    group_width = (width-left-right)/max(1,len(names)); bar_width = group_width/(len(patterns)+1)
    y = lambda value: top+(1-value/maximum)*(height-top-bottom)
    bars=[]
    for group,name in enumerate(names):
        for offset,runtime in enumerate(patterns):
            value=patterns[runtime][name]["wall"]["median"]
            xpos=left+group*group_width+(offset+.5)*bar_width
            bars.append(f'<rect x="{xpos:.1f}" y="{y(value):.1f}" width="{bar_width*.82:.1f}" height="{height-bottom-y(value):.1f}" rx="3" fill="{RUNTIME_COLORS[runtime]}"><title>{runtime} {name}: {value:.3f}s</title></rect>')
    labels="".join(f'<text x="{left+(i+.5)*group_width:.1f}" y="{height-38}" text-anchor="middle">{name}</text>' for i,name in enumerate(names))
    grid="".join(f'<line x1="{left}" x2="{width-right}" y1="{y(maximum*i/4):.1f}" y2="{y(maximum*i/4):.1f}"/><text x="{left-10}" y="{y(maximum*i/4)+4:.1f}" text-anchor="end">{maximum*i/4:.1f}s</text>' for i in range(5))
    legend="".join(f'<g transform="translate({left+i*150} 18)"><circle r="5" fill="{RUNTIME_COLORS[n]}"/><text x="10" y="4">{n}</text></g>' for i,n in enumerate(patterns))
    return f'<svg class="chart" viewBox="0 0 {width} {height}"><g class="legend">{legend}</g><g class="grid">{grid}</g>{"".join(bars)}<g class="axis">{labels}<text x="{(left+width-right)/2}" y="{height-5}" text-anchor="middle">256-task graph pattern</text></g></svg>'


def parse_sampler(path: Path) -> list[dict]:
    values=[]
    for line in path.read_text(encoding="utf-8",errors="replace").splitlines():
        try: values.append(json.loads(line))
        except json.JSONDecodeError: pass
    return values


def measured_intervals_peer(documents: list[dict]) -> list[tuple[float,float]]:
    intervals=[]
    for data in documents:
        groups=[]
        if data.get("workflow"): groups.append(data["workflow"])
        groups += data.get("sweep",[])
        for group in groups:
            for row in group.get("rows",[]):
                if row.get("warmup"): continue
                tasks=row.get("task_metrics",[])
                if tasks: intervals.append((min(t["started"] for t in tasks),max(t["finished"] for t in tasks)))
    return intervals


def measured_intervals_titan(summary: dict, wfbench: dict, telemetry: list[Path]) -> list[tuple[float,float]]:
    prefixes=[]
    for row in summary.get("results",[]):
        digest=str(row.get("dag") or "").rsplit("_",1)[-1]
        if digest: prefixes.append("DAG-tb_"+digest+"_")
    for row in wfbench.get("rows",[]):
        if row.get("warmup"): continue
        parts=str(row.get("dag") or "").split("_")
        if len(parts)>=4: prefixes.append(f"DAG-wfb_{parts[-2]}_{parts[-1]}_")
    intervals=[]
    for source in telemetry:
        paths=source.glob("spans-*.jsonl") if source.is_dir() else [source]
        for path in paths:
            for line in path.read_text(encoding="utf-8",errors="replace").splitlines():
                try: span=json.loads(line)
                except json.JSONDecodeError: continue
                if any(str(span.get("job_id") or "").startswith(prefix) for prefix in prefixes):
                    started=float(span.get("started_at") or 0)/1000
                    ended=float(span.get("ended_at") or 0)/1000
                    if started and ended: intervals.append((started,ended))
    return intervals


def resource_summary(samples: list[dict], intervals: list[tuple[float,float]]) -> dict:
    selected=[row for row in samples if any(start <= row["epoch"] <= end for start,end in intervals)]
    by_host: dict[str,list[dict]]={}
    for row in selected: by_host.setdefault(row["host"],[]).append(row)
    network=sum(max(v["rx_bytes"] for v in rows)-min(v["rx_bytes"] for v in rows)
                +max(v["tx_bytes"] for v in rows)-min(v["tx_bytes"] for v in rows)
                for rows in by_host.values() if len(rows)>1)
    return {"samples":len(selected),"hosts":len(by_host),
            "cpu_mean_pct":statistics.fmean(row["cpu_busy_pct"] for row in selected) if selected else None,
            "cpu_p95_pct":latency([row["cpu_busy_pct"] for row in selected])["p95"] if selected else None,
            "memory_peak_gib":max((row["memory_used_bytes"] for row in selected),default=0)/2**30,
            "network_mib":network/2**20}


def fmt(value, digits=2):
    return "—" if value is None else f"{value:.{digits}f}"


def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--titan-summary",type=Path,required=True)
    parser.add_argument("--titan-wfbench",type=Path,required=True)
    parser.add_argument("--titan-worker-fault",type=Path,required=True)
    parser.add_argument("--titan-head-fault",type=Path,required=True)
    parser.add_argument("--utilization",type=Path,action="append",default=[])
    parser.add_argument("--titan-telemetry",type=Path,action="append",default=[])
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    raw=args.root/"raw"; dask=raw/"dask"; ray=raw/"results"/"ray"
    titan_summary=load(args.titan_summary)
    durations={"Titan":titan_duration(titan_summary),"Dask":peer_duration(dask/"duration-external.json"),"Ray":peer_duration(ray/"duration-external.json")}
    native={"Dask":peer_duration(dask/"duration-native.json"),"Ray":peer_duration(ray/"duration-native.json")}
    patterns={"Titan":titan_patterns(titan_summary),"Dask":peer_patterns(dask/"patterns"),"Ray":peer_patterns(ray/"patterns")}
    wf={"Titan":load(args.titan_wfbench),
        "Dask":load(dask/"wfbench-montage-128.json")["workflow"],
        "Ray":load(ray/"wfbench-montage-128.json")["workflow"]}
    faults={"Titan":{"worker":load(args.titan_worker_fault),"head":load(args.titan_head_fault)},
            "Dask":{"worker":load(dask/"faults"/"worker-loss-valid.json"),"head":load(dask/"faults"/"head-loss.json")},
            "Ray":{"worker":load(ray/"faults"/"worker-loss.json"),"head":{"completed":False,"error":"GCS unavailable; client terminated after 60-second reconnect deadline","elapsed_seconds":60.0}}}
    samplers=[row for path in args.utilization for row in parse_sampler(path)]
    peer_docs={runtime:[load(base/"duration-external.json"),load(base/"wfbench-montage-128.json")]+[load(path) for path in (base/"patterns").glob("*.json")]
               for runtime,base in (("Dask",dask),("Ray",ray))}
    resources={runtime:resource_summary(samplers,measured_intervals_peer(docs)) for runtime,docs in peer_docs.items()}
    resources["Titan"]=resource_summary(samplers,measured_intervals_titan(titan_summary,wf["Titan"],args.titan_telemetry))
    h=html.escape
    duration_rows="".join(f'<tr><td>{row["duration_ms"]:g} ms</td>'+"".join(f'<td>{durations[r][i]["throughput"]:.2f}</td><td>{durations[r][i]["queue_p95_ms"]:.1f}</td><td>{durations[r][i]["queue_p99_ms"]:.1f}</td>' for r in ("Titan","Dask","Ray"))+"</tr>" for i,row in enumerate(durations["Titan"]))
    wf_rows="".join(f'<tr><td>{runtime}</td><td>{data["wall_seconds"]["median"]:.3f}</td><td>{data["wall_seconds"]["min"]:.3f}–{data["wall_seconds"]["max"]:.3f}</td><td>{data["wall_seconds"]["coefficient_of_variation"]:.1%}</td><td>{data["throughput"]["median"]:.3f}</td><td>{data.get("queue_ms",{}).get("p95") if data.get("queue_ms") else "—"}</td></tr>' for runtime,data in wf.items())
    pattern_names = [name for name in ("bag", "chain", "diamond", "fanin", "fanout", "layered", "stencil")
                     if all(name in runtime_patterns for runtime_patterns in patterns.values())]
    pattern_rows="".join(f'<tr><td>{name}</td>'+"".join(f'<td>{patterns[r][name]["wall"]["median"]:.3f}</td><td>{patterns[r][name]["throughput"]["median"]:.2f}</td>' for r in ("Titan","Dask","Ray"))+"</tr>" for name in pattern_names)
    fault_rows="".join(f'<tr><td>{runtime}</td><td class="{("pass" if pair["worker"].get("completed") else "fail")}">{"Recovered" if pair["worker"].get("completed") else "Did not recover"}</td><td>{pair["worker"].get("elapsed_seconds",0):.2f}s</td><td class="{("pass" if pair["head"].get("completed") else "finding")}">{"Recovered after restart" if pair["head"].get("completed") else "Unavailable / no automatic recovery"}</td><td>{h(str(pair["head"].get("error") or ""))}</td></tr>' for runtime,pair in faults.items())
    resource_rows="".join(f'<tr><td>{runtime}</td><td>{value["samples"]}</td><td>{fmt(value["cpu_mean_pct"])}</td><td>{fmt(value["cpu_p95_pct"])}</td><td>{fmt(value["memory_peak_gib"])}</td><td>{fmt(value["network_mib"])}</td></tr>' for runtime,value in resources.items())
    document=f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Titan, Dask and Ray benchmark</title><style>
:root{{--bg:#071013;--panel:#0f1b20;--line:#29404a;--text:#ecf5f7;--muted:#98adb4;--green:#58e0a1;--amber:#efb54a;--red:#ff7389;--blue:#68a7ff}}*{{box-sizing:border-box}}html{{scroll-behavior:smooth}}body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.55 Inter,Segoe UI,Arial,sans-serif}}nav{{position:sticky;top:0;z-index:5;display:flex;gap:8px;overflow:auto;padding:10px 20px;background:#081217ee;border-bottom:1px solid var(--line)}}nav a{{color:var(--muted);text-decoration:none;padding:7px 10px;border-radius:7px;white-space:nowrap}}nav a:hover{{background:var(--panel);color:var(--text)}}main{{max-width:1220px;margin:auto;padding:30px 24px 80px}}h1{{font-size:34px;margin:.2rem 0}}h2{{font-size:22px}}h3{{font-size:16px}}p,li{{max-width:940px}}.eyebrow{{color:var(--green);font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase}}.lede,.muted{{color:var(--muted)}}section,.card{{background:var(--panel);border:1px solid var(--line);border-radius:11px}}section{{padding:22px;margin:18px 0;scroll-margin-top:60px}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;margin:20px 0}}.card{{padding:16px}}.card b{{display:block;font-size:24px;margin-top:4px}}.table{{overflow:auto}}table{{border-collapse:collapse;width:100%;min-width:850px}}th,td{{padding:9px 11px;text-align:left;border-bottom:1px solid var(--line);white-space:nowrap}}th{{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.06em}}.chart{{width:100%;max-height:390px;background:#09151a;border:1px solid var(--line);border-radius:9px}}.grid line{{stroke:#20333b}}.grid text,.axis text,.legend text{{fill:var(--muted);font-size:11px}}.pass{{color:var(--green)}}.finding{{color:var(--amber)}}.fail{{color:var(--red)}}details{{border-top:1px solid var(--line);padding-top:12px;margin-top:12px}}summary{{cursor:pointer;font-weight:700}}code{{color:#b9e5ff}}a{{color:#8bc4ff}}</style></head><body><nav><a href="#summary">Summary</a><a href="#duration">Task duration</a><a href="#wfbench">WfBench</a><a href="#patterns">Graph patterns</a><a href="#recovery">Recovery</a><a href="#resources">Resources</a><a href="#dashboards">Dashboards</a><a href="#method">Method</a></nav><main>
<div class="eyebrow">Three-VM GCP · matched runtime comparison</div><h1>Titan, Dask and Ray</h1><p class="lede">The same two worker VMs, eight logical slots, external-process payloads, graph inputs, warm-up policy and five measured passes. Native Dask and Ray functions are retained as a best-case reference, while external-process mode is the equal-work comparison with Titan.</p>
<section id="summary"><h2>Executive summary</h2><div class="cards"><div class="card">Correctness<b>{titan_summary["validity"]["valid_cases"]}/{titan_summary["validity"]["total_cases"]}</b><span class="muted">valid measured Titan cases</span></div><div class="card">Executable WfBench<b>{wf["Titan"]["wall_seconds"]["median"]:.2f}s</b><span class="muted">Titan median, 128 tasks</span></div><div class="card">Worker loss<b>{"recovered" if faults["Titan"]["worker"].get("completed") else "finding"}</b><span class="muted">12 active tasks, one worker terminated</span></div><div class="card">Process model<b>equal work</b><span class="muted">all headline comparisons launch external scripts</span></div></div><ul><li>Titan is evaluated as a durable workflow and agent runtime; Dask and Ray provide scheduler-overhead and distributed-compute references.</li><li>Native function-task results are shown separately and are not presented as execution-model-equivalent.</li><li>All measured passes are retained. No result is removed as an outlier.</li></ul></section>
<section id="duration"><h2>Task-duration sweep</h2><p>Thirty-two independent tasks per case. One warm-up is excluded; five measured passes contribute medians, ranges, CV and bootstrap confidence intervals.</p>{svg_lines(durations,"throughput","External-process throughput","tasks / second")}{svg_lines(durations,"queue_p95_ms","External-process queue latency","queue p95 · ms")}<div class="table"><table><thead><tr><th rowspan="2">Duration</th><th colspan="3">Titan</th><th colspan="3">Dask</th><th colspan="3">Ray</th></tr><tr><th>tasks/s</th><th>q95</th><th>q99</th><th>tasks/s</th><th>q95</th><th>q99</th><th>tasks/s</th><th>q95</th><th>q99</th></tr></thead><tbody>{duration_rows}</tbody></table></div><details><summary>Native function reference</summary><p>Dask and Ray native tasks omit per-task Python process launch. They establish runtime best-case overhead and should not be read as an equal execution model.</p>{svg_lines(native,"throughput","Native function throughput","tasks / second")}</details></section>
<section id="wfbench"><h2>Executable WfBench · Montage 128</h2><p>The official graph and command profiles are translated into each runtime. This tests executable CPU, memory and I/O payloads while preserving dependencies.</p><div class="table"><table><thead><tr><th>Runtime</th><th>Median wall</th><th>Range</th><th>CV</th><th>Median tasks/s</th><th>Queue p95 ms</th></tr></thead><tbody>{wf_rows}</tbody></table></div></section>
<section id="patterns"><h2>Graph-pattern sensitivity</h2><p>Seven 256-task shapes, 100 ms external processes, one warm-up and five measured passes.</p>{svg_grouped(patterns)}<div class="table"><table><thead><tr><th rowspan="2">Pattern</th><th colspan="2">Titan</th><th colspan="2">Dask</th><th colspan="2">Ray</th></tr><tr><th>wall s</th><th>tasks/s</th><th>wall s</th><th>tasks/s</th><th>wall s</th><th>tasks/s</th></tr></thead><tbody>{pattern_rows}</tbody></table></div></section>
<section id="recovery"><h2>Matched recovery behavior</h2><p>Twelve 30-second tasks are submitted, the target component is terminated after confirmed admission, and one task retry is allowed. Control-plane semantics are reported explicitly.</p><div class="table"><table><thead><tr><th>Runtime</th><th>Worker loss</th><th>Elapsed</th><th>Control-plane loss</th><th>Evidence</th></tr></thead><tbody>{fault_rows}</tbody></table></div><details><summary>Interpretation</summary><ul><li>Worker recovery measures re-execution of active work on the surviving VM.</li><li>Dask and Ray head-loss cases use their default non-HA deployments.</li><li>Titan additionally restarts its persisted master and checks convergence; this is restart recovery, not active-active high availability.</li></ul></details></section>
<section id="resources"><h2>CPU, memory and network</h2><p>One-second Linux host samples are selected only when measured runtime tasks are executing. Values aggregate the same three VMs.</p><div class="table"><table><thead><tr><th>Runtime</th><th>Samples</th><th>Mean CPU %</th><th>CPU p95 %</th><th>Peak host memory GiB</th><th>Network MiB</th></tr></thead><tbody>{resource_rows}</tbody></table></div></section>
<section id="dashboards"><h2>Operational visibility</h2><div class="table"><table><thead><tr><th>View</th><th>Titan</th><th>Ray</th><th>Dask</th></tr></thead><tbody><tr><td>Persisted DAG/run history</td><td class="pass">Built in</td><td>Job/task oriented</td><td>Live session</td></tr><tr><td>Blocked vs ready dependencies</td><td class="pass">Workflow lanes</td><td>Task state</td><td>Task stream</td></tr><tr><td>Agent stages → DAG drill-down</td><td class="pass">Built in</td><td>Application defined</td><td>Application defined</td></tr><tr><td>Node/actor/object-store diagnostics</td><td>Basic</td><td class="pass">Strong</td><td>Worker/task diagnostics</td></tr><tr><td>Recovery/lifecycle evidence</td><td class="pass">Run and service history</td><td>Logs and state APIs</td><td>Live logs/events</td></tr></tbody></table></div></section>
<section id="method"><h2>Method and limits</h2><details open><summary>Controlled configuration</summary><ul><li>GCP: one e2-medium master/store VM and two e2-medium worker VMs; each VM has 2 vCPU and 4 GiB class memory.</li><li>Both worker VMs expose four logical slots/CPUs: eight total.</li><li>All systems use the same VMs and input files. Runtime phases do not overlap.</li><li>One warm-up precedes five measured passes. {h(OUTLIER_POLICY)}</li></ul></details><details><summary>Fairness boundary</summary><p>Titan always executes a new external process. Headline Dask and Ray results therefore use <code>subprocess.run</code> per task. Their native function-task variants are labelled separately. The adapter preserves graph dependencies and payloads; it is not the original workflow runtime implementation.</p></details><details><summary>Known boundaries</summary><ul><li>Logical slots exceed physical vCPUs on each worker. The configuration matches the requested Titan deployment and is identical across systems.</li><li>Worker clocks are NTP synchronized; small negative cross-host queue estimates are clamped to zero.</li><li>Control-plane recovery reflects default deployments, not configured HA clusters.</li></ul></details></section>
</main></body></html>'''
    args.output.parent.mkdir(parents=True,exist_ok=True); args.output.write_text(document,encoding="utf-8")
    evidence={"schema":"titan-runtime-comparison-v1","durations":durations,"native":native,"patterns":patterns,"wfbench":wf,"faults":faults,"resources":resources,"outlier_policy":OUTLIER_POLICY}
    args.output.with_suffix(".json").write_text(json.dumps(evidence,indent=2),encoding="utf-8")


if __name__=="__main__": main()
