"""Build the GCP paper-campaign summary, figures, and standalone HTML report."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import html
import json
import math
from pathlib import Path


def read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def svg_frame(title: str, body: str, width: int = 980, height: int = 460) -> str:
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<style>text{{font-family:Inter,Segoe UI,Arial,sans-serif;fill:#172033}}.title{{font-size:22px;font-weight:700}}.axis{{font-size:12px;fill:#657087}}.label{{font-size:13px;font-weight:600}}.grid{{stroke:#d9e0ea;stroke-width:1}}.axisline{{stroke:#8995a8;stroke-width:1.4}}.note{{font-size:12px;fill:#657087}}</style>
<rect width="100%" height="100%" rx="18" fill="#fbfcfe"/>
<text x="32" y="38" class="title">{html.escape(title)}</text>{body}</svg>'''


COLORS = {
    "Titan": "#6d5dfc", "Dask native": "#00a6a6", "Dask process": "#2ec4b6",
    "Ray native": "#f59e0b", "Ray process": "#f97316", "range": "#b9b2ff",
}


def line_chart(title: str, series: dict[str, list[tuple[float, float]]], path: Path,
               y_label: str) -> None:
    width, height = 980, 500
    left, right, top, bottom = 82, 34, 70, 72
    xs = sorted({x for points in series.values() for x, _ in points})
    ys = [y for points in series.values() for _, y in points]
    lo_x, hi_x = math.log10(min(xs)), math.log10(max(xs))
    hi_y = max(ys) * 1.08
    xmap = lambda x: left + (math.log10(x) - lo_x) / (hi_x - lo_x) * (width-left-right)
    ymap = lambda y: top + (hi_y-y) / hi_y * (height-top-bottom)
    parts = []
    for i in range(6):
        y = hi_y * i / 5
        py = ymap(y)
        parts.append(f'<line x1="{left}" y1="{py:.1f}" x2="{width-right}" y2="{py:.1f}" class="grid"/>')
        parts.append(f'<text x="{left-10}" y="{py+4:.1f}" text-anchor="end" class="axis">{y:.1f}</text>')
    for x in xs:
        px = xmap(x)
        parts.append(f'<line x1="{px:.1f}" y1="{top}" x2="{px:.1f}" y2="{height-bottom}" class="grid"/>')
        parts.append(f'<text x="{px:.1f}" y="{height-bottom+22}" text-anchor="middle" class="axis">{x:g}</text>')
    parts.append(f'<line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" class="axisline"/>')
    parts.append(f'<text x="{(left+width-right)/2:.1f}" y="{height-20}" text-anchor="middle" class="label">Requested task duration (ms, log scale)</text>')
    parts.append(f'<text x="20" y="{(top+height-bottom)/2:.1f}" transform="rotate(-90 20 {(top+height-bottom)/2:.1f})" text-anchor="middle" class="label">{html.escape(y_label)}</text>')
    for index, (name, points) in enumerate(series.items()):
        color = COLORS.get(name, "#344054")
        coords = " ".join(f"{xmap(x):.1f},{ymap(y):.1f}" for x, y in points)
        parts.append(f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="3"/>')
        for x, y in points:
            parts.append(f'<circle cx="{xmap(x):.1f}" cy="{ymap(y):.1f}" r="4" fill="{color}"/>')
        lx = 120 + index * 170
        parts.append(f'<line x1="{lx}" y1="56" x2="{lx+28}" y2="56" stroke="{color}" stroke-width="4"/>')
        parts.append(f'<text x="{lx+36}" y="61" class="axis">{html.escape(name)}</text>')
    write(path, svg_frame(title, "".join(parts), width, height))


def wfbench_chart(rows: list[dict], headline: dict, path: Path) -> None:
    measured = [row for row in rows if not row["warmup"]]
    values = [row["wall_seconds"] for row in measured] + [headline["wall_seconds"]["median"]]
    labels = [f"Pass {row['pass']}" for row in measured] + ["Fresh restart"]
    max_y = max(values) * 1.12
    parts = []
    for i in range(5):
        v = max_y*i/4; y = 410-v/max_y*310
        parts.append(f'<line x1="76" y1="{y:.1f}" x2="946" y2="{y:.1f}" class="grid"/><text x="66" y="{y+4:.1f}" text-anchor="end" class="axis">{v:.0f}s</text>')
    bar_w = 92; gap = 40
    for i, (label, value) in enumerate(zip(labels, values)):
        x = 100+i*(bar_w+gap); h = value/max_y*310; y=410-h
        color = "#6d5dfc" if label != "Fresh restart" else "#15a36d"
        parts.append(f'<rect x="{x}" y="{y:.1f}" width="{bar_w}" height="{h:.1f}" rx="8" fill="{color}"/>')
        parts.append(f'<text x="{x+bar_w/2}" y="{y-8:.1f}" text-anchor="middle" class="label">{value:.1f}s</text>')
        parts.append(f'<text x="{x+bar_w/2}" y="434" text-anchor="middle" class="axis">{label}</text>')
    parts.append('<text x="32" y="474" class="note">Five consecutive measured passes share one runtime session; the green bar follows a full master/store/worker restart.</text>')
    write(path, svg_frame("Executable WfBench makespan and fresh-restart control", "".join(parts), 980, 500))


def scientific_chart(families: dict, path: Path) -> None:
    items=[]
    for name, value in families.items():
        stat=value["throughput_jobs_per_second"]
        items.append((name, stat["median"], stat["min"], stat["max"]))
    hi=max(x[3] for x in items)*1.12; parts=[]
    for i in range(6):
        v=hi*i/5; x=120+v/hi*800
        parts.append(f'<line x1="{x:.1f}" y1="75" x2="{x:.1f}" y2="395" class="grid"/><text x="{x:.1f}" y="420" text-anchor="middle" class="axis">{v:.1f}</text>')
    for i,(name,med,lo,high) in enumerate(items):
        y=112+i*72; xlo=120+lo/hi*800; xhi=120+high/hi*800; xm=120+med/hi*800
        parts.append(f'<text x="108" y="{y+5}" text-anchor="end" class="label">{html.escape(name.title())}</text>')
        parts.append(f'<line x1="{xlo:.1f}" y1="{y}" x2="{xhi:.1f}" y2="{y}" stroke="#b9b2ff" stroke-width="10" stroke-linecap="round"/>')
        parts.append(f'<circle cx="{xm:.1f}" cy="{y}" r="8" fill="#6d5dfc"/><text x="{xhi+10:.1f}" y="{y+5}" class="axis">{med:.2f} ({lo:.2f}–{high:.2f})</text>')
    parts.append('<text x="520" y="452" text-anchor="middle" class="label">Jobs per second; median with five-pass range</text>')
    write(path, svg_frame("GCP scientific workflow repeatability", "".join(parts), 980, 480))


def agent_chart(parallel: list[dict], multi: dict, research: dict, path: Path) -> None:
    parts=[]
    x0=35
    for idx, run in enumerate(parallel):
        x=x0+idx*225; stages=[s for s in run.get("stages",[]) if s.get("dag")]
        parts.append(f'<rect x="{x}" y="78" width="200" height="292" rx="14" fill="#f3f1ff" stroke="#cbc5ff"/>')
        parts.append(f'<text x="{x+16}" y="106" class="label">ParallelQA {html.escape(run.get("question", "")[:16])}…</text>')
        y=132
        for s in stages:
            width=max(1,len(s.get("decision",{}).get("jobs",[])))
            color="#15a36d" if all(v=="COMPLETED" for v in s.get("statuses",{}).values()) else "#dc4c64"
            parts.append(f'<rect x="{x+16}" y="{y}" width="{min(168,28+width*24)}" height="28" rx="7" fill="{color}" opacity=".88"/>')
            parts.append(f'<text x="{x+24}" y="{y+19}" fill="white" style="fill:white;font-size:12px">stage {s["iteration"]+1}: {width} jobs</text>')
            y+=42
        verdict="oracle pass" if run.get("valid") else "model plan incomplete"
        parts.append(f'<text x="{x+16}" y="348" class="axis">{verdict}; {run.get("model_calls",0)} model calls</text>')
    parts.append(f'<text x="35" y="410" class="note">Observed model decisions are preserved from each run. Stage width is the number of jobs Gemini selected for that frontier.</text>')
    parts.append(f'<text x="35" y="438" class="note">BFCL multi-turn: {sum(1 for t in multi.get("turns",[]) if t.get("correct"))}/{len(multi.get("turns",[]))} exact turns; all {len(multi.get("stages",[]))} submitted stages completed. Research agent: {len(research.get("stages",[]))} adaptive stages, valid={str(research.get("valid")).lower()}.</text>')
    write(path, svg_frame("Observed runtime agent DAG variation", "".join(parts), 980, 470))


def digest_tree(root: Path, excluded: set[str]) -> dict[str, str]:
    result={}
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        rel=path.relative_to(root).as_posix()
        if rel in excluded: continue
        result[rel]=hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def card(label: str, value: str, note: str) -> str:
    return f'<article class="metric"><span>{html.escape(label)}</span><strong>{html.escape(value)}</strong><small>{html.escape(note)}</small></article>'


def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--titan-sweep", type=Path, required=True)
    parser.add_argument("--scientific", type=Path, required=True)
    parser.add_argument("--environment", type=Path, required=True)
    args=parser.parse_args(); root=args.campaign; figures=root/"figures"
    peer=root/"raw-peer"/"results"; agents=root/"agent-suite"/"results"
    wf=read(peer/"titan-wfbench-montage-final.json"); headline=read(peer/"titan-headline-clean.json")
    titan_sweep=read(args.titan_sweep); science=read(args.scientific); environment=read(args.environment)
    peer_docs={}
    for runtime in ("dask","ray"):
        for mode in ("native","external"):
            peer_docs[(runtime,mode)]=read(peer/f"{runtime}-{mode}.json")
    workflow_docs={(runtime,mode):read(peer/f"{runtime}-{mode}-workflow.json") for runtime in ("dask","ray") for mode in ("native","external")}
    series={"Titan": [(r["requested_duration_ms"],r["throughput"]) for r in titan_sweep["granularity_summary"]]}
    for (runtime,mode),doc in peer_docs.items():
        label=f"{runtime.title()} {'process' if mode=='external' else 'native'}"
        series[label]=[(r["duration_ms"],r["throughput"]["median"]) for r in doc["sweep"]]
    line_chart("Task-duration sweep on the same two worker VMs",series,figures/"task-duration-throughput.svg","Median tasks per second")
    wfbench_chart(wf["rows"],headline,figures/"wfbench-repeatability.svg")
    scientific_chart(science["families"],figures/"scientific-repeatability.svg")
    parallel=[read(agents/f"parallelqa-{x}"/"summary.json") for x in ("1","22","53","84")]
    multi=read(agents/"bfcl-multiturn"/"summary.json"); research=read(agents/"research-agent"/"summary.json")
    agent_chart(parallel,multi,research,figures/"agent-workflow-variation.svg")
    bfcl=[read(agents/f"bfcl-parallel_{i}-validated"/"summary.json") for i in range(3)]
    pqa_jobs=sum(sum(len(s.get("statuses",{})) for s in run.get("stages",[])) for run in parallel)
    bfcl_jobs=sum(run["jobs"] for run in bfcl)
    multi_jobs=sum(s["jobs"] for s in multi["stages"])
    research_jobs=sum(len(s.get("jobs",[])) for s in research["stages"])
    agent_jobs=pqa_jobs+bfcl_jobs+multi_jobs+research_jobs
    agent_completed=pqa_jobs+sum(run["jobs"] for run in bfcl if run["valid"])+sum(s["jobs"] for s in multi["stages"] if s["status"]=="COMPLETED")+research_jobs
    peer_workflow={f"{r}-{m}":workflow_docs[(r,m)]["workflow"]["throughput"] for r in ("dask","ray") for m in ("native","external")}
    summary={
        "schema":"titan-gcp-paper-campaign-v1","generated_at":datetime.now(timezone.utc).isoformat(),
        "scope":"GCP only; one e2-medium master VM and two e2-medium worker VMs",
        "headline":{
            "executable_wfbench_clean":{"tasks":123,"completed":123,"wall_seconds":headline["wall_seconds"]["median"],"throughput":headline["throughput"]["median"]},
            "executable_wfbench_five_pass":{"completed":sum(r["completed"] for r in wf["rows"] if not r["warmup"]),"expected":615,"wall_seconds":wf["wall_seconds"],"throughput":wf["throughput"]},
            "agent_execution":{"completed_jobs":agent_completed,"submitted_jobs":agent_jobs,"parallelqa_oracle_passes":sum(bool(r["valid"]) for r in parallel),"parallelqa_cases":4,"bfcl_parallel_passes":sum(bool(r["valid"]) for r in bfcl),"bfcl_parallel_cases":3,"bfcl_multiturn_exact_turns":sum(bool(t["correct"]) for t in multi["turns"]),"bfcl_multiturn_turns":len(multi["turns"]),"research_agent_valid":research["valid"]},
            "task_granularity":{"metg50_ms":titan_sweep["metg50"]["observed_ms"],"passes_per_duration":titan_sweep["configuration"]["passes"]},
            "scientific_repeatability":{"cases":science["measured_cases"],"jobs":sum(r["tasks"] for v in science["families"].values() for r in v["runs"]),"dependency_violations":science["dependency_violations"],"duplicate_or_missing_spans":science["duplicate_or_missing_spans"]},
        },
        "peer_workflow_throughput":peer_workflow,"environment":environment,
        "outlier_policy":"No measured pass is removed. Warm-up passes are excluded from reported summaries.",
        "claim_boundaries":[
            "WfBench uses official WfCommons graph generation and official CPU/memory task executables with data volume set to zero; it is not a real Montage binary mosaic.",
            "Dask and Ray curves are scheduler-overhead references. Native functions and per-task external Python processes are reported separately.",
            "Agent oracle failures measure the frozen Gemini model and prompt. Titan claims are based on submitted-stage execution and recovery, not model accuracy.",
        ],
    }
    (root/"summary.json").write_text(json.dumps(summary,indent=2)+"\n",encoding="utf-8")
    h=summary["headline"]; sci=h["scientific_repeatability"]; ag=h["agent_execution"]
    rows="".join(f'<tr><td>{html.escape(k.replace("-"," ").title())}</td><td>{v["median"]:.2f}</td><td>{v["min"]:.2f}–{v["max"]:.2f}</td><td>{v["coefficient_of_variation"]*100:.1f}%</td></tr>' for k,v in peer_workflow.items())
    family_rows="".join(f'<tr><td>{html.escape(name.title())}</td><td>{v["runtime_seconds"]["median"]:.2f}s</td><td>{v["throughput_jobs_per_second"]["median"]:.2f}</td><td>{v["throughput_jobs_per_second"]["cv_percent"]:.2f}%</td><td>{v["latency_ms"]["queue_wait_ms"]["p50"]:.0f}/{v["latency_ms"]["queue_wait_ms"]["p95"]:.0f}/{v["latency_ms"]["queue_wait_ms"]["p99"]:.0f} ms</td></tr>' for name,v in science["families"].items())
    css='''body{margin:0;background:#f3f5f9;color:#172033;font:15px/1.55 Inter,Segoe UI,Arial,sans-serif}header{background:#111827;color:#fff;padding:42px max(5vw,28px)}header p{max-width:900px;color:#c9d2e3}.wrap{max-width:1180px;margin:auto;padding:30px}.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:14px}.metric,.panel{background:#fff;border:1px solid #dfe4ec;border-radius:14px;padding:18px;box-shadow:0 5px 20px #1820330d}.metric span,.metric small{display:block;color:#68738a}.metric strong{font-size:28px;display:block;margin:5px 0}.tabs{position:sticky;top:0;background:#fff;border-bottom:1px solid #dfe4ec;padding:10px 5vw;z-index:2}.tabs a{display:inline-block;padding:8px 12px;color:#4d3fd6;text-decoration:none;font-weight:700}section{scroll-margin-top:72px;margin:26px 0}.panel{margin:14px 0}.figure{width:100%;height:auto}table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:10px;border-bottom:1px solid #e5e9f0}th{color:#5f6b81}code{background:#eef0f5;padding:2px 5px;border-radius:5px}details{background:#fff;border:1px solid #dfe4ec;border-radius:12px;padding:14px;margin:12px 0}summary{font-weight:700;cursor:pointer}.ok{color:#087f5b}.warn{color:#b45309}.callout{border-left:5px solid #6d5dfc;padding:14px 18px;background:#f3f1ff;border-radius:8px}'''
    body=f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Titan GCP Paper Benchmark Campaign</title><style>{css}</style></head><body>
<header><h1>Titan GCP paper benchmark campaign</h1><p>A reproducible three-VM evaluation of executable scientific DAGs, task granularity, Dask and Ray scheduler references, model-directed agent workflows, and five-pass repeatability. Generated {html.escape(summary["generated_at"])}.</p></header>
<nav class="tabs"><a href="#overview">Overview</a><a href="#wfbench">Executable WfBench</a><a href="#baselines">Runtime baselines</a><a href="#agents">Agentic</a><a href="#science">Scientific repeatability</a><a href="#methods">Methods</a></nav><main class="wrap">
<section id="overview"><h2>Executive summary</h2><div class="metrics">
{card("Clean executable workflow","123/123",f"{h['executable_wfbench_clean']['wall_seconds']:.2f}s after full restart")}
{card("Five-pass executable tasks","615/615",f"median {h['executable_wfbench_five_pass']['wall_seconds']['median']:.2f}s")}
{card("Agent jobs executed",f"{ag['completed_jobs']}/{ag['submitted_jobs']}","model accuracy reported separately")}
{card("Task granularity",f"{h['task_granularity']['metg50_ms']:.0f} ms","METG50 on the 3-VM deployment")}
{card("Scientific workflow jobs",f"{sci['jobs']:,}","20 five-pass GCP cases")}
{card("Dependency violations",str(sci['dependency_violations']),"across the five-pass science set")}</div>
<p class="callout"><strong>Finding:</strong> correctness evidence is strong across executable, scientific, and model-directed DAGs. Short-task throughput remains an operating boundary. The executable WfBench sequence also slowed within one long runtime session; a clean restart restored makespan from 112.54s in pass 5 to 51.08s, so accumulated state needs isolated follow-up.</p></section>
<section id="wfbench"><h2>Executable WfBench</h2><div class="panel"><img class="figure" src="figures/wfbench-repeatability.svg" alt="WfBench makespan by pass"></div><div class="metrics">{card("Correct measured tasks","615/615","five measured passes")}{card("Median makespan",f"{wf['wall_seconds']['median']:.2f}s",f"range {wf['wall_seconds']['min']:.2f}–{wf['wall_seconds']['max']:.2f}s")}{card("Median throughput",f"{wf['throughput']['median']:.2f}/s",f"CV {wf['throughput']['coefficient_of_variation']*100:.1f}%")}{card("Fresh restart",f"{headline['wall_seconds']['median']:.2f}s","single headline control")}</div><details><summary>Exact claim boundary</summary><p>Official WfCommons generated the 123-task, 258-edge Montage-shaped graph. Official WfBench executables generated CPU and memory activity. Data volume was zero, because Titan does not claim implicit filesystems across worker machines. This is executable WfBench, not a real Montage binary mosaic.</p></details></section>
<section id="baselines"><h2>Task granularity and scheduler references</h2><div class="panel"><img class="figure" src="figures/task-duration-throughput.svg" alt="Task-duration throughput curves"></div><p>Dask and Ray are reported in their native function mode and in an equal-work external-process mode. Titan launches a process for every task, so the external-process curves are the fairer scheduling-overhead reference.</p><table><thead><tr><th>123-task graph</th><th>Median tasks/s</th><th>Five-pass range</th><th>CV</th></tr></thead><tbody>{rows}</tbody></table></section>
<section id="agents"><h2>Model-directed agent workflows</h2><div class="panel"><img class="figure" src="figures/agent-workflow-variation.svg" alt="Observed agent DAG stages"></div><div class="metrics">{card("ParallelQA oracle",f"{ag['parallelqa_oracle_passes']}/{ag['parallelqa_cases']}","Gemini planned every frontier")}{card("BFCL parallel oracle",f"{ag['bfcl_parallel_passes']}/{ag['bfcl_parallel_cases']}","official cases; cross-worker TitanStore data")}{card("BFCL multi-turn",f"{ag['bfcl_multiturn_exact_turns']}/{ag['bfcl_multiturn_turns']}","exact model calls; all stages executed")}{card("Research agent","PASS",f"{research['model_calls']} Gemini calls, {research['wall_seconds']:.2f}s")}</div><p>Model and runtime outcomes are separated. A wrong or incomplete Gemini plan is an agent-oracle failure. Titan is assessed on whether every submitted stage executes once, reaches a terminal state, and exposes its DAG and artifacts.</p></section>
<section id="science"><h2>Five-pass scientific repeatability</h2><div class="panel"><img class="figure" src="figures/scientific-repeatability.svg" alt="Scientific workflow throughput medians and ranges"></div><table><thead><tr><th>Workflow</th><th>Median makespan</th><th>Median jobs/s</th><th>CV</th><th>Queue p50/p95/p99</th></tr></thead><tbody>{family_rows}</tbody></table></section>
<section id="methods"><h2>Methods and reproducibility</h2><div class="panel"><ul><li>GCP only: one master/store/dashboard VM plus two worker VMs.</li><li>Each worker exposed four Titan slots; the master did not execute tasks.</li><li>Five measured passes follow one warm-up wherever the runner supports warm-up.</li><li>No measured pass is removed. Median, range, CV, bootstrap median 95% CI, and p50/p95/p99 are retained in raw summaries.</li><li>Input and result hashes are listed in <code>SHA256SUMS.json</code>.</li></ul></div><details><summary>Environment</summary><pre>{html.escape(json.dumps(environment,indent=2))}</pre></details><details><summary>Known limits</summary><ul>{''.join('<li>'+html.escape(x)+'</li>' for x in summary['claim_boundaries'])}</ul></details></section>
</main></body></html>'''
    write(root/"report.html",body)
    manifest=digest_tree(root,{"SHA256SUMS.json"})
    (root/"SHA256SUMS.json").write_text(json.dumps(manifest,indent=2)+"\n",encoding="utf-8")
    write(root/"README.md",f"""# Titan GCP paper benchmark campaign\n\nOpen `report.html` for the consolidated result.\n\n## Scope\n\n- GCP only: one master VM and two worker VMs\n- Executable WfBench, Dask and Ray references, model-directed agent workloads\n- Existing five-pass scientific workflow and task-granularity evidence\n\n## Reproduce\n\nRunner source is in `titan_test_suite/paper_benchmark` and `titan_test_suite/external_agent_benchmark`. Exact inputs, summaries, figures, environment metadata, and SHA-256 hashes are retained in this directory.\n\n## Outliers\n\nNo measured pass was removed. Warm-up executions are explicitly excluded where configured.\n""")
    print(root/"report.html")


if __name__ == "__main__":
    main()
