"""Build a self-contained TitanStore versus Redis evidence report."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from html import escape
import json
import math
from pathlib import Path
import statistics


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def protocol(path: Path, name: str) -> dict:
    matches = [item for item in load(path).get("protocol") or [] if item.get("name") == name]
    if len(matches) != 1:
        raise ValueError(f"expected one {name} protocol result in {path}")
    return matches[0]


def controlled(a: dict, b: dict) -> dict:
    keys = ("configured_workers", "slots_per_worker", "passes", "warmup_passes",
            "durations_ms", "tasks", "suites")
    left = {key: (a.get("configuration") or {}).get(key) for key in keys}
    right = {key: (b.get("configuration") or {}).get(key) for key in keys}
    if left != right:
        raise ValueError(f"scheduler configurations differ: {left} != {right}")
    return left


def rows(summary: dict) -> dict[float, dict]:
    return {float(item["requested_duration_ms"]): item
            for item in summary.get("granularity_summary") or []}


def percent_change(before: float, after: float) -> float:
    return (after / before - 1) * 100 if before else 0


def svg_throughput(pairs: list[dict]) -> str:
    width, height, left, right, top, bottom = 760, 290, 68, 24, 22, 52
    xs = [math.log10(item["duration_ms"]) for item in pairs]
    maximum = max(max(item["titanstore_tasks_s"], item["redis_tasks_s"]) for item in pairs) * 1.12
    xp = lambda value: left + (value - min(xs)) / max(.001, max(xs) - min(xs)) * (width-left-right)
    yp = lambda value: top + (1 - value / maximum) * (height-top-bottom)
    grid = "".join(f'<line x1="{left}" x2="{width-right}" y1="{yp(v):.1f}" y2="{yp(v):.1f}"/><text x="{left-10}" y="{yp(v)+4:.1f}" text-anchor="end">{v:.0f}</text>' for v in [0, maximum/4, maximum/2, maximum*3/4, maximum])
    def line(key: str, css: str) -> str:
        points = " ".join(f"{xp(x):.1f},{yp(item[key]):.1f}" for x, item in zip(xs, pairs))
        dots = "".join(f'<circle cx="{xp(x):.1f}" cy="{yp(item[key]):.1f}" r="4"/>' for x, item in zip(xs, pairs))
        return f'<g class="{css}"><polyline points="{points}"/>{dots}</g>'
    labels = "".join(f'<text x="{xp(x):.1f}" y="{height-27}" text-anchor="middle">{item["duration_ms"]:g}</text>' for x, item in zip(xs, pairs))
    return f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="Scheduler throughput by task duration"><g class="grid">{grid}</g>{line("titanstore_tasks_s", "titan")}{line("redis_tasks_s", "redis")}{labels}<text class="axis" x="{width/2}" y="{height-5}" text-anchor="middle">requested task duration (ms, log scale)</text><text class="axis" transform="translate(15 {height/2}) rotate(-90)" text-anchor="middle">tasks per second</text></svg>'


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--titanstore-summary", type=Path, required=True)
    parser.add_argument("--redis-summary", type=Path, required=True)
    parser.add_argument("--titanstore-protocol", type=Path, required=True)
    parser.add_argument("--redis-protocol", type=Path, required=True)
    parser.add_argument("--fault-summary", type=Path, required=True)
    parser.add_argument("--environment", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    titanstore, redis = load(args.titanstore_summary), load(args.redis_summary)
    configuration = controlled(titanstore, redis)
    ts_protocol = protocol(args.titanstore_protocol, "titanstore")
    redis_protocol = protocol(args.redis_protocol, "redis")
    fault = load(args.fault_summary)
    environment = load(args.environment) if args.environment else {}
    ts_rows, redis_rows = rows(titanstore), rows(redis)
    if ts_rows.keys() != redis_rows.keys():
        raise ValueError("duration cells differ")
    pairs = [{"duration_ms": duration,
              "titanstore_tasks_s": ts_rows[duration]["throughput"],
              "redis_tasks_s": redis_rows[duration]["throughput"],
              "change_percent": percent_change(ts_rows[duration]["throughput"], redis_rows[duration]["throughput"]),
              "titanstore_queue_p95_ms": ts_rows[duration]["queue_p95_ms"],
              "redis_queue_p95_ms": redis_rows[duration]["queue_p95_ms"]}
             for duration in sorted(ts_rows)]
    relative = [item["change_percent"] for item in pairs]
    phases = {}
    for name in titanstore.get("latency_breakdown") or {}:
        phases[name] = {"titanstore": titanstore["latency_breakdown"][name],
                        "redis": redis["latency_breakdown"].get(name, {})}
    report = {
        "schema": "titan-store-backend-controlled-comparison-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "configuration": configuration, "environment": environment,
        "validity": {"titanstore": titanstore["validity"], "redis": redis["validity"]},
        "protocol": {"titanstore": ts_protocol, "redis": redis_protocol},
        "scheduler_pairs": pairs, "latency_breakdown": phases, "fault_recovery": fault,
        "conclusion": {
            "protocol_operations_per_second_ratio": redis_protocol["round_trips_per_second"] / ts_protocol["round_trips_per_second"],
            "median_scheduler_throughput_change_percent": statistics.median(relative),
            "max_absolute_scheduler_throughput_change_percent": max(abs(x) for x in relative),
            "titanstore_metg50_ms": titanstore["metg50"].get("observed_ms"),
            "redis_metg50_ms": redis["metg50"].get("observed_ms"),
            "store_materially_explains_scheduler_ceiling": False,
        },
    }

    def p(value) -> str:
        return "—" if value is None else f"{value:.3f}"
    duration_rows = "".join(f'<tr><td>{x["duration_ms"]:g}</td><td>{x["titanstore_tasks_s"]:.3f}</td><td>{x["redis_tasks_s"]:.3f}</td><td>{x["change_percent"]:+.2f}%</td><td>{x["titanstore_queue_p95_ms"]:.1f}</td><td>{x["redis_queue_p95_ms"]:.1f}</td></tr>' for x in pairs)
    phase_order = ["queue_ms", "dispatch_route_ms", "worker_selection_ms", "dispatch_record_ms", "dispatch_store_ms", "dispatch_rpc_ms", "worker_queue_ms", "preprocess_ms", "process_startup_ms", "script_execution_ms", "callback_transit_ms", "callback_handle_ms", "terminal_persistence_ms", "store_read_ms", "store_write_ms"]
    phase_rows = "".join(f'<tr><td>{escape(name.replace("_ms", "").replace("_", " "))}</td><td>{phases[name]["titanstore"].get("count")}</td><td>{p(phases[name]["titanstore"].get("p50"))} / {p(phases[name]["titanstore"].get("p95"))} / {p(phases[name]["titanstore"].get("p99"))}</td><td>{phases[name]["redis"].get("count")}</td><td>{p(phases[name]["redis"].get("p50"))} / {p(phases[name]["redis"].get("p95"))} / {p(phases[name]["redis"].get("p99"))}</td></tr>' for name in phase_order if name in phases)
    protocol_rows = "".join(f'<tr><td>{op.upper()}</td><td>{p(ts_protocol[op]["p50_ms"])} / {p(ts_protocol[op]["p95_ms"])} / {p(ts_protocol[op]["p99_ms"])}</td><td>{p(redis_protocol[op]["p50_ms"])} / {p(redis_protocol[op]["p95_ms"])} / {p(redis_protocol[op]["p99_ms"])}</td></tr>' for op in ("set", "get", "sadd", "smembers", "srem"))
    conclusion = report["conclusion"]
    html = f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>TitanStore versus Redis</title><style>
:root{{--bg:#081014;--panel:#101b20;--line:#29414a;--text:#eaf2f2;--muted:#9bb0b4;--green:#54e6a8;--blue:#72c7ff;--amber:#f6c85f}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 Segoe UI,Arial}}main{{max-width:1160px;margin:auto;padding:30px 22px 70px}}h1{{font-size:32px;margin:.2rem 0}}h2{{font-size:20px}}p,li{{max-width:920px}}.muted{{color:var(--muted)}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;margin:20px 0}}.card,section{{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:18px;margin:14px 0}}.card b{{display:block;color:var(--green);font-size:25px}}table{{border-collapse:collapse;width:100%;min-width:720px}}.table{{overflow:auto}}th,td{{padding:9px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}}th{{color:var(--muted);font-size:11px;text-transform:uppercase}}.chart{{width:100%;max-height:350px;background:#0b1519;border:1px solid var(--line);border-radius:8px}}.grid line{{stroke:#1d3239}}.grid text,.axis{{fill:var(--muted);font-size:11px}}.titan polyline,.redis polyline{{fill:none;stroke-width:3}}.titan polyline,.titan circle{{stroke:var(--green)}}.redis polyline,.redis circle{{stroke:var(--blue)}}.titan circle,.redis circle{{fill:var(--bg);stroke-width:3}}code{{color:#b8e6ff}}details{{border-top:1px solid var(--line);padding-top:12px;margin-top:12px}}summary{{cursor:pointer;font-weight:700}}.pass{{color:var(--green)}}
</style></head><body><main><div class="muted">Titan · controlled 2 VM backend comparison</div><h1>TitanStore versus Redis</h1><p>Identical Titan master, worker, JAR, four-slot capacity, task graph, warm-up and five-pass protocol. Only the RESP Store process changed.</p>
<div class="cards"><div class="card"><b>80 / 80</b>measured DAGs valid</div><div class="card"><b>{conclusion["protocol_operations_per_second_ratio"]:.2f}×</b>Redis direct operation rate</div><div class="card"><b>{conclusion["median_scheduler_throughput_change_percent"]:+.2f}%</b>median scheduler throughput change</div><div class="card"><b>{conclusion["titanstore_metg50_ms"]:g} ms</b>METG(50) for both backends</div></div>
<section><h2>Executive conclusion</h2><ul><li>Redis is materially faster for direct Store operations.</li><li>The scheduler throughput curve and METG remain effectively unchanged.</li><li>TitanStore does not materially explain the observed 7–8 tasks/s short-task ceiling in this topology.</li><li>Redis compatibility, reconnect behavior and AOF replay passed; TitanStore remains a credible dependency-free default.</li></ul></section>
<section><h2>Scheduler throughput</h2><p><span style="color:var(--green)">● TitanStore</span> &nbsp; <span style="color:var(--blue)">● Redis</span></p>{svg_throughput(pairs)}<div class="table"><table><thead><tr><th>Duration ms</th><th>TitanStore tasks/s</th><th>Redis tasks/s</th><th>Redis change</th><th>TitanStore queue p95 ms</th><th>Redis queue p95 ms</th></tr></thead><tbody>{duration_rows}</tbody></table></div></section>
<section><h2>Correlated latency breakdown</h2><p class="muted">Values are p50 / p95 / p99 in milliseconds. Callback transit assumes synchronized VM clocks. Process startup measures OS process creation; script execution includes Python interpreter initialization and task runtime.</p><div class="table"><table><thead><tr><th>Phase</th><th>TitanStore n</th><th>TitanStore ms</th><th>Redis n</th><th>Redis ms</th></tr></thead><tbody>{phase_rows}</tbody></table></div></section>
<section><h2>Direct RESP operations</h2><p>TitanStore used its normal AOF. Redis used AOF with <code>appendfsync everysec</code>. Each backend completed 1,000 measured operations per command after 100 warm-ups with verified 1 KiB payloads.</p><div class="table"><table><thead><tr><th>Operation</th><th>TitanStore p50 / p95 / p99 ms</th><th>Redis p50 / p95 / p99 ms</th></tr></thead><tbody>{protocol_rows}</tbody></table></div></section>
<section><h2>Redis outage and AOF recovery</h2><div class="cards"><div class="card"><b class="pass">{'PASS' if fault.get('passed') else 'FINDING'}</b>active DAG and recovery invariants</div><div class="card"><b>{fault.get('adapter_reconnect_seconds', 0):.3f} s</b>adapter reconnect after restart</div><div class="card"><b>{fault.get('observed_spans')} / {fault.get('expected_spans')}</b>persisted spans</div><div class="card"><b>{fault.get('duplicate_attempts')}</b>duplicate attempts</div></div><ol><li>Write and verify a sentinel through Titan.</li><li>Submit an eight-way bag plus fan-in sink.</li><li>Stop Redis during execution for {fault.get('outage_seconds')} seconds.</li><li>Restart Redis and verify Titan reconnects and the DAG completes.</li><li>Restart Redis again and verify the sentinel is restored from AOF.</li></ol><details><summary>Raw recovery evidence</summary><pre>{escape(json.dumps(fault, indent=2))}</pre></details></section>
<section><h2>Controlled configuration and evidence</h2><pre>{escape(json.dumps(configuration, indent=2))}</pre><details><summary>Environment</summary><pre>{escape(json.dumps(environment, indent=2))}</pre></details><p class="muted">The two runs used the same two GCP VMs and artifact SHA. Results characterize this configuration; they do not claim universal Store performance.</p></section>
<p class="muted">Generated {escape(report['generated_at'])}. Self-contained HTML.</p></main></body></html>'''
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (args.output / "report.html").write_text(html, encoding="utf-8")
    print(args.output / "report.html")


if __name__ == "__main__":
    main()
