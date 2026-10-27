"""Dependency-free TitanStore versus Redis benchmark and report generator."""
from __future__ import annotations

import argparse
import json
import socket
import statistics
import time
from datetime import datetime, timezone
from html import escape
from pathlib import Path


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def distribution(values: list[float]) -> dict:
    return {"count": len(values), "mean_ms": statistics.fmean(values) if values else None,
            "p50_ms": percentile(values, .50), "p95_ms": percentile(values, .95),
            "p99_ms": percentile(values, .99), "min_ms": min(values) if values else None,
            "max_ms": max(values) if values else None}


class RespClient:
    def __init__(self, host: str, port: int, timeout: float = 3.0):
        self.socket = socket.create_connection((host, port), timeout=timeout)
        self.file = self.socket.makefile("rb")

    def close(self):
        self.file.close()
        self.socket.close()

    def command(self, *parts: str):
        payload = [f"*{len(parts)}\r\n".encode()]
        for part in parts:
            encoded = part.encode()
            payload.extend((f"${len(encoded)}\r\n".encode(), encoded, b"\r\n"))
        self.socket.sendall(b"".join(payload))
        return self._response()

    def _line(self) -> bytes:
        line = self.file.readline()
        if not line.endswith(b"\r\n"):
            raise OSError("incomplete RESP line")
        return line[:-2]

    def _response(self):
        prefix = self.file.read(1)
        if prefix == b"+": return self._line().decode()
        if prefix == b"-": raise RuntimeError(self._line().decode())
        if prefix == b":": return int(self._line())
        if prefix == b"$":
            size = int(self._line())
            if size < 0: return None
            value = self.file.read(size)
            if self.file.read(2) != b"\r\n": raise OSError("invalid RESP bulk terminator")
            return value.decode()
        if prefix == b"*":
            count = int(self._line())
            return [self._response() for _ in range(max(0, count))]
        raise OSError(f"unsupported RESP prefix {prefix!r}")


def probe_backend(name: str, endpoint: str, operations: int, payload_bytes: int,
                  warmup: int) -> dict:
    host, port_text = endpoint.rsplit(":", 1)
    client = RespClient(host, int(port_text))
    value = "x" * payload_bytes
    prefix = f"titan:backend-benchmark:{name}:{time.time_ns()}"
    try:
        for index in range(warmup):
            client.command("SET", f"{prefix}:warm:{index}", value)
            client.command("GET", f"{prefix}:warm:{index}")
        writes, reads, set_adds, set_reads, set_removes = [], [], [], [], []
        set_key = f"{prefix}:members"
        started = time.perf_counter_ns()
        for index in range(operations):
            key = f"{prefix}:item:{index}"
            before = time.perf_counter_ns(); response = client.command("SET", key, value)
            writes.append((time.perf_counter_ns() - before) / 1_000_000)
            if response != "OK": raise RuntimeError(f"SET returned {response!r}")
            before = time.perf_counter_ns(); observed = client.command("GET", key)
            reads.append((time.perf_counter_ns() - before) / 1_000_000)
            if observed != value: raise RuntimeError("GET payload mismatch")
            member = f"member-{index}"
            before = time.perf_counter_ns(); added = client.command("SADD", set_key, member)
            set_adds.append((time.perf_counter_ns() - before) / 1_000_000)
            if added != 1: raise RuntimeError(f"SADD returned {added!r}")
            before = time.perf_counter_ns(); members = client.command("SMEMBERS", set_key)
            set_reads.append((time.perf_counter_ns() - before) / 1_000_000)
            if member not in members: raise RuntimeError("SMEMBERS omitted inserted member")
            before = time.perf_counter_ns(); removed = client.command("SREM", set_key, member)
            set_removes.append((time.perf_counter_ns() - before) / 1_000_000)
            if removed != 1: raise RuntimeError(f"SREM returned {removed!r}")
        elapsed = (time.perf_counter_ns() - started) / 1_000_000_000
        return {"name": name, "endpoint": endpoint, "operations": operations,
                "payload_bytes": payload_bytes, "set": distribution(writes),
                "get": distribution(reads), "sadd": distribution(set_adds),
                "smembers": distribution(set_reads), "srem": distribution(set_removes),
                "round_trips_per_second": (operations * 5) / elapsed,
                "verified": True}
    finally:
        client.close()


def scheduler_case(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = data.get("granularity_summary") or []
    return {"path": str(path.resolve()), "configuration": data.get("configuration") or {},
            "metg50": data.get("metg50") or {}, "rows": rows,
            "valid": (data.get("validity") or {}).get("valid_cases"),
            "total": (data.get("validity") or {}).get("total_cases")}


def comparable_configuration(cases: dict[str, dict]) -> dict:
    keys = ("configured_workers", "slots_per_worker", "passes", "warmup_passes",
            "durations_ms", "tasks")
    normalized = {name: {key: case["configuration"].get(key) for key in keys}
                  for name, case in cases.items()}
    if len({json.dumps(value, sort_keys=True) for value in normalized.values()}) > 1:
        raise ValueError(f"scheduler configurations differ: {normalized}")
    return next(iter(normalized.values()), {})


def render(report: dict, destination: Path) -> None:
    cards = "".join(f"<article><h3>{escape(item['name'])}</h3><b>{item['round_trips_per_second']:.1f}</b><small>sequential RESP operations/s</small><p>SET p50/p95/p99: {item['set']['p50_ms']:.3f} / {item['set']['p95_ms']:.3f} / {item['set']['p99_ms']:.3f} ms<br>GET p50/p95/p99: {item['get']['p50_ms']:.3f} / {item['get']['p95_ms']:.3f} / {item['get']['p99_ms']:.3f} ms<br>SADD p50/p95/p99: {item['sadd']['p50_ms']:.3f} / {item['sadd']['p95_ms']:.3f} / {item['sadd']['p99_ms']:.3f} ms<br>SMEMBERS p50/p95/p99: {item['smembers']['p50_ms']:.3f} / {item['smembers']['p95_ms']:.3f} / {item['smembers']['p99_ms']:.3f} ms<br>SREM p50/p95/p99: {item['srem']['p50_ms']:.3f} / {item['srem']['p95_ms']:.3f} / {item['srem']['p99_ms']:.3f} ms</p></article>" for item in report["protocol"])
    rows = "".join(f"<tr><td>{escape(name)}</td><td>{case['valid']}/{case['total']}</td><td>{case['metg50'].get('observed_ms')}</td><td>{max((row.get('throughput') or 0 for row in case['rows']), default=0):.3f}</td><td>{escape(case['path'])}</td></tr>" for name, case in report["scheduler"].items())
    html = f"""<!doctype html><html><head><meta charset='utf-8'><title>Titan Store backend comparison</title><style>body{{margin:auto;max-width:1100px;padding:30px;background:#081014;color:#eaf2f2;font:14px/1.5 Segoe UI,Arial}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}}article,section{{background:#101b20;border:1px solid #29414a;border-radius:10px;padding:18px;margin:14px 0}}b{{font-size:25px;display:block;color:#54e6a8}}small,p{{color:#9bb0b4}}table{{width:100%;border-collapse:collapse}}th,td{{padding:9px;border-bottom:1px solid #29414a;text-align:left}}code{{color:#72c7ff}}</style></head><body><h1>TitanStore versus Redis</h1><p>Same RESP client contract. Direct protocol latency is separated from scheduler workload results.</p><div class='cards'>{cards}</div><section><h2>Controlled scheduler sweep</h2><table><tr><th>Backend</th><th>Valid DAGs</th><th>METG(50), ms</th><th>Peak median tasks/s</th><th>Evidence</th></tr>{rows}</table></section><section><h2>Comparison boundary</h2><pre>{escape(json.dumps(report['controlled_configuration'], indent=2))}</pre><p>Only summaries with identical worker, slot, pass, warm-up, duration and task-count settings are accepted.</p></section><p>Generated {escape(report['generated_at'])}</p></body></html>"""
    destination.write_text(html, encoding="utf-8")


def parse_mapping(items: list[str]) -> dict[str, str]:
    result = {}
    for item in items:
        if "=" not in item: raise ValueError(f"expected NAME=VALUE: {item}")
        name, value = item.split("=", 1); result[name] = value
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", action="append", default=[], metavar="NAME=HOST:PORT")
    parser.add_argument("--scheduler-summary", action="append", default=[], metavar="NAME=PATH")
    parser.add_argument("--operations", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.operations < 10 or args.warmup < 0 or args.payload_bytes < 1:
        parser.error("operations >= 10, warmup >= 0 and payload-bytes >= 1 are required")
    endpoints = parse_mapping(args.backend)
    summaries = {name: scheduler_case(Path(path))
                 for name, path in parse_mapping(args.scheduler_summary).items()}
    if set(summaries) and set(summaries) != set(endpoints):
        parser.error("backend and scheduler-summary names must match")
    report = {"schema": "titan-store-backend-comparison-v1",
              "generated_at": datetime.now(timezone.utc).isoformat(),
              "protocol": [probe_backend(name, endpoint, args.operations, args.payload_bytes, args.warmup)
                           for name, endpoint in endpoints.items()],
              "scheduler": summaries,
              "controlled_configuration": comparable_configuration(summaries)}
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    render(report, args.output / "report.html")
    print(args.output / "report.html")


if __name__ == "__main__":
    main()

