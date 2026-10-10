"""Combine compatible task-granularity benchmark invocations into one report."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from titan_test_suite.task_granularity_benchmark.runner import median_rows, observed_metg, render_report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summaries", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runtime-metadata", type=Path,
                        help="JSON describing hardware, store mode, and other run context")
    args = parser.parse_args()
    sources = [json.loads(path.read_text(encoding="utf-8")) for path in args.summaries]
    capacities = {item["configuration"]["capacity"] for item in sources}
    if len(capacities) != 1:
        parser.error("all inputs must use the same controlled capacity")
    results = [result for item in sources for result in item["results"]]
    for result in results:
        if "slot_utilization" not in result:
            window = result.get("execution_window_seconds") or 0
            capacity = result.get("capacity") or 0
            result["slot_utilization"] = round(
                min(1, result.get("requested_work_ms", 0) / (capacity * window * 1000)), 6
            ) if window and capacity else 0
    granularity = median_rows(results, "granularity", "requested_duration_ms")
    combined = dict(sources[0])
    combined["generated_at"] = datetime.now(timezone.utc).isoformat()
    combined["configuration"] = dict(sources[0]["configuration"])
    combined["configuration"]["suites"] = sorted({result["suite"] for result in results})
    combined["configuration"]["combined_inputs"] = [str(path.resolve()) for path in args.summaries]
    if args.runtime_metadata:
        combined["configuration"]["runtime"] = json.loads(
            args.runtime_metadata.read_text(encoding="utf-8-sig"))
    combined["results"] = results
    combined["granularity_summary"] = granularity
    combined["metg50"] = observed_metg(granularity)
    combined["validity"] = {
        "valid_cases": sum(result["valid"] for result in results),
        "total_cases": len(results),
        "invalid_cases": [{"label": result["label"], "errors": result["errors"]}
                          for result in results if not result["valid"]],
        "limitations": ["single-host run; no network communication-hiding claim",
                        "Python process launch is part of measured Titan execution overhead",
                        "METG is specific to this worker/slot/hardware configuration"],
    }
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(combined, indent=2) + "\n", encoding="utf-8")
    render_report(combined, args.output / "report.html")
    print(args.output / "report.html")


if __name__ == "__main__":
    main()
