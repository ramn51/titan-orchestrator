"""Dependency-free worker stage for the ParallelQA compatibility adapter.

The task plan and factual inputs are pinned so this stage measures Titan's
execution of an agent-produced parallel tool graph.  It deliberately does not
claim to evaluate an LLM planner or live Wikipedia retrieval.
"""
from __future__ import annotations

import base64
import json
from pathlib import Path
import sys


def safe(value: str) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    if not value or any(char not in allowed for char in value):
        raise ValueError(f"unsafe identifier: {value!r}")
    return value


def decode(raw: str) -> dict:
    padding = "=" * (-len(raw) % 4)
    return json.loads(base64.urlsafe_b64decode(raw + padding).decode("utf-8"))


def encode(record: dict) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":"))


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("usage: stage.py <run-id> <operation> <node> <payload-b64>", file=sys.stderr)
        return 2
    run_id, operation, node = map(safe, argv[:3])
    payload = decode(argv[3])
    root = Path("external_agent_benchmark") / run_id
    root.mkdir(parents=True, exist_ok=True)

    if operation == "plan":
        result = {
            "question": payload["question"],
            "strategy": "parallel lookup -> independent transforms -> comparison -> answer",
            "branches": [entity["name"] for entity in payload["entities"]],
            "planner": "pinned-reference-plan",
        }
    elif operation == "lookup":
        result = {
            "entity": payload["name"], "measurement": float(payload["measurement"]),
            "unit": payload["unit"], "tool": "pinned-knowledge-lookup",
        }
    elif operation == "transform":
        source = json.loads((root / f"{safe(payload['source'])}.json").read_text(encoding="utf-8"))
        result = dict(source)
        result.update({"factor": float(payload["factor"]),
                       "adjusted": source["measurement"] * float(payload["factor"]),
                       "tool": "calculator"})
    elif operation == "compare":
        values = [json.loads((root / f"{safe(name)}.json").read_text(encoding="utf-8"))
                  for name in payload["sources"]]
        selection = payload.get("selection", "min")
        if selection not in {"min", "max"}:
            raise ValueError(f"unsupported comparison selection: {selection}")
        winner = (min if selection == "min" else max)(
            values, key=lambda item: item["adjusted"])
        result = {"answer": winner["entity"], "adjusted_values": values,
                  "rule": f"{selection} adjusted measurement"}
    elif operation == "answer":
        comparison = json.loads((root / f"{safe(payload['source'])}.json").read_text(encoding="utf-8"))
        expected = payload["expected"]
        if comparison["answer"].casefold() != expected.casefold():
            raise RuntimeError(f"answer mismatch: expected {expected!r}, got {comparison['answer']!r}")
        result = {"question": payload["question"], "answer": comparison["answer"],
                  "expected": expected, "correct": True,
                  "evidence": comparison["adjusted_values"]}
        artifact = Path("artifacts") / f"DAG-{node}"
        artifact.mkdir(parents=True, exist_ok=True)
        (artifact / "parallelqa-result.json").write_text(
            json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print("PARALLELQA_RESULT " + encode(result), flush=True)
    else:
        raise ValueError(f"unsupported operation: {operation}")

    (root / f"{node}.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print("PARALLELQA_STAGE " + encode({"operation": operation, "node": node, **result}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
