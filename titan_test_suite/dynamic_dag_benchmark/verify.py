"""Independent verifier for dynamic DAG benchmark event journals."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path


def _graph_hash(specs: dict[str, dict]) -> str:
    canonical = [
        {"id": node_id, "parents": list(specs[node_id]["parents"]),
         "kind": specs[node_id]["kind"]}
        for node_id in sorted(specs)
    ]
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _has_cycle(specs: dict[str, dict]) -> bool:
    indegree = {node_id: len(spec["parents"]) for node_id, spec in specs.items()}
    children = defaultdict(list)
    for node_id, spec in specs.items():
        for parent in spec["parents"]:
            children[parent].append(node_id)
    ready = [node_id for node_id, degree in indegree.items() if degree == 0]
    visited = 0
    while ready:
        node_id = ready.pop()
        visited += 1
        for child in children[node_id]:
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
    return visited != len(specs)


def verify_events(events: list[dict], expected: dict | None = None) -> dict:
    """Replay a case without importing or calling the reference model."""
    errors: list[str] = []
    specs: dict[str, dict] = {}
    status: dict[str, str] = {}
    ready_counts = Counter()
    mutations: dict[str, tuple[str, int]] = {}
    version = 0
    previous_seq = 0

    for event in events:
        seq = event.get("seq")
        if seq != previous_seq + 1:
            errors.append(f"non-contiguous sequence at {seq}; expected {previous_seq + 1}")
        previous_seq = seq if isinstance(seq, int) else previous_seq
        event_type = event.get("type")

        if event_type == "mutation_applied":
            if event.get("version_before") != version or event.get("version_after") != version + 1:
                errors.append(f"invalid version transition for {event.get('mutation_id')}")
            mutation_id = event.get("mutation_id")
            if mutation_id in mutations:
                errors.append(f"mutation applied twice: {mutation_id}")
            for node in event.get("add", []):
                node_id = node.get("id")
                parents = tuple(node.get("parents", []))
                if node_id in specs:
                    errors.append(f"node added twice: {node_id}")
                missing = [parent for parent in parents if parent not in specs
                           and parent not in {item.get('id') for item in event.get('add', [])}]
                if missing:
                    errors.append(f"{node_id} has missing parents: {missing}")
                specs[node_id] = {"parents": parents, "kind": node.get("kind", "task")}
                status[node_id] = "PENDING"
            if _has_cycle(specs):
                errors.append(f"cycle present after mutation {mutation_id}")
            for node_id in event.get("cancel", []):
                if node_id not in specs:
                    errors.append(f"mutation cancelled unknown node: {node_id}")
            version += 1
            mutations[mutation_id] = (event.get("fingerprint", ""), version)
        elif event_type == "mutation_duplicate":
            known = mutations.get(event.get("mutation_id"))
            if known != (event.get("fingerprint", ""), event.get("version")):
                errors.append(f"invalid duplicate mutation: {event.get('mutation_id')}")
        elif event_type == "node_ready":
            node_id = event.get("node_id")
            if node_id not in specs:
                errors.append(f"unknown node became ready: {node_id}")
                continue
            if status[node_id] != "PENDING":
                errors.append(f"{node_id} became ready from {status[node_id]}")
            incomplete = [parent for parent in specs[node_id]["parents"]
                          if status.get(parent) != "COMPLETED"]
            if incomplete:
                errors.append(f"{node_id} became ready before parents completed: {incomplete}")
            status[node_id] = "READY"
            ready_counts[node_id] += 1
        elif event_type == "node_running":
            node_id = event.get("node_id")
            if status.get(node_id) != "READY":
                errors.append(f"{node_id} ran from {status.get(node_id)}")
            status[node_id] = "RUNNING"
        elif event_type == "node_completed":
            node_id = event.get("node_id")
            if status.get(node_id) != "RUNNING":
                errors.append(f"{node_id} completed from {status.get(node_id)}")
            incomplete = [parent for parent in specs.get(node_id, {}).get("parents", ())
                          if status.get(parent) != "COMPLETED"]
            if incomplete:
                errors.append(f"{node_id} completed before parents: {incomplete}")
            status[node_id] = "COMPLETED"
        elif event_type == "completion_duplicate":
            if status.get(event.get("node_id")) != "COMPLETED":
                errors.append(f"duplicate completion for non-completed node: {event.get('node_id')}")
        elif event_type == "node_cancelled":
            node_id = event.get("node_id")
            if node_id not in specs:
                errors.append(f"unknown node cancelled: {node_id}")
            elif status[node_id] in {"COMPLETED", "FAILED", "CANCELLED"}:
                errors.append(f"terminal node cancelled: {node_id}")
            status[node_id] = "CANCELLED"
        elif event_type == "node_failed":
            node_id = event.get("node_id")
            if status.get(node_id) != "RUNNING":
                errors.append(f"{node_id} failed from {status.get(node_id)}")
            status[node_id] = "FAILED"
        else:
            errors.append(f"unknown event type: {event_type}")

    duplicates = sorted(node_id for node_id, count in ready_counts.items() if count != 1)
    if duplicates:
        errors.append(f"nodes did not become ready exactly once: {duplicates[:20]}")
    nonterminal = sorted(node_id for node_id, value in status.items()
                         if value not in {"COMPLETED", "FAILED", "CANCELLED"})
    if nonterminal:
        errors.append(f"nonterminal nodes remain: {nonterminal[:20]}")

    observed = {
        "version": version,
        "nodes": len(specs),
        "edges": sum(len(spec["parents"]) for spec in specs.values()),
        "status": dict(sorted(Counter(status.values()).items())),
        "duplicate_ready": sorted(node_id for node_id, count in ready_counts.items() if count > 1),
        "graph_hash": _graph_hash(specs),
        "event_count": len(events),
    }
    if expected:
        for key in ("version", "nodes", "edges", "status", "duplicate_ready", "graph_hash", "event_count"):
            if observed[key] != expected.get(key):
                errors.append(f"snapshot mismatch for {key}: {observed[key]!r} != {expected.get(key)!r}")
    return {"passed": not errors, "errors": errors, "observed": observed}


def read_journal(path: Path) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"{path}:{line_number}: {error}") from error
            case_id = item.pop("case_id", None)
            if not case_id:
                raise ValueError(f"{path}:{line_number}: missing case_id")
            grouped[case_id].append(item)
    return grouped


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path)
    parser.add_argument("--expected", type=Path, required=True)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    expected = json.loads(args.expected.read_text(encoding="utf-8"))
    journals = read_journal(args.journal)
    results = {}
    for case_id, snapshot in expected["cases"].items():
        results[case_id] = verify_events(journals.get(case_id, []), snapshot["snapshot"])
    report = {"passed": bool(results) and all(item["passed"] for item in results.values()),
              "cases": results}
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
