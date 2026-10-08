"""Bounded Titan DAG benchmark driven by an official WfFormat instance."""
import argparse
import hashlib
import json
import sys
import time
import urllib.request
import urllib.error
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from titan_sdk.titan_sdk import TitanClient, TitanJob  # noqa: E402

SOURCE_ROOT = "https://github.com/wfcommons/WfInstances/blob/main/pegasus/"
FAMILIES = {"montage", "seismology", "soykb", "1000genome"}


def family_of(filename):
    family = filename.split("-", 1)[0].lower()
    if family not in FAMILIES:
        raise ValueError(f"Unsupported WfInstances family: {family}")
    return family


def read_graph(path):
    raw = Path(path).read_bytes()
    tasks = json.loads(raw)["workflow"]["specification"]["tasks"]
    graph = {str(task["id"]): tuple(map(str, task.get("parents") or ())) for task in tasks}
    if len(graph) != len(tasks) or any(p not in graph for deps in graph.values() for p in deps):
        raise ValueError("Duplicate task ID or missing parent")
    return graph, hashlib.sha256(raw).hexdigest()


def order_graph(graph):
    children = defaultdict(list)
    remaining = {job: len(deps) for job, deps in graph.items()}
    for job, deps in graph.items():
        for parent in deps:
            children[parent].append(job)
    ready = deque(sorted(job for job, count in remaining.items() if not count))
    order = []
    while ready:
        job = ready.popleft()
        order.append(job)
        for child in children[job]:
            remaining[child] -= 1
            if not remaining[child]:
                ready.append(child)
    if len(order) != len(graph):
        raise ValueError("Cyclic WfFormat graph")
    return order, children


def sample_graph(graph, limit):
    if limit >= len(graph):
        return order_graph(graph)[0]
    order, children = order_graph(graph)
    def ancestors(job):
        result, stack = set(), [job]
        while stack:
            current = stack.pop()
            if current not in result:
                result.add(current)
                stack.extend(graph[current])
        return result
    selected = next((ancestors(job) for job in sorted(order, key=lambda j: -len(graph[j]))
                     if 2 <= len(ancestors(job)) <= limit), {order[0]})
    # Sets intentionally discard order. Sorting here makes a bounded sample
    # reproducible across Python processes and PYTHONHASHSEED values.
    frontier = deque(sorted(selected))
    while frontier and len(selected) < limit:
        for child in children[frontier.popleft()]:
            addition = ancestors(child) - selected
            if len(selected) + len(addition) <= limit:
                selected.update(addition)
                frontier.append(child)
    return [job for job in order if job in selected]


def inspect(graph, sha, filename):
    order, children = order_graph(graph)
    depth = {}
    for job in order:
        depth[job] = 1 + max((depth[parent] for parent in graph[job]), default=0)
    return {"source": SOURCE_ROOT + family_of(filename) + "/" + filename,
            "sha256": sha, "tasks": len(graph),
            "edges": sum(map(len, graph.values())),
            "roots": sum(not deps for deps in graph.values()),
            "leaves": sum(not children[job] for job in graph),
            "max_parents": max(map(len, graph.values())),
            "max_fan_out": max(map(len, children.values())),
            "critical_path_tasks": max(depth.values())}


def observe(name, graph, selected, seconds, timeout, started):
    """Poll an accepted run; tolerate a busy dashboard's transient 502s."""
    url = "http://127.0.0.1:5000/api/dag_status"
    deadline, dag = time.monotonic() + timeout, None
    last_completed, last_progress = -1, time.monotonic()
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=20) as reply:
                dag = next((item for item in json.load(reply)["dags"]
                            if item["id"] == "DAG-" + name), None)
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(2)
            continue
        if dag:
            completed = int(dag.get("completed") or 0)
            if completed > last_completed:
                last_completed, last_progress = completed, time.monotonic()
            if dag.get("status") == "COMPLETED" and completed == len(selected):
                break
            if dag.get("status") in ("FAILED", "CANCELLED") and time.monotonic() - last_progress > 60:
                break
        time.sleep(2)
    elapsed = time.time() - started
    counts = defaultdict(int)
    for job in (dag or {}).get("jobs", []):
        counts[job.get("status", "UNKNOWN")] += 1
    return {"dag": name, "url": f"http://127.0.0.1:5000/dags/DAG-{name}",
            "tasks": len(selected), "edges": sum(len(graph[job]) for job in selected),
            "surrogate_seconds": seconds, "wall_seconds": round(elapsed, 3),
            "jobs_per_second": round(len(selected) / elapsed, 2) if elapsed > 0 else None,
            "status": dag.get("status") if dag else "TIMEOUT",
            "completed": dag.get("completed") if dag else 0,
            "status_counts": dict(counts)}


def submission_plan(graph, selected, stamp):
    ids = {job: f"wfc_{index:04d}_{stamp}" for index, job in enumerate(selected)}
    return {
        "jobs": [{"source_task": job, "job_id": "DAG-" + ids[job],
                  "parents": ["DAG-" + ids[parent] for parent in graph[job] if parent in ids]}
                 for job in selected]
    }, ids


def live(graph, selected, seconds, timeout, family="montage"):
    stamp = int(time.time())
    name = f"WFC_{family.upper()}_{len(selected)}_{stamp}"
    plan, ids = submission_plan(graph, selected, stamp)
    jobs = [TitanJob(ids[job], str(Path(__file__).with_name("stage.py")),
                     args=f"{index} {seconds}", parents=[ids[p] for p in graph[job] if p in ids])
            for index, job in enumerate(selected)]
    started = time.time()
    response = TitanClient().submit_dag(name, jobs)
    if response != "DAG_ACCEPTED":
        raise RuntimeError(f"Master rejected DAG: {response}")
    return observe(name, graph, selected, seconds, timeout, started), plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("instance", type=Path)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--observe-dag", help="Observe an already accepted run without submitting it again")
    parser.add_argument("--max-tasks", type=int, default=619)
    parser.add_argument("--stage-seconds", type=float, default=.05)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 2 <= args.max_tasks <= 10_000 or not 0 <= args.stage_seconds <= 2:
        parser.error("Use 2-10000 tasks and 0-2 stage seconds")
    graph, sha = read_graph(args.instance)
    selected = sample_graph(graph, args.max_tasks)
    result = {"instance": inspect(graph, sha, args.instance.name),
              "sample": {"tasks": len(selected), "edges": sum(len(graph[j]) for j in selected)}}
    if args.observe_dag:
        started = float(args.observe_dag.rsplit("_", 1)[-1])
        result["live"] = observe(args.observe_dag, graph, selected,
                                 args.stage_seconds, args.timeout, started)
        result["submission"], _ = submission_plan(
            graph, selected, args.observe_dag.rsplit("_", 1)[-1]
        )
    elif args.live:
        result["live"], result["submission"] = live(
            graph, selected, args.stage_seconds, args.timeout,
            family_of(args.instance.name)
        )
    rendered = json.dumps(result, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
