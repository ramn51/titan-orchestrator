"""Run a real Gemini-planned ParallelQA workflow that evolves while Titan executes it."""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.request

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
import titan_sdk.titan_sdk as sdk  # noqa: E402
from titan_sdk.titan_sdk import TitanClient, TitanJob  # noqa: E402
from titan_test_suite.external_agent_benchmark.bfcl_runner import configure_dashboard_manifest  # noqa: E402
from titan_test_suite.external_agent_benchmark.runner import wait_for  # noqa: E402

SAFE_ID = re.compile(r"[A-Za-z0-9_-]{1,48}")
TOOLS = {
    "lookup": {"required": {"entity"}, "allowed": {"entity"}},
    "scale": {"required": {"source", "factor"}, "allowed": {"source", "factor"}},
    "compare": {"required": {"sources", "selection"}, "allowed": {"sources", "selection"}},
    "answer": {"required": {"source"}, "allowed": {"source"}},
}


def encoded(value: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode().rstrip("=")


def validate_frontier(value: object, known: set[str], entities: set[str]) -> dict:
    if not isinstance(value, dict) or value.get("action") not in {"SUBMIT", "FINISH"}:
        raise ValueError("planner response requires action SUBMIT or FINISH")
    if value["action"] == "FINISH":
        return {"action": "FINISH", "reason": str(value.get("reason", "")), "jobs": []}
    raw_jobs = value.get("jobs")
    if not isinstance(raw_jobs, list) or not 1 <= len(raw_jobs) <= 8:
        raise ValueError("SUBMIT requires one to eight jobs")
    ids = []
    jobs = []
    for raw in raw_jobs:
        if not isinstance(raw, dict) or not SAFE_ID.fullmatch(str(raw.get("id", ""))):
            raise ValueError("job ID is invalid")
        node = raw["id"]
        if node in known or node in ids:
            raise ValueError("job ID is duplicated or already used")
        tool = raw.get("tool")
        if tool not in TOOLS:
            raise ValueError("unknown tool")
        arguments = raw.get("arguments")
        if not isinstance(arguments, dict) or not TOOLS[tool]["required"].issubset(arguments):
            raise ValueError(f"missing arguments for {tool}")
        unused = set(arguments) - TOOLS[tool]["allowed"]
        if unused:
            raise ValueError(f"unused arguments for {tool}: {', '.join(sorted(unused))}")
        parents = raw.get("parents", [])
        if not isinstance(parents, list) or any(parent not in known for parent in parents):
            raise ValueError("a frontier may depend only on completed prior jobs")
        if tool == "lookup" and arguments["entity"] not in entities:
            raise ValueError("lookup selected an entity outside the task catalog")
        if tool == "compare" and arguments["selection"] not in {"min", "max"}:
            raise ValueError("compare selection must be min or max")
        sources = ([arguments["source"]] if "source" in arguments else arguments.get("sources", []))
        if any(source not in parents for source in sources):
            raise ValueError("every consumed source must be declared as a parent")
        ids.append(node)
        jobs.append({"id": node, "tool": tool, "arguments": arguments, "parents": parents})
    return {"action": "SUBMIT", "reason": str(value.get("reason", "")), "jobs": jobs}


class GeminiFrontierPlanner:
    def __init__(self, key: str, model: str, max_requests: int = 7):
        self.key = key
        self.model = model.removeprefix("models/")
        self.max_requests = max_requests
        self.requests = 0

    def plan(self, question: str, entities: list[str], completed: dict, known: set[str]) -> tuple[dict, str]:
        prompt = f"""You control a distributed DAG one executable frontier at a time.
Goal: {question}
Allowed entities: {json.dumps(entities)}
Completed job results: {json.dumps(completed, indent=2)}
Used job IDs: {json.dumps(sorted(known))}

Tools:
- lookup(entity): retrieve a measurement for one allowed entity.
- scale(source, factor): multiply a lookup measurement; source must be a completed parent.
- compare(sources, selection): choose the minimum or maximum adjusted value requested by the question; selection is "min" or "max" and every source must be a completed parent.
- answer(source): produce the final answer from a completed comparison parent.

Return only the NEXT executable frontier. Independent jobs should be in the same response.
Do not predict tool outputs. Observe them before planning the next frontier.
After an answer job has completed, return FINISH. Every consumed source must also appear in parents.
Call one or more available functions for the NEXT frontier. Calls in the same response must be independent.
The adapter assigns safe job IDs and derives DAG parents from your source arguments.
"""
        attempts = []
        for attempt in range(3):
            if self.requests >= self.max_requests:
                raise RuntimeError(
                    f"model request budget exhausted ({self.requests}/{self.max_requests})")
            retry_context = ""
            if attempts:
                retry_context = ("\nYour previous response was rejected. Correct it.\nValidation error: "
                                 + attempts[-1]["error"] + "\nPrevious JSON:\n"
                                 + attempts[-1]["response"][:800])
            declarations = [
                {"name": "lookup", "description": "Retrieve one allowed entity measurement.",
                 "parameters": {"type": "OBJECT", "properties": {
                     "entity": {"type": "STRING", "enum": entities}}, "required": ["entity"]}},
                {"name": "scale", "description": "Multiply one completed lookup measurement.",
                 "parameters": {"type": "OBJECT", "properties": {
                     "source": {"type": "STRING", "description": "One completed job ID."},
                     "factor": {"type": "NUMBER"}}, "required": ["source", "factor"]}},
                {"name": "compare", "description": "Choose the requested minimum or maximum adjusted value.",
                 "parameters": {"type": "OBJECT", "properties": {
                     "sources": {"type": "ARRAY", "maxItems": 8,
                                 "items": {"type": "STRING"},
                                 "description": "Completed scale job IDs."},
                     "selection": {"type": "STRING", "enum": ["min", "max"]}},
                     "required": ["sources", "selection"]}},
                {"name": "answer", "description": "Produce the final answer from a comparison.",
                 "parameters": {"type": "OBJECT", "properties": {
                     "source": {"type": "STRING", "description": "One completed compare job ID."}},
                     "required": ["source"]}},
                {"name": "finish", "description": "Finish only after an answer job has completed.",
                 "parameters": {"type": "OBJECT", "properties": {
                     "reason": {"type": "STRING"}}, "required": ["reason"]}},
            ]
            body = json.dumps({
                "contents": [{"parts": [{"text": prompt + retry_context}]}],
                "tools": [{"functionDeclarations": declarations}],
                "toolConfig": {"functionCallingConfig": {"mode": "ANY"}},
                "generationConfig": {"temperature": 0, "maxOutputTokens": 1536},
            }).encode()
            request = urllib.request.Request(
                f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent",
                data=body, headers={"x-goog-api-key": self.key, "Content-Type": "application/json"},
                method="POST")
            self.requests += 1
            with urllib.request.urlopen(request, timeout=60) as response:
                raw = json.load(response)
            parts = (((raw.get("candidates") or [{}])[0].get("content") or {}).get("parts") or [])
            if not parts:
                raise RuntimeError("Gemini returned no planner response")
            calls = [part["functionCall"] for part in parts if "functionCall" in part]
            text = json.dumps(calls, separators=(",", ":"))
            try:
                return function_calls_to_frontier(calls, known, set(entities)), text
            except ValueError as error:
                attempts.append({"attempt": attempt + 1, "response": text, "error": str(error)})
        raise ValueError("Gemini failed planner validation after three attempts: "
                         + json.dumps(attempts))


def function_calls_to_frontier(calls: object, known: set[str], entities: set[str]) -> dict:
    if not isinstance(calls, list) or not 1 <= len(calls) <= 8:
        raise ValueError("model must return one to eight function calls")
    names = [call.get("name") for call in calls if isinstance(call, dict)]
    if "finish" in names:
        if len(calls) != 1:
            raise ValueError("finish cannot be mixed with executable tools")
        return {"action": "FINISH", "reason": str(calls[0].get("args", {}).get("reason", "")),
                "jobs": []}
    jobs = []
    for index, call in enumerate(calls):
        if not isinstance(call, dict) or call.get("name") not in TOOLS:
            raise ValueError("model selected an unknown function")
        tool = call["name"]
        arguments = call.get("args")
        if not isinstance(arguments, dict) or not TOOLS[tool]["required"].issubset(arguments):
            raise ValueError(f"missing arguments for {tool}")
        unused = set(arguments) - TOOLS[tool]["allowed"]
        if unused:
            raise ValueError(f"unused arguments for {tool}: {', '.join(sorted(unused))}")
        if tool == "compare" and arguments["selection"] not in {"min", "max"}:
            raise ValueError("compare selection must be min or max")
        if tool == "lookup":
            if arguments["entity"] not in entities:
                raise ValueError("lookup selected an entity outside the task catalog")
            parents = []
        elif tool in {"scale", "answer"}:
            parents = [arguments["source"]]
        else:
            parents = list(arguments["sources"])
            if not parents:
                raise ValueError("compare requires at least one source")
        if any(parent not in known for parent in parents):
            raise ValueError("a frontier may consume only completed prior jobs")
        jobs.append({"id": f"job_{len(known) + index}", "tool": tool,
                     "arguments": arguments, "parents": parents})
    return {"action": "SUBMIT", "reason": "native Gemini function calls", "jobs": jobs}


def store_result(client: TitanClient, run_id: str, node: str) -> dict:
    value = client.store_get(f"agent:{run_id}:{node}")
    if not value or value.startswith("ERROR"):
        raise RuntimeError(f"missing result for {node}: {value}")
    return json.loads(value)


def materialize_artifact(job_id: str, name: str, value: object) -> str:
    root = REPO / "titan_workspace" / "shared" / "artifacts" / ("DAG-" + job_id)
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return path.relative_to(REPO / "titan_workspace" / "shared").as_posix()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", default="1")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--model", default="gemini-2.5-flash")
    parser.add_argument("--api-key-file", type=Path)
    parser.add_argument("--max-iterations", type=int, default=6)
    parser.add_argument("--max-model-calls", type=int, default=7)
    parser.add_argument("--requirement", default="GENERAL")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    key = os.environ.get("GEMINI_API_KEY", "")
    if not key and args.api_key_file:
        key = args.api_key_file.read_text(encoding="utf-8").strip()
    if not key:
        raise RuntimeError("GEMINI_API_KEY or --api-key-file is required")
    cases = json.loads(Path(__file__).with_name("parallelqa_cases.json").read_text(encoding="utf-8"))
    case = next(item for item in cases["cases"] if item["id"] == args.case)
    catalog = {item["name"]: {"measurement": item["measurement"], "unit": item["unit"]}
               for item in case["entities"]}
    sdk.TITAN_HOST, sdk.TITAN_PORT = args.host, args.port
    configure_dashboard_manifest()
    run_id = "llmdyn_" + str(int(time.time()))
    planner = GeminiFrontierPlanner(key, args.model, args.max_model_calls)
    client = TitanClient()
    script = str(Path(__file__).with_name("llm_dynamic_stage.py"))
    known: set[str] = set()
    completed: dict[str, dict] = {}
    stages = []
    started = time.perf_counter()
    finished = False
    for iteration in range(args.max_iterations):
        decision, raw = planner.plan(case["question"], list(catalog), completed, known)
        stage_record = {"iteration": iteration, "planner": args.model, "decision": decision,
                        "raw_response": raw}
        if decision["action"] == "FINISH":
            stage_record["dag"] = None
            stages.append(stage_record)
            finished = True
            break
        jobs = []
        for spec in decision["jobs"]:
            node = run_id + "_" + spec["id"]
            parent_ids = [run_id + "_" + parent for parent in spec["parents"]]
            payload = dict(spec["arguments"])
            if spec["tool"] == "lookup":
                payload["catalog"] = catalog
            if spec["tool"] == "answer":
                payload["question"] = case["question"]
            # Store keys use logical node IDs; worker job IDs include the run prefix.
            for field in ("source",):
                if field in payload:
                    payload[field] = run_id + "_" + payload[field]
            if "sources" in payload:
                payload["sources"] = [run_id + "_" + value for value in payload["sources"]]
            jobs.append(TitanJob(node, script,
                args=f"{run_id} {spec['tool']} {node} {encoded(payload)}", parents=parent_ids,
                requirement=args.requirement))
        dag_name = f"LLM_DYNAMIC_PARALLELQA_{iteration}_{run_id}"
        response = client.submit_dag(dag_name, jobs, agent_run_id=run_id,
            agent_run_name="LLM-Planned Dynamic ParallelQA",
            agent_stage=f"FRONTIER_{iteration}", agent_stage_order=iteration)
        if response != "DAG_ACCEPTED":
            raise RuntimeError(f"frontier {iteration} rejected: {response}")
        statuses = {}
        artifacts = []
        for spec, job in zip(decision["jobs"], jobs):
            status = wait_for(client, "DAG-" + job.id, args.timeout)
            statuses[job.id] = status
            if status != "COMPLETED":
                raise RuntimeError(f"{job.id} ended as {status}")
            completed[spec["id"]] = store_result(client, run_id, job.id)
            artifacts.append(materialize_artifact(job.id, "tool-result.json", {
                "logical_job": spec["id"], "tool": spec["tool"],
                "parents": spec["parents"], "result": completed[spec["id"]],
            }))
            known.add(spec["id"])
        stage_record.update({"dag": dag_name, "statuses": statuses,
                             "dashboard_artifacts": artifacts,
                             "results": {spec["id"]: completed[spec["id"]]
                                         for spec in decision["jobs"]}})
        stages.append(stage_record)
    answers = [value for key_name, value in completed.items()
               if any(job["id"] == key_name and job["tool"] == "answer"
                      for stage in stages for job in stage.get("decision", {}).get("jobs", []))]
    actual = answers[-1].get("answer") if answers else None
    valid = bool(finished and actual and actual.casefold() == case["answer"].casefold()
                 and len([stage for stage in stages if stage.get("dag")]) >= 2)
    summary = {"schema": "titan-llm-dynamic-agent-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "Gemini plans each frontier after observing prior Titan job results",
        "mutation_boundary": "Runtime DAG evolution across staged submissions; not atomic in-place mutation",
        "question": case["question"], "expected_answer": case["answer"],
        "actual_answer": actual, "agent_run_id": run_id, "stages": stages,
        "worker_requirement": args.requirement,
        "model_calls": planner.requests, "model_call_limit": args.max_model_calls,
        "wall_seconds": round(time.perf_counter() - started, 3), "valid": valid,
        "agent_url": "/agents"}
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    if not valid:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
