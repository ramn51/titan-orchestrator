"""Execute one official BFCL multi-turn conversation as model-planned Titan stages."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import urllib.request

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
import titan_sdk.titan_sdk as sdk  # noqa: E402
from titan_sdk.titan_sdk import TitanClient, TitanJob  # noqa: E402
from titan_test_suite.external_agent_benchmark.bfcl_runner import configure_dashboard_manifest  # noqa: E402
from titan_test_suite.external_agent_benchmark.runner import encoded, wait_for  # noqa: E402


def normalize(value):
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def canonical(call: dict) -> tuple[str, str]:
    return call["name"], json.dumps(normalize(call["arguments"]), sort_keys=True, separators=(",", ":"))


def schema_type(value: str) -> str:
    return {"dict": "OBJECT", "string": "STRING", "integer": "INTEGER", "float": "NUMBER",
            "number": "NUMBER", "boolean": "BOOLEAN", "array": "ARRAY"}.get(value.lower(), "STRING")


def convert_schema(parameters: dict) -> dict:
    properties = {}
    for name, spec in parameters.get("properties", {}).items():
        item = {"type": schema_type(str(spec.get("type", "string")))}
        if item["type"] == "ARRAY":
            item["items"] = {"type": schema_type(str(spec.get("items", {}).get("type", "string")))}
        properties[name] = item
    result = {"type": "OBJECT", "properties": properties}
    if parameters.get("required"):
        result["required"] = list(parameters["required"])
    return result


class GeminiTurnPlanner:
    def __init__(self, key: str, model: str, max_requests: int):
        self.key = key
        self.model = model.removeprefix("models/")
        self.max_requests = max_requests
        self.requests = 0

    def plan(self, user: str, tools: list[dict], state: dict,
             observations: list[dict]) -> tuple[list[dict] | None, dict]:
        declarations = [{"name": tool["name"], "description": tool["description"],
                         "parameters": convert_schema(tool["parameters"])} for tool in tools]
        declarations.append({"name": "advance_turn",
            "description": "Finish this user turn only when its request has been fully satisfied.",
            "parameters": {"type": "OBJECT", "properties": {
                "reason": {"type": "STRING"}}, "required": ["reason"]}})
        prompt = f"""You are controlling tools for one turn in an official BFCL multi-turn conversation.
User request: {user}
Current application state: {json.dumps(state, indent=2)}
Tool results already observed during this turn: {json.dumps(observations, indent=2)}

Call only the next executable tools. Put independent calls in one response. Never invent an output
that should come from a tool. If a tool needs the result of another tool, wait until that result is
present in the observations. Do not repeat a tool call whose successful output is already present
in the observations. Call advance_turn alone as soon as this turn's request is fully satisfied.
"""
        errors = []
        for _ in range(3):
            if self.requests >= self.max_requests:
                raise RuntimeError(f"model request budget exhausted ({self.requests}/{self.max_requests})")
            suffix = "" if not errors else "\nCorrect the previous invalid choice: " + errors[-1]
            body = json.dumps({
                "contents": [{"parts": [{"text": prompt + suffix}]}],
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
            calls = [part["functionCall"] for part in parts if "functionCall" in part]
            evidence = {"request_number": self.requests, "function_calls": calls,
                        "response_sha256": hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()}
            try:
                return self.validate(calls, tools, state, observations), evidence
            except ValueError as error:
                errors.append(str(error))
        raise ValueError("Gemini failed frontier validation: " + "; ".join(errors))

    @staticmethod
    def validate(calls: list[dict], tools: list[dict], state: dict,
                 observations: list[dict]) -> list[dict] | None:
        if not calls:
            raise ValueError("no function call returned")
        if any(call.get("name") == "advance_turn" for call in calls):
            if len(calls) != 1:
                raise ValueError("advance_turn must be called alone")
            return None
        definitions = {tool["name"]: tool for tool in tools}
        completed = {
            canonical(item["call"])
            for item in observations
            if isinstance(item, dict)
            and isinstance(item.get("call"), dict)
            and not (isinstance(item.get("output"), dict) and "error" in item["output"])
        }
        result = []
        for call in calls:
            name = call.get("name")
            arguments = call.get("args", {})
            if name not in definitions or not isinstance(arguments, dict):
                raise ValueError("unknown function or invalid arguments")
            parameters = definitions[name]["parameters"]
            required = set(parameters.get("required", []))
            allowed = set(parameters.get("properties", {}))
            if not required.issubset(arguments) or set(arguments) - allowed:
                raise ValueError(f"argument mismatch for {name}")
            planned = {"name": name, "arguments": arguments}
            if canonical(planned) in completed:
                raise ValueError(f"{name} with these arguments already completed; advance the turn")
            result.append(planned)
        return result


def read_key(path: Path | None) -> str:
    key = os.environ.get("GEMINI_API_KEY", "")
    if key or not path:
        return key
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("GEMINI_API_KEY="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return path.read_text(encoding="utf-8").strip()


def load_result(client: TitanClient, run_id: str, node: str) -> dict:
    raw = client.store_get(f"agent:{run_id}:{node}")
    if not raw or raw.startswith("ERROR"):
        raise RuntimeError(f"missing worker result {node}: {raw}")
    return json.loads(raw)


def apply_state(state: dict, call: dict, output: dict) -> None:
    name, args = call["name"], call["arguments"]
    vehicle, messages = state["VehicleControlAPI"], state["MessageAPI"]
    if name == "fillFuelTank":
        vehicle["fuelLevel"] = output["fuelLevel"]
    elif name == "lockDoors":
        for door in args["door"]:
            vehicle["doorStatus"][door] = output["lockStatus"]
    elif name == "pressBrakePedal":
        vehicle["brakePressed"] = output["brakePedalStatus"] == "pressed"
    elif name == "startEngine":
        vehicle["engineState"] = output["engineState"]
    elif name == "send_message":
        receiver = args["receiver_id"]
        messages.setdefault("sent", {}).setdefault(receiver, []).append(args["message"])
        messages["message_count"] = output["message_id"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--model", default="gemini-2.5-flash")
    parser.add_argument("--api-key-file", type=Path)
    parser.add_argument("--max-model-calls", type=int, default=24)
    parser.add_argument("--max-frontiers-per-turn", type=int, default=5)
    parser.add_argument("--requirement", default="GENERAL")
    parser.add_argument("--timeout", type=float, default=240)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    key = read_key(args.api_key_file)
    if not key:
        raise RuntimeError("a Gemini API key is required; pinned planning is intentionally unsupported")
    document = json.loads(Path(__file__).with_name("bfcl_multiturn_case.json").read_text(encoding="utf-8"))
    case = document["case"]
    sdk.TITAN_HOST, sdk.TITAN_PORT = args.host, args.port
    configure_dashboard_manifest()
    client = TitanClient()
    planner = GeminiTurnPlanner(key, args.model, args.max_model_calls)
    run_id = "bfclmt_" + str(int(time.time()))
    state = deepcopy(case["initial_config"])
    script = str(Path(__file__).with_name("bfcl_multiturn_stage.py"))
    stages, model_evidence, observed_turns = [], [], []
    started = time.perf_counter()
    stage_order = 0
    aborted_reason = None
    for turn_index, user in enumerate(case["turns"]):
        observations = []
        observed_calls = []
        completed_turn = False
        for frontier in range(args.max_frontiers_per_turn):
            try:
                calls, evidence = planner.plan(user, case["tools"], state, observations)
            except (ValueError, RuntimeError) as error:
                aborted_reason = f"turn {turn_index + 1}: {error}"
                break
            evidence.update({"turn": turn_index + 1, "frontier": frontier + 1})
            model_evidence.append(evidence)
            (args.output / "model-evidence.partial.json").write_text(
                json.dumps(model_evidence, indent=2) + "\n", encoding="utf-8")
            if calls is None:
                completed_turn = True
                break
            token = f"t{turn_index + 1}f{frontier + 1}"
            context_id = f"{run_id}_{token}_context"
            context_payload = {"turn": turn_index + 1, "frontier": frontier + 1,
                               "user": user, "state": state}
            jobs = [TitanJob(context_id, script,
                             args=f"{run_id} context {context_id} {encoded(context_payload)}",
                             requirement=args.requirement)]
            tool_nodes = []
            named_nodes = {}
            for index, call in enumerate(calls):
                node = f"{run_id}_{token}_tool{index + 1}"
                semantic_sources = []
                if call["name"] == "estimate_distance":
                    semantic_sources = list(named_nodes.get("get_zipcode_based_on_city", []))
                elif call["name"] == "estimate_drive_feasibility_by_mileage":
                    semantic_sources = list(named_nodes.get("estimate_distance", []))
                elif call["name"] == "startEngine":
                    semantic_sources = (list(named_nodes.get("lockDoors", []))
                                        + list(named_nodes.get("pressBrakePedal", [])))
                parents = [context_id] + semantic_sources
                jobs.append(TitanJob(node, script, args=f"{run_id} tool {node} "
                    f"{encoded({'call': call, 'state': state, 'sources': semantic_sources})}",
                    parents=parents, requirement=args.requirement))
                tool_nodes.append(node)
                named_nodes.setdefault(call["name"], []).append(node)
            observe_id = f"{run_id}_{token}_observe"
            jobs.append(TitanJob(observe_id, script, args=f"{run_id} observe {observe_id} "
                f"{encoded({'turn': turn_index + 1, 'frontier': frontier + 1, 'sources': tool_nodes})}",
                parents=tool_nodes, requirement=args.requirement))
            dag_name = f"BFCL_MT_{case['id'].upper()}_{token.upper()}_{run_id}"
            response = client.submit_dag(dag_name, jobs, agent_run_id=run_id,
                agent_run_name="BFCL Multi-Turn Dynamic Agent",
                agent_stage=f"TURN_{turn_index + 1:02d}_FRONTIER_{frontier + 1:02d}",
                agent_stage_order=stage_order)
            stage_order += 1
            if response != "DAG_ACCEPTED":
                raise RuntimeError(f"{dag_name} rejected: {response}")
            status = wait_for(client, "DAG-" + observe_id, args.timeout)
            if status != "COMPLETED":
                raise RuntimeError(f"{observe_id} ended as {status}")
            tool_results = [load_result(client, run_id, node) for node in tool_nodes]
            for item in tool_results:
                observations.append(item)
                observed_calls.append(item["call"])
                apply_state(state, item["call"], item["output"])
            stages.append({"stage": f"TURN_{turn_index + 1:02d}_FRONTIER_{frontier + 1:02d}",
                "dag": dag_name, "jobs": len(jobs),
                "edges": sum(len(job.parents) for job in jobs), "calls": calls,
                "status": status, "model_request": evidence["request_number"]})
            (args.output / "progress.json").write_text(json.dumps({
                "agent_run_id": run_id, "model": args.model,
                "model_calls": planner.requests, "stages": stages,
                "current_turn": turn_index + 1, "current_frontier": frontier + 1,
                "state": state,
            }, indent=2) + "\n", encoding="utf-8")
        if aborted_reason:
            observed_turns.append({"turn": turn_index + 1,
                "expected": case["ground_truth"][turn_index], "observed": observed_calls,
                "correct": False, "planner_error": aborted_reason})
            break
        if not completed_turn:
            raise RuntimeError(f"turn {turn_index + 1} exceeded frontier limit")
        expected = sorted(map(canonical, case["ground_truth"][turn_index]))
        actual = sorted(map(canonical, observed_calls))
        observed_turns.append({"turn": turn_index + 1, "expected": case["ground_truth"][turn_index],
                               "observed": observed_calls, "correct": actual == expected})
    valid = (all(item["correct"] for item in observed_turns) and len(stages) >= len(case["turns"])
             and planner.requests == len(model_evidence))
    summary = {"schema": "titan-bfcl-multiturn-dynamic-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(), "source": document["source"],
        "case_id": case["id"], "mode": "Gemini-planned runtime frontiers executed as Titan DAG stages",
        "claim_boundary": "Official BFCL inputs and answers; Titan adapter and deterministic tool simulator, not the BFCL harness runtime.",
        "agent_run_id": run_id, "model": args.model, "model_calls": planner.requests,
        "model_call_limit": args.max_model_calls, "stages": stages, "turns": observed_turns,
        "worker_requirement": args.requirement, "aborted_reason": aborted_reason,
        "final_state": state, "wall_seconds": round(time.perf_counter() - started, 3),
        "valid": valid, "agent_url": f"/agents/{run_id}"}
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (args.output / "model-evidence.json").write_text(json.dumps(model_evidence, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    if not valid:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
