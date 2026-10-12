"""Run a real Gemini-directed, multi-stage Research Agent through Titan."""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
import urllib.request

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
import titan_sdk.titan_sdk as sdk
from titan_sdk.titan_sdk import TitanClient, TitanJob
from titan_test_suite.external_agent_benchmark.bfcl_runner import configure_dashboard_manifest
from titan_test_suite.external_agent_benchmark.runner import wait_for

CATALOG = {
    "dependency-safety": "Concurrent dependency release needs atomic satisfied-parent tracking and exactly-once admission.",
    "failure-recovery": "Worker loss requires redispatch, attempt identity, late-callback rejection, and process-tree cleanup.",
    "observability": "Queue eligibility, dispatch, worker, attempt, result, and failure reason are required to explain execution.",
    "data-integrity": "Cross-stage data should be verified with deterministic filenames, sizes, and content hashes after retries.",
    "service-readiness": "A service deployment is complete only after a bounded network readiness probe succeeds.",
    "human-gates": "Approval and rejection must be durable terminal transitions that propagate to dependent work.",
    "scaling": "Worker and slot scaling must separate useful concurrency from scheduler overhead and saturation churn.",
    "agent-mutation": "Runtime DAG expansion needs idempotent mutation IDs, version checks, and atomic cycle rejection.",
}


def enc(value: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode().rstrip("=")


class Gemini:
    def __init__(self, key: str, model: str):
        self.key = key
        self.model = model.removeprefix("models/")
        self.calls = 0

    def _call(self, prompt: str, json_mode: bool = True, temperature: float = 0.25):
        config = {"temperature": temperature, "maxOutputTokens": 4096}
        if json_mode:
            config["responseMimeType"] = "application/json"
        body = json.dumps({"contents": [{"parts": [{"text": prompt}]}],
                           "generationConfig": config}).encode()
        req = urllib.request.Request(
            f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent",
            data=body, headers={"x-goog-api-key": self.key, "Content-Type": "application/json"},
            method="POST")
        self.calls += 1
        with urllib.request.urlopen(req, timeout=90) as response:
            raw = json.load(response)
        parts = (((raw.get("candidates") or [{}])[0].get("content") or {}).get("parts") or [])
        if not parts or not isinstance(parts[0].get("text"), str):
            raise RuntimeError("Gemini returned no content")
        text = parts[0]["text"]
        return json.loads(text) if json_mode else text

    def plan(self, query: str, seen: list[str]) -> list[str]:
        prompt = f"""Act as a research planner. Query: {query}
Evidence catalog: {json.dumps(CATALOG, indent=2)}
Already researched: {json.dumps(seen)}
Select 3 or 4 distinct catalog keys that form the next useful independent research frontier.
Return JSON only as {{"subtopics":["key", ...]}}."""
        value = self._call(prompt)
        topics = value.get("subtopics") if isinstance(value, dict) else None
        if not isinstance(topics, list) or not 1 <= len(topics) <= 4:
            raise ValueError("planner did not return one to four subtopics")
        topics = list(dict.fromkeys(map(str, topics)))
        if any(x not in CATALOG or x in seen for x in topics):
            raise ValueError("planner selected an unknown or repeated topic")
        return topics

    def evaluate(self, query: str, findings: dict, unseen: list[str]) -> dict:
        prompt = f"""Evaluate whether the collected evidence can answer this query: {query}
Findings: {json.dumps(findings, indent=2)}
Unseen catalog keys: {json.dumps(unseen)}
Return JSON only. Use {{"decision":"SYNTHESIZE","gaps":[],"reason":"..."}} if adequate.
Otherwise use {{"decision":"DEEPEN","gaps":[up to 3 unseen keys],"reason":"..."}}."""
        value = self._call(prompt)
        if not isinstance(value, dict) or value.get("decision") not in {"SYNTHESIZE", "DEEPEN"}:
            raise ValueError("invalid evaluator decision")
        gaps = list(dict.fromkeys(map(str, value.get("gaps", []))))
        if value["decision"] == "DEEPEN" and (not gaps or any(x not in unseen for x in gaps)):
            raise ValueError("invalid evaluator gaps")
        return {"decision": value["decision"], "gaps": gaps[:3], "reason": str(value.get("reason", ""))}

    def synthesize(self, query: str, findings: dict) -> str:
        return self._call(
            f"Write a concise technical research memo answering: {query}\n"
            f"Use only these findings and cite their catalog keys inline: {json.dumps(findings, indent=2)}\n"
            "Include Findings, Risks, and Recommended experiments. Return Markdown.", json_mode=False,
            temperature=0.2)


def read_result(client: TitanClient, run: str, node: str) -> dict:
    raw = client.store_get(f"agent:{run}:{node}")
    if not raw or raw.startswith("ERROR"):
        raise RuntimeError(f"missing result for {node}: {raw}")
    return json.loads(raw)


def submit_stage(client, script, run_id, order, topics, requirement, timeout):
    jobs = []
    for index, topic in enumerate(topics):
        node = f"{run_id}_r{order}_{index}"
        jobs.append(TitanJob(node, str(script), args=f"{run_id} research {node} " + enc({
            "topic": topic, "finding": CATALOG[topic], "source": "fixed benchmark evidence catalog"}),
            requirement=requirement))
    dag = f"RESEARCH_{run_id}_ITER_{order}"
    response = client.submit_dag(dag, jobs, agent_run_id=run_id,
        agent_run_name="Model-Directed Research Agent", agent_stage=f"RESEARCH_{order}",
        agent_stage_order=order)
    if response != "DAG_ACCEPTED":
        raise RuntimeError(f"research frontier rejected: {response}")
    results = {}
    for topic, job in zip(topics, jobs):
        status = wait_for(client, "DAG-" + job.id, timeout)
        if status != "COMPLETED":
            raise RuntimeError(f"{job.id} ended as {status}")
        results[topic] = read_result(client, run_id, job.id)
    return dag, jobs, results


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="127.0.0.1"); p.add_argument("--port", type=int, default=9090)
    p.add_argument("--model", default="gemini-2.5-flash"); p.add_argument("--requirement", default="GENERAL")
    p.add_argument("--timeout", type=float, default=180); p.add_argument("--output", type=Path, required=True)
    p.add_argument("--query", default="How should a distributed runtime safely execute adaptive scientific agent workflows?")
    args = p.parse_args()
    key = os.environ.get("GEMINI_API_KEY", "")
    if not key: raise RuntimeError("GEMINI_API_KEY is required")
    sdk.TITAN_HOST, sdk.TITAN_PORT = args.host, args.port
    configure_dashboard_manifest()
    run_id = "research_" + str(int(time.time()))
    model, client = Gemini(key, args.model), TitanClient()
    script = Path(__file__).with_name("research_dynamic_stage.py")
    findings, seen, stages = {}, [], []
    started = time.perf_counter()
    topics = model.plan(args.query, seen)
    for iteration in range(1, 3):
        dag, jobs, results = submit_stage(client, script, run_id, iteration, topics,
                                          args.requirement, args.timeout)
        findings.update(results); seen.extend(topics)
        unseen = [key for key in CATALOG if key not in seen]
        decision = model.evaluate(args.query, findings, unseen)
        stages.append({"iteration": iteration, "dag": dag, "topics": topics,
                       "jobs": [job.id for job in jobs], "evaluation": decision})
        if decision["decision"] == "SYNTHESIZE" or not unseen:
            break
        topics = decision["gaps"]
    report = model.synthesize(args.query, findings)
    node = f"{run_id}_synthesis"
    publish = TitanJob(node, str(script), args=f"{run_id} publish {node} " + enc({"report": report}),
                       requirement=args.requirement)
    dag = f"RESEARCH_{run_id}_SYNTHESIS"
    response = client.submit_dag(dag, [publish], agent_run_id=run_id,
        agent_run_name="Model-Directed Research Agent", agent_stage="SYNTHESIS",
        agent_stage_order=len(stages) + 1)
    if response != "DAG_ACCEPTED": raise RuntimeError("synthesis rejected: " + response)
    status = wait_for(client, "DAG-" + node, args.timeout)
    if status != "COMPLETED": raise RuntimeError(f"synthesis ended as {status}")
    result = read_result(client, run_id, node)
    summary = {"schema": "titan-research-agent-v1", "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "Gemini plans and evaluates frontiers; Titan executes every frontier on GENERAL workers",
        "agent_run_id": run_id, "query": args.query, "model": args.model,
        "model_calls": model.calls, "stages": stages + [{"dag": dag, "jobs": [node], "artifact": result}],
        "topics_researched": seen, "worker_requirement": args.requirement,
        "wall_seconds": round(time.perf_counter() - started, 3), "valid": True,
        "report_markdown": report}
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (args.output / "research-report.md").write_text(report, encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__": main()

