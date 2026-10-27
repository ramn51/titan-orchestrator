"""End-to-end MCP smoke campaign against a running Titan cluster.

This script drives the stdio MCP server as a real client and writes one JSON
evidence file. It creates only uniquely named jobs and user:mcp: keys.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


class McpClient:
    def __init__(self, cwd: Path, workspace: Path):
        environment = os.environ.copy()
        environment.update({
            "TITAN_MCP_OPERATOR": "true",
            "TITAN_MCP_ALLOWED_ROOTS": str(workspace),
        })
        self.process = subprocess.Popen(
            [sys.executable, "-m", "titan_mcp"], cwd=str(cwd), env=environment,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )
        self.next_id = 1
        self.request("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                                    "clientInfo": {"name": "titan-mcp-live-smoke", "version": "1"}})
        self.notify("notifications/initialized", {})

    def notify(self, method, params):
        self.process.stdin.write(json.dumps({"jsonrpc": "2.0", "method": method, "params": params}) + "\n")
        self.process.stdin.flush()

    def request(self, method, params):
        request_id = self.next_id
        self.next_id += 1
        self.process.stdin.write(json.dumps({"jsonrpc": "2.0", "id": request_id,
                                             "method": method, "params": params}) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError("MCP server exited: " + self.process.stderr.read())
        response = json.loads(line)
        if response.get("id") != request_id or "error" in response:
            raise RuntimeError(f"Invalid MCP response: {response}")
        return response["result"]

    def tool(self, name, arguments=None):
        result = self.request("tools/call", {"name": name, "arguments": arguments or {}})
        if result.get("isError"):
            raise RuntimeError(f"{name}: {result['structuredContent']}")
        return result["structuredContent"]

    def try_tool(self, name, arguments=None):
        return self.request("tools/call", {"name": name, "arguments": arguments or {}})

    def close(self):
        if self.process.stdin:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()


def wait_status(client, job_id, wanted, timeout=90):
    deadline = time.monotonic() + timeout
    seen = []
    while time.monotonic() < deadline:
        status = client.tool("titan_get_job", {"job_id": job_id})["status"]
        if not seen or seen[-1] != status:
            seen.append(status)
        if status in wanted:
            return status, seen
        time.sleep(0.25)
    raise TimeoutError(f"{job_id} did not reach {wanted}; transitions={seen}")


def run(repo: Path, output: Path) -> dict:
    stamp = str(int(time.time()))
    workspace = repo / "work" / ("mcp-live-" + stamp)
    workspace.mkdir(parents=True, exist_ok=False)
    (workspace / "root.py").write_text("print('mcp-root')\n", encoding="utf-8")
    (workspace / "child.py").write_text("print('mcp-child')\n", encoding="utf-8")
    (workspace / "long.py").write_text("import time\nprint('started', flush=True)\ntime.sleep(30)\n", encoding="utf-8")
    (workspace / "fail.py").write_text("print('intentional failure', flush=True)\nraise SystemExit(3)\n", encoding="utf-8")
    service_port = 19000 + int(stamp[-3:]) % 500
    (workspace / "service.py").write_text(
        f"from http.server import ThreadingHTTPServer,SimpleHTTPRequestHandler\n"
        f"ThreadingHTTPServer(('0.0.0.0',{service_port}),SimpleHTTPRequestHandler).serve_forever()\n",
        encoding="utf-8")
    asset = workspace / "asset.txt"
    asset.write_text("titan-mcp-asset-" + stamp, encoding="utf-8")
    evidence = {"started_at": int(time.time() * 1000), "stamp": stamp, "checks": {}}
    client = McpClient(repo, workspace)
    service_id = None
    try:
        tools = client.request("tools/list", {})["tools"]
        evidence["checks"]["tool_count"] = len(tools)
        if len(tools) != 30:
            raise RuntimeError(f"Expected 30 tools, received {len(tools)}")
        cluster = client.tool("titan_cluster_status")
        if cluster["active_workers"] < 1:
            raise RuntimeError("Live smoke requires at least one worker")
        evidence["checks"]["workers"] = cluster["active_workers"]

        jobs = [
            {"id": "mcp_root_" + stamp, "script_path": str(workspace / "root.py")},
            {"id": "mcp_child_" + stamp, "script_path": str(workspace / "child.py"),
             "parents": ["mcp_root_" + stamp]},
        ]
        validation = client.tool("titan_validate_dag", {"name": "mcp_chain_" + stamp, "jobs": jobs})
        if not validation["valid"]:
            raise RuntimeError(f"Live DAG did not pass dry-run validation: {validation}")
        capabilities = client.tool("titan_get_capabilities")
        if "GENERAL" not in capabilities["capabilities"]:
            raise RuntimeError("Live worker did not advertise GENERAL capability")
        evidence["checks"]["validation_and_capabilities"] = {
            "validation": validation, "capabilities": capabilities,
        }
        submitted = client.tool("titan_submit_dag", {
            "name": "mcp_chain_" + stamp, "jobs": jobs,
            "agent": {"run_id": "mcp-agent-" + stamp, "run_name": "MCP live smoke",
                      "stage": "CHAIN", "stage_order": 0},
        })
        final_id = "DAG-mcp_child_" + stamp
        waited = client.tool("titan_wait_for_jobs", {"job_ids": [final_id], "timeout_seconds": 30})
        status = waited["states"][final_id]
        transitions = waited["transitions"][final_id]
        if status != "COMPLETED":
            raise RuntimeError(f"Chain failed with {status}")
        evidence["checks"]["chain"] = {"submission": submitted, "transitions": transitions,
                                               "logs": client.tool("titan_get_job_logs", {"job_id": final_id})}
        dag = client.tool("titan_get_dag", {"name": "mcp_chain_" + stamp})
        if dag["status"] != "COMPLETED" or dag["total"] != 2:
            raise RuntimeError(f"DAG projection mismatch: {dag}")
        runs = client.tool("titan_list_agent_runs")
        run = client.tool("titan_get_agent_run", {"run_id": "mcp-agent-" + stamp})
        if run["status"] != "COMPLETED" or not any(item["id"] == run["id"] for item in runs["runs"]):
            raise RuntimeError("Agent-run projection mismatch")
        evidence["checks"]["dag_and_agent_projection"] = {"dag": dag, "agent_run": run}

        client.tool("titan_kv_put", {"key": "smoke:" + stamp, "value": "roundtrip"})
        kv = client.tool("titan_kv_get", {"key": "smoke:" + stamp})["value"]
        if kv != "roundtrip":
            raise RuntimeError("KV roundtrip mismatch")
        evidence["checks"]["kv"] = "PASS"

        client.tool("titan_upload_asset", {"path": str(asset)})
        fetched = workspace / "fetched.txt"
        client.tool("titan_fetch_artifact", {"name": asset.name, "destination": str(fetched)})
        original_hash = hashlib.sha256(asset.read_bytes()).hexdigest()
        fetched_hash = hashlib.sha256(fetched.read_bytes()).hexdigest()
        if original_hash != fetched_hash:
            raise RuntimeError("Artifact SHA-256 mismatch")
        evidence["checks"]["artifact"] = {"sha256": original_hash, "match": True}

        long_name = "mcp_long_" + stamp
        client.tool("titan_submit_job", {"job": {"id": long_name, "script_path": str(workspace / "long.py")}})
        wait_status(client, "DAG-" + long_name, {"RUNNING"}, timeout=30)
        client.tool("titan_cancel_job", {"job_id": "DAG-" + long_name})
        cancelled, transitions = wait_status(client, "DAG-" + long_name, {"CANCELLED", "FAILED"}, timeout=30)
        if cancelled != "CANCELLED":
            raise RuntimeError(f"Cancellation ended as {cancelled}")
        evidence["checks"]["cancellation"] = transitions

        review = "mcp_review_" + stamp
        after = "mcp_after_review_" + stamp
        client.tool("titan_submit_dag", {"name": "mcp_hitl_" + stamp, "jobs": [
            {"id": review, "script_path": str(workspace / "root.py"),
             "hitl_message": "Approve MCP smoke continuation", "max_wait_seconds": 60},
            {"id": after, "script_path": str(workspace / "child.py"), "parents": [review]},
        ]})
        gate = "DAG-hitl-gate-" + review
        wait_status(client, gate, {"RUNNING"}, timeout=30)
        approval_deadline = time.monotonic() + 15
        while True:
            pending = client.tool("titan_list_pending_approvals")
            if any(item["id"] == gate for item in pending["approvals"]):
                break
            if time.monotonic() >= approval_deadline:
                raise RuntimeError(f"HITL gate did not appear in pending approvals: {pending}")
            time.sleep(0.25)
        decision_deadline = time.monotonic() + 15
        while True:
            decision = client.try_tool("titan_hitl_decide", {"gate_id": gate, "decision": "APPROVED"})
            if not decision.get("isError"):
                break
            if time.monotonic() >= decision_deadline:
                raise RuntimeError(f"HITL gate did not become decidable: {decision['structuredContent']}")
            time.sleep(0.25)
        final, _ = wait_status(client, "DAG-" + after, {"COMPLETED", "FAILED", "DEAD"})
        if final != "COMPLETED":
            raise RuntimeError(f"HITL downstream ended as {final}")
        evidence["checks"]["hitl"] = "PASS"

        failed_name = "mcp_failed_" + stamp
        client.tool("titan_submit_job", {"job": {"id": failed_name,
                    "script_path": str(workspace / "fail.py")}})
        failed, _ = wait_status(client, "DAG-" + failed_name, {"FAILED", "DEAD"}, timeout=30)
        failures = client.tool("titan_list_failures", {"since_ms": evidence["started_at"], "limit": 50})
        if not any(item.get("job_id") == "DAG-" + failed_name for item in failures["failures"]):
            raise RuntimeError(f"Intentional failure missing from failure summary: {failures}")
        evidence["checks"]["failure_summary"] = {"terminal": failed, "summary": failures}

        service_name = "mcp_service_" + stamp
        service_id = "DAG-" + service_name
        client.tool("titan_submit_job", {"job": {"id": service_name,
                    "script_path": str(workspace / "service.py"), "kind": "service", "port": service_port}})
        ready, _ = wait_status(client, service_id, {"COMPLETED", "FAILED"}, timeout=45)
        if ready != "COMPLETED":
            raise RuntimeError("Service readiness failed")
        endpoint = client.tool("titan_resolve_service", {"service_id": service_id})["endpoint"]
        stopped = client.tool("titan_stop_service", {"service_id": service_id})["response"]
        evidence["checks"]["service"] = {"endpoint": endpoint, "stop": stopped}
        service_id = None

        timeline = client.tool("titan_get_timeline", {"since_ms": evidence["started_at"], "limit": 100})
        serialized = json.dumps(timeline)
        if final_id not in serialized:
            raise RuntimeError("Completed MCP chain is absent from execution timeline")
        evidence["checks"]["timeline"] = "PASS"
        evidence["result"] = "PASS"
        return evidence
    finally:
        if service_id:
            try:
                client.tool("titan_stop_service", {"service_id": service_id})
            except Exception:
                pass
        evidence["ended_at"] = int(time.time() * 1000)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(evidence, indent=2, sort_keys=True), encoding="utf-8")
        client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    evidence = run(args.repo.resolve(), args.output.resolve())
    print(json.dumps({"result": evidence["result"], "output": str(args.output.resolve())}))


if __name__ == "__main__":
    main()
