import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from titan_mcp.config import McpConfig
from titan_mcp.server import TitanMcpServer
from titan_mcp.tools import ToolCatalog


class FakeGateway:
    def __init__(self, config):
        self.config = config
        self.submitted = []

    def stats(self, view="system"):
        if view.startswith("queue"):
            return {"counts": {"ready": 1}, "jobs": [{"id": "DAG-a"}]}
        if view.startswith("timeline"):
            return [{"job_id": "DAG-failed", "status": "FAILED", "reason": "boom",
                     "started_at": 10, "ended_at": 20},
                    {"job_id": "DAG-a", "status": "COMPLETED", "started_at": 1, "ended_at": 2}]
        if view.startswith("metrics"):
            return {"samples": [{"queue_depth": 0}]}
        if view.startswith("history"):
            return {"spans": []}
        return {"active_workers": 1, "queue_size": 0,
                "workers": [{"port": 8080, "load": "1/4", "capabilities": "[GENERAL, GPU]",
                             "services": ["DAG-api"]}],
                "dag_jobs": {"DAG-a": {"status": "COMPLETED", "worker": "worker:8080"},
                             "DAG-hitl-gate-a": {"status": "RUNNING"}},
                "execution_spans": [{"job_id": "DAG-a", "status": "COMPLETED"}]}

    def dag_records(self):
        return {"agent_stage": {"name": "agent_stage", "run_ts": 100, "jobs": {
            "DAG-a": {"dag": "agent_stage", "deps": [],
                      "agent_run_id": "agent-1", "agent_run_name": "Agent one",
                      "agent_stage": "PLAN", "agent_stage_order": 0,
                      "spec": {"script": "job.py"}},
            "DAG-hitl-gate-a": {"dag": "agent_stage", "deps": ["DAG-a"],
                                "agent_run_id": "agent-1", "agent_run_name": "Agent one",
                                "agent_stage": "PLAN", "agent_stage_order": 0,
                                "spec": {"hitl_message": "Review output"}},
        }}}

    def hitl_status(self, gate_id): return "WAITING"

    def status(self, job_id): return "COMPLETED"
    def logs(self, job_id): return "ok\n"
    def submit_job(self, spec): self.submitted.append(spec); return "DAG_ACCEPTED"
    def submit_dag(self, name, specs, agent): self.submitted.extend(specs); return "DAG_ACCEPTED"
    def cancel(self, job_id): return "CANCELLED"
    def resolve_service(self, service_id): return "127.0.0.1|9000"
    def stop_service(self, service_id): return "STOPPED"
    def hitl_decide(self, gate_id, decision): return decision
    def kv_get(self, key): return "value"
    def kv_put(self, key, value): return "OK"
    def kv_add(self, key, member): return 1
    def kv_members(self, key): return ["a"]
    def upload_asset(self, path): return "ASSET_UPLOADED"
    def fetch_artifact(self, name, destination): Path(destination).write_text("data"); return destination


class TitanMcpTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name).resolve()
        self.script = root / "job.py"
        self.script.write_text("print('ok')\n", encoding="utf-8")
        self.read_only = McpConfig("127.0.0.1", 9090, False, (root,), "user:mcp:", 10000, 10000)
        self.operator = McpConfig("127.0.0.1", 9090, True, (root,), "user:mcp:", 10000, 10000)

    def tearDown(self):
        self.temp.cleanup()

    def server(self, config=None):
        config = config or self.read_only
        server = TitanMcpServer(ToolCatalog(FakeGateway(config), config))
        server.handle({"jsonrpc": "2.0", "id": "init", "method": "initialize",
                       "params": {"protocolVersion": "2025-11-25"}})
        server.handle({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return server

    def call(self, server, request_id, method, params=None):
        return server.handle({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}})

    def test_initialize_and_tools_are_deterministic(self):
        server = TitanMcpServer(ToolCatalog(FakeGateway(self.read_only), self.read_only))
        initialized = self.call(server, 1, "initialize", {"protocolVersion": "2025-11-25"})
        self.assertEqual("2025-11-25", initialized["result"]["protocolVersion"])
        premature = self.call(server, 9, "tools/list")
        self.assertEqual(-32002, premature["error"]["code"])
        server.handle({"jsonrpc": "2.0", "method": "notifications/initialized"})
        first = self.call(server, 2, "tools/list")["result"]["tools"]
        second = self.call(server, 3, "tools/list")["result"]["tools"]
        self.assertEqual(first, second)
        self.assertEqual(sorted(item["name"] for item in first), [item["name"] for item in first])
        self.assertEqual(30, len(first))

    def test_read_only_profile_observes_but_rejects_mutation(self):
        server = self.server()
        observed = self.call(server, 1, "tools/call", {"name": "titan_cluster_status", "arguments": {}})
        self.assertFalse(observed["result"]["isError"])
        rejected = self.call(server, 2, "tools/call", {"name": "titan_cancel_job", "arguments": {"job_id": "DAG-a"}})
        self.assertTrue(rejected["result"]["isError"])
        self.assertIn("TITAN_MCP_OPERATOR", rejected["result"]["structuredContent"]["message"])

    def test_operator_can_stop_service_with_existing_opcode_gateway(self):
        server = self.server(self.operator)
        stopped = self.call(server, 2, "tools/call", {
            "name": "titan_stop_service", "arguments": {"service_id": "DAG-service"}
        })
        self.assertFalse(stopped["result"]["isError"])
        self.assertEqual("STOPPED", stopped["result"]["structuredContent"]["response"])

    def test_services_and_job_counts_are_derived_from_existing_stats(self):
        server = self.server()
        status = self.call(server, 1, "tools/call", {"name": "titan_cluster_status", "arguments": {}})
        body = status["result"]["structuredContent"]
        self.assertEqual({"COMPLETED": 1, "RUNNING": 1}, body["job_counts"])
        self.assertEqual([{"id": "DAG-api", "worker_port": 8080}], body["services"])
        services = self.call(server, 2, "tools/call", {"name": "titan_list_services", "arguments": {}})
        self.assertEqual(1, services["result"]["structuredContent"]["count"])

    def test_dag_agent_approval_and_capability_projections(self):
        server = self.server()
        dag = self.call(server, 1, "tools/call", {
            "name": "titan_get_dag", "arguments": {"name": "agent_stage"}
        })["result"]["structuredContent"]
        self.assertEqual(2, dag["total"])
        self.assertEqual(["DAG-a"], dag["roots"])
        self.assertEqual(["DAG-hitl-gate-a"], dag["leaves"])
        runs = self.call(server, 2, "tools/call", {
            "name": "titan_list_agent_runs", "arguments": {}
        })["result"]["structuredContent"]
        self.assertEqual("agent-1", runs["runs"][0]["id"])
        run = self.call(server, 3, "tools/call", {
            "name": "titan_get_agent_run", "arguments": {"run_id": "agent-1"}
        })["result"]["structuredContent"]
        self.assertEqual("PLAN", run["stages"][0]["label"])
        self.assertEqual(1, len(run["dags"]))
        approvals = self.call(server, 4, "tools/call", {
            "name": "titan_list_pending_approvals", "arguments": {}
        })["result"]["structuredContent"]
        self.assertEqual("Review output", approvals["approvals"][0]["message"])
        capabilities = self.call(server, 5, "tools/call", {
            "name": "titan_get_capabilities", "arguments": {}
        })["result"]["structuredContent"]
        self.assertEqual({"workers": 1, "slots": 4, "free_slots": 3},
                         capabilities["capabilities"]["GPU"])

    def test_validate_wait_and_failure_summary(self):
        server = self.server()
        valid_jobs = [
            {"id": "root", "script_path": str(self.script)},
            {"id": "child", "script_path": str(self.script), "parents": ["root"]},
        ]
        validation = self.call(server, 1, "tools/call", {
            "name": "titan_validate_dag", "arguments": {"name": "valid", "jobs": valid_jobs}
        })["result"]["structuredContent"]
        self.assertTrue(validation["valid"])
        self.assertEqual(1, validation["edges"])
        cyclic = [
            {"id": "a", "script_path": str(self.script), "parents": ["b"]},
            {"id": "b", "script_path": str(self.script), "parents": ["a"]},
        ]
        validation = self.call(server, 2, "tools/call", {
            "name": "titan_validate_dag", "arguments": {"name": "cycle", "jobs": cyclic}
        })["result"]["structuredContent"]
        self.assertFalse(validation["valid"])
        self.assertIn("DAG contains a dependency cycle", validation["errors"])
        waited = self.call(server, 3, "tools/call", {
            "name": "titan_wait_for_jobs", "arguments": {"job_ids": ["DAG-a"], "timeout_seconds": 1}
        })["result"]["structuredContent"]
        self.assertFalse(waited["timed_out"])
        self.assertEqual(["DAG-a"], waited["terminal"])

    def test_validate_large_chain(self):
        server = self.server()
        jobs = []
        for index in range(2000):
            job = {"id": f"node-{index}", "script_path": str(self.script)}
            if index:
                job["parents"] = [f"node-{index - 1}"]
            jobs.append(job)
        validation = self.call(server, 1, "tools/call", {
            "name": "titan_validate_dag",
            "arguments": {"name": "large-chain", "jobs": jobs},
        })["result"]["structuredContent"]
        self.assertTrue(validation["valid"])
        self.assertEqual(2000, validation["jobs"])
        self.assertEqual(1999, validation["edges"])
        failures = self.call(server, 4, "tools/call", {
            "name": "titan_list_failures", "arguments": {"limit": 10}
        })["result"]["structuredContent"]
        self.assertEqual("DAG-failed", failures["failures"][0]["job_id"])

    def test_operator_submits_validated_dag(self):
        server = self.server(self.operator)
        jobs = [
            {"id": "root", "script_path": str(self.script)},
            {"id": "child", "script_path": str(self.script), "parents": ["root"]},
        ]
        result = self.call(server, 1, "tools/call", {"name": "titan_submit_dag", "arguments": {"name": "mcp-test", "jobs": jobs}})
        self.assertFalse(result["result"]["isError"])
        self.assertEqual(2, result["result"]["structuredContent"]["jobs"])

    def test_unknown_parent_is_rejected_before_titan(self):
        server = self.server(self.operator)
        jobs = [{"id": "child", "script_path": str(self.script), "parents": ["missing"]}]
        result = self.call(server, 1, "tools/call", {"name": "titan_submit_dag", "arguments": {"name": "bad", "jobs": jobs}})
        self.assertTrue(result["result"]["isError"])
        self.assertIn("unknown parents", result["result"]["structuredContent"]["message"])

    def test_nested_schema_rejects_unknown_fields_and_wrong_types(self):
        server = self.server(self.operator)
        unknown = {"id": "a", "script_path": str(self.script), "shell": "rm -rf /"}
        result = self.call(server, 1, "tools/call", {"name": "titan_submit_job", "arguments": {"job": unknown}})
        self.assertTrue(result["result"]["isError"])
        self.assertIn("unknown fields", result["result"]["structuredContent"]["message"])
        wrong = {"id": "a", "script_path": str(self.script), "priority": "high"}
        result = self.call(server, 2, "tools/call", {"name": "titan_submit_job", "arguments": {"job": wrong}})
        self.assertTrue(result["result"]["isError"])
        self.assertIn("must be an integer", result["result"]["structuredContent"]["message"])

    def test_paths_cannot_escape_allowed_root(self):
        outside = Path(self.temp.name).parent / "outside.py"
        outside.write_text("print('outside')", encoding="utf-8")
        try:
            with self.assertRaises(PermissionError):
                self.operator.resolve_read_path(str(outside), 10000)
        finally:
            outside.unlink(missing_ok=True)

    def test_kv_namespace_blocks_delimiter(self):
        self.assertEqual("user:mcp:key", self.operator.namespaced_key("key"))
        with self.assertRaises(ValueError):
            self.operator.namespaced_key("system:active|jobs")

    def test_internal_kv_prefix_cannot_be_configured(self):
        old = os.environ.get("TITAN_MCP_KV_PREFIX")
        os.environ["TITAN_MCP_KV_PREFIX"] = "system:"
        try:
            with self.assertRaisesRegex(ValueError, "user: namespace"):
                McpConfig.from_env()
        finally:
            if old is None:
                os.environ.pop("TITAN_MCP_KV_PREFIX", None)
            else:
                os.environ["TITAN_MCP_KV_PREFIX"] = old

    def test_parse_and_method_errors_do_not_crash_server(self):
        server = self.server()
        bad = server.handle([])
        self.assertEqual(-32600, bad["error"]["code"])
        unknown = self.call(server, 2, "not/a/method")
        self.assertEqual(-32601, unknown["error"]["code"])
        self.assertIsNone(server.handle({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        invalid_params = server.handle({"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": []})
        self.assertEqual(-32602, invalid_params["error"]["code"])

    def test_stdio_wire_contains_only_json_rpc(self):
        environment = os.environ.copy()
        environment["TITAN_MCP_ALLOWED_ROOTS"] = self.temp.name
        process = subprocess.run(
            [sys.executable, "-m", "titan_mcp"],
            cwd=str(Path(__file__).resolve().parents[2]),
            env=environment,
            input=(
                '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}\n'
                '{"jsonrpc":"2.0","method":"notifications/initialized"}\n'
                '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}\n'
            ),
            text=True,
            capture_output=True,
            timeout=5,
            check=True,
        )
        lines = [json.loads(line) for line in process.stdout.splitlines()]
        self.assertEqual([1, 2], [line["id"] for line in lines])
        self.assertIn("serverInfo", lines[0]["result"])
        self.assertGreaterEqual(len(lines[1]["result"]["tools"]), 21)


if __name__ == "__main__":
    unittest.main()
