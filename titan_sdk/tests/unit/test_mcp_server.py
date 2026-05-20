"""
Unit tests for Titan MCP Server implementation functions.

Tests the _impl functions directly — no MCP framework, no running cluster.
TitanClient._send_request is mocked so all network I/O is bypassed.
"""

import json
import os
import sys
import socket
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Mock the MCP package before importing the server module so tests work
# even if 'mcp' is not installed.
# The @mcp.tool() decorator must be a passthrough so the impl functions
# remain the original callables after decoration.
# ---------------------------------------------------------------------------
_passthrough = lambda f: f
_fastmcp_instance = MagicMock()
_fastmcp_instance.tool.return_value = _passthrough

_mcp_stub = MagicMock()
_mcp_stub.server = MagicMock()
_mcp_stub.server.fastmcp = MagicMock(FastMCP=MagicMock(return_value=_fastmcp_instance))

sys.modules.setdefault("mcp", _mcp_stub)
sys.modules.setdefault("mcp.server", _mcp_stub.server)
sys.modules.setdefault("mcp.server.fastmcp", _mcp_stub.server.fastmcp)

from titan_sdk.titan_mcp_server import (
    _ping_master_impl,
    _submit_single_job_impl,
    _submit_dag_pipeline_impl,
    _submit_yaml_pipeline_impl,
    _get_job_status_impl,
    _get_job_logs_impl,
    _approve_hitl_gate_impl,
    _reject_hitl_gate_impl,
    _store_get_impl,
    _store_put_impl,
    _deploy_script_impl,
)
from titan_sdk.titan_sdk import TitanClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_mock_client(send_response="OK"):
    """Return a TitanClient whose _send_request always returns send_response."""
    client = TitanClient()
    client._send_request = MagicMock(return_value=send_response)
    return client


# ---------------------------------------------------------------------------
# ping_master
# ---------------------------------------------------------------------------

class TestPingMasterImpl:

    def test_returns_ok_when_connectable(self):
        with patch("titan_sdk.titan_mcp_server.socket.socket") as mock_sock_cls:
            mock_sock = MagicMock()
            mock_sock_cls.return_value = mock_sock
            result = _ping_master_impl("127.0.0.1", 9090)

        assert result["status"] == "ok"
        assert result["host"] == "127.0.0.1"
        assert result["port"] == 9090
        mock_sock.connect.assert_called_once_with(("127.0.0.1", 9090))
        mock_sock.close.assert_called_once()

    def test_returns_error_when_unreachable(self):
        with patch("titan_sdk.titan_mcp_server.socket.socket") as mock_sock_cls:
            mock_sock = MagicMock()
            mock_sock.connect.side_effect = ConnectionRefusedError("refused")
            mock_sock_cls.return_value = mock_sock
            result = _ping_master_impl("127.0.0.1", 9090)

        assert result["status"] == "error"
        assert "Cannot connect" in result["message"]

    def test_response_includes_host_and_port_on_error(self):
        with patch("titan_sdk.titan_mcp_server.socket.socket") as mock_sock_cls:
            mock_sock = MagicMock()
            mock_sock.connect.side_effect = OSError("timeout")
            mock_sock_cls.return_value = mock_sock
            result = _ping_master_impl("10.0.0.1", 9090)

        assert result["host"] == "10.0.0.1"
        assert result["port"] == 9090

    def test_sets_3s_timeout(self):
        with patch("titan_sdk.titan_mcp_server.socket.socket") as mock_sock_cls:
            mock_sock = MagicMock()
            mock_sock_cls.return_value = mock_sock
            _ping_master_impl("127.0.0.1", 9090)

        mock_sock.settimeout.assert_called_once_with(3)


# ---------------------------------------------------------------------------
# submit_single_job
# ---------------------------------------------------------------------------

class TestSubmitSingleJobImpl:

    def test_returns_submitted_status(self):
        client = make_mock_client("DAG_ACCEPTED")
        result = _submit_single_job_impl(client, "job1", "print('hello')")
        assert result["status"] == "submitted"

    def test_returns_job_id(self):
        client = make_mock_client("DAG_ACCEPTED")
        result = _submit_single_job_impl(client, "my-job", "pass")
        assert result["job_id"] == "my-job"

    def test_master_response_included(self):
        client = make_mock_client("DAG_ACCEPTED")
        result = _submit_single_job_impl(client, "j1", "pass")
        assert result["master_response"] == "DAG_ACCEPTED"

    def test_calls_submit_job_on_client(self):
        client = make_mock_client()
        client.submit_job = MagicMock(return_value="OK")
        _submit_single_job_impl(client, "j1", "print('x')")
        assert client.submit_job.call_count == 1

    def test_requirement_passed_to_job(self):
        client = make_mock_client()
        captured = []

        def capture_job(job):
            captured.append(job)
            return "OK"

        client.submit_job = capture_job
        _submit_single_job_impl(client, "gpu-job", "pass", requirement="GPU")
        assert captured[0].requirement == "GPU"

    def test_priority_passed_to_job(self):
        client = make_mock_client()
        captured = []
        client.submit_job = lambda job: captured.append(job) or "OK"
        _submit_single_job_impl(client, "j1", "pass", priority=3)
        assert captured[0].priority == 3

    def test_args_passed_to_job(self):
        client = make_mock_client()
        captured = []
        client.submit_job = lambda job: captured.append(job) or "OK"
        _submit_single_job_impl(client, "j1", "pass", args="--mode train")
        assert captured[0].args == "--mode train"

    def test_script_name_is_job_id_dot_py(self):
        """The TitanJob filename basename should be {job_id}.py for clean dashboard display."""
        client = make_mock_client()
        captured = []
        client.submit_job = lambda job: captured.append(job) or "OK"
        _submit_single_job_impl(client, "my-worker", "pass")
        assert os.path.basename(captured[0].filename) == "my-worker.py"

    def test_temp_file_cleaned_up_after_submit(self):
        """Temp script file must not leak to disk after submission."""
        client = make_mock_client()
        captured = []
        client.submit_job = lambda job: captured.append(job) or "OK"
        _submit_single_job_impl(client, "cleanup-test", "pass")
        assert not os.path.exists(captured[0].filename)


# ---------------------------------------------------------------------------
# submit_dag_pipeline
# ---------------------------------------------------------------------------

class TestSubmitDagPipelineImpl:

    def _make_jobs_json(self, jobs):
        return json.dumps(jobs)

    def test_single_job_dag_submitted(self):
        client = make_mock_client()
        client.submit_dag = MagicMock(return_value="ACCEPTED")
        jobs = [{"job_id": "step1", "script_content": "pass"}]
        result = _submit_dag_pipeline_impl(client, "my-dag", self._make_jobs_json(jobs))
        assert result["status"] == "submitted"
        assert result["job_count"] == 1

    def test_dag_name_in_response(self):
        client = make_mock_client()
        client.submit_dag = MagicMock(return_value="OK")
        jobs = [{"job_id": "j1", "script_content": "pass"}]
        result = _submit_dag_pipeline_impl(client, "pipeline-x", self._make_jobs_json(jobs))
        assert result["dag_name"] == "pipeline-x"

    def test_multi_job_dag_count(self):
        client = make_mock_client()
        client.submit_dag = MagicMock(return_value="OK")
        jobs = [
            {"job_id": "a", "script_content": "pass"},
            {"job_id": "b", "script_content": "pass", "parents": ["a"]},
            {"job_id": "c", "script_content": "pass", "parents": ["b"]},
        ]
        result = _submit_dag_pipeline_impl(client, "chain", self._make_jobs_json(jobs))
        assert result["job_count"] == 3

    def test_parent_relationships_preserved(self):
        client = make_mock_client()
        submitted_jobs = []
        client.submit_dag = lambda name, jobs, **kw: submitted_jobs.extend(jobs) or "OK"

        jobs = [
            {"job_id": "root", "script_content": "pass"},
            {"job_id": "child", "script_content": "pass", "parents": ["root"]},
        ]
        _submit_dag_pipeline_impl(client, "dag", self._make_jobs_json(jobs))
        child = next(j for j in submitted_jobs if j.id == "child")
        assert "root" in child.parents

    def test_requirement_per_job(self):
        client = make_mock_client()
        submitted_jobs = []
        client.submit_dag = lambda name, jobs, **kw: submitted_jobs.extend(jobs) or "OK"

        jobs = [
            {"job_id": "cpu-step", "script_content": "pass", "requirement": "GENERAL"},
            {"job_id": "gpu-step", "script_content": "pass", "requirement": "GPU", "parents": ["cpu-step"]},
        ]
        _submit_dag_pipeline_impl(client, "mixed", self._make_jobs_json(jobs))
        gpu_job = next(j for j in submitted_jobs if j.id == "gpu-step")
        assert gpu_job.requirement == "GPU"

    def test_hitl_message_set_on_job(self):
        client = make_mock_client()
        submitted_jobs = []
        client.submit_dag = lambda name, jobs, **kw: submitted_jobs.extend(jobs) or "OK"
        # store_put is called during HITL injection — mock it
        client.store_put = MagicMock(return_value="OK")

        jobs = [{"job_id": "train", "script_content": "pass", "hitl_message": "Approve before deploy"}]
        _submit_dag_pipeline_impl(client, "hitl-dag", self._make_jobs_json(jobs))
        train = next(j for j in submitted_jobs if j.id == "train")
        assert train.hitl_message == "Approve before deploy"

    def test_agent_run_id_forwarded(self):
        client = make_mock_client()
        call_kwargs = {}
        def capture_dag(name, jobs, **kw):
            call_kwargs.update(kw)
            return "OK"
        client.submit_dag = capture_dag

        jobs = [{"job_id": "j1", "script_content": "pass"}]
        _submit_dag_pipeline_impl(client, "dag", self._make_jobs_json(jobs), agent_run_id="run-abc")
        assert call_kwargs.get("agent_run_id") == "run-abc"

    def test_empty_agent_run_id_passed_as_none(self):
        client = make_mock_client()
        call_kwargs = {}
        def capture_dag(name, jobs, **kw):
            call_kwargs.update(kw)
            return "OK"
        client.submit_dag = capture_dag

        jobs = [{"job_id": "j1", "script_content": "pass"}]
        _submit_dag_pipeline_impl(client, "dag", self._make_jobs_json(jobs), agent_run_id="")
        assert call_kwargs.get("agent_run_id") is None

    def test_invalid_json_returns_error(self):
        client = make_mock_client()
        result = _submit_dag_pipeline_impl(client, "dag", "NOT JSON")
        assert result["status"] == "error"
        assert "Invalid jobs_json" in result["message"]

    def test_empty_array_returns_error(self):
        client = make_mock_client()
        result = _submit_dag_pipeline_impl(client, "dag", "[]")
        assert result["status"] == "error"

    def test_missing_job_id_returns_error(self):
        client = make_mock_client()
        jobs = [{"script_content": "pass"}]  # no job_id
        result = _submit_dag_pipeline_impl(client, "dag", json.dumps(jobs))
        assert result["status"] == "error"
        assert "job_id" in result["message"]

    def test_default_requirement_is_general(self):
        client = make_mock_client()
        submitted_jobs = []
        client.submit_dag = lambda name, jobs, **kw: submitted_jobs.extend(jobs) or "OK"
        jobs = [{"job_id": "j1", "script_content": "pass"}]
        _submit_dag_pipeline_impl(client, "dag", json.dumps(jobs))
        assert submitted_jobs[0].requirement == "GENERAL"

    def test_default_priority_is_1(self):
        client = make_mock_client()
        submitted_jobs = []
        client.submit_dag = lambda name, jobs, **kw: submitted_jobs.extend(jobs) or "OK"
        jobs = [{"job_id": "j1", "script_content": "pass"}]
        _submit_dag_pipeline_impl(client, "dag", json.dumps(jobs))
        assert submitted_jobs[0].priority == 1

    def test_temp_files_cleaned_up(self):
        """No temp .py files should leak to disk after submission."""
        client = make_mock_client()
        submitted_jobs = []
        client.submit_dag = lambda name, jobs, **kw: submitted_jobs.extend(jobs) or "OK"
        jobs = [{"job_id": "leak-test", "script_content": "pass"}]
        _submit_dag_pipeline_impl(client, "dag", json.dumps(jobs))
        for job in submitted_jobs:
            assert not os.path.exists(job.filename)


# ---------------------------------------------------------------------------
# submit_yaml_pipeline
# ---------------------------------------------------------------------------

class TestSubmitYamlPipelineImpl:

    def test_missing_yaml_returns_error(self):
        client = make_mock_client()
        result = _submit_yaml_pipeline_impl(client, "/no/such/file.yaml")
        assert result["status"] == "error"
        assert "not found" in result["message"]

    def test_existing_yaml_submitted(self, tmp_path):
        yaml_file = tmp_path / "pipeline.yaml"
        yaml_file.write_text("project: test\njobs:\n  - id: j1\n    script: pass.py\n")
        client = make_mock_client()
        client.submit_yaml = MagicMock(return_value="ACCEPTED")
        result = _submit_yaml_pipeline_impl(client, str(yaml_file))
        assert result["status"] == "submitted"
        client.submit_yaml.assert_called_once()

    def test_yaml_path_in_response(self, tmp_path):
        yaml_file = tmp_path / "p.yaml"
        yaml_file.write_text("project: x\n")
        client = make_mock_client()
        client.submit_yaml = MagicMock(return_value="OK")
        result = _submit_yaml_pipeline_impl(client, str(yaml_file))
        assert result["yaml_path"] == str(yaml_file)

    def test_agent_run_id_forwarded(self, tmp_path):
        yaml_file = tmp_path / "p.yaml"
        yaml_file.write_text("project: x\n")
        client = make_mock_client()
        call_kwargs = {}
        def capture(path, **kw):
            call_kwargs.update(kw)
            return "OK"
        client.submit_yaml = capture
        _submit_yaml_pipeline_impl(client, str(yaml_file), agent_run_id="run-xyz")
        assert call_kwargs.get("agent_run_id") == "run-xyz"

    def test_empty_agent_run_id_passed_as_none(self, tmp_path):
        yaml_file = tmp_path / "p.yaml"
        yaml_file.write_text("project: x\n")
        client = make_mock_client()
        call_kwargs = {}
        client.submit_yaml = lambda path, **kw: call_kwargs.update(kw) or "OK"
        _submit_yaml_pipeline_impl(client, str(yaml_file), agent_run_id="")
        assert call_kwargs.get("agent_run_id") is None


# ---------------------------------------------------------------------------
# get_job_status
# ---------------------------------------------------------------------------

class TestGetJobStatusImpl:

    def test_returns_status_from_client(self):
        client = make_mock_client()
        client.get_job_status = MagicMock(return_value="COMPLETED")
        result = _get_job_status_impl(client, "my-job")
        assert result["status"] == "COMPLETED"

    def test_job_id_in_response_is_original(self):
        """Response job_id should be the original (unprefixed) id passed in."""
        client = make_mock_client()
        client.get_job_status = MagicMock(return_value="RUNNING")
        result = _get_job_status_impl(client, "my-job")
        assert result["job_id"] == "my-job"

    def test_dag_prefix_added_when_missing(self):
        """The Master stores status under DAG-{id} — impl must add prefix before querying."""
        client = make_mock_client()
        queried = []
        client.get_job_status = lambda jid: queried.append(jid) or "COMPLETED"
        _get_job_status_impl(client, "raw-job")
        assert queried[0] == "DAG-raw-job"

    def test_dag_prefix_not_doubled(self):
        """If caller already passes DAG- prefix, it must not be doubled."""
        client = make_mock_client()
        queried = []
        client.get_job_status = lambda jid: queried.append(jid) or "COMPLETED"
        _get_job_status_impl(client, "DAG-raw-job")
        assert queried[0] == "DAG-raw-job"

    def test_none_response_becomes_unknown(self):
        client = make_mock_client()
        client.get_job_status = MagicMock(return_value=None)
        result = _get_job_status_impl(client, "j1")
        assert result["status"] == "UNKNOWN"

    def test_whitespace_stripped_from_status(self):
        client = make_mock_client()
        client.get_job_status = MagicMock(return_value="  FAILED  ")
        result = _get_job_status_impl(client, "j1")
        assert result["status"] == "FAILED"

    @pytest.mark.parametrize("status", ["PENDING", "RUNNING", "COMPLETED", "FAILED", "REJECTED"])
    def test_all_valid_statuses_returned_as_is(self, status):
        client = make_mock_client()
        client.get_job_status = MagicMock(return_value=status)
        result = _get_job_status_impl(client, "j1")
        assert result["status"] == status


# ---------------------------------------------------------------------------
# get_job_logs
# ---------------------------------------------------------------------------

class TestGetJobLogsImpl:

    def test_returns_logs_from_client(self):
        client = make_mock_client()
        client.fetch_logs = MagicMock(return_value="step 1 done\nstep 2 done")
        result = _get_job_logs_impl(client, "j1")
        assert "step 1 done" in result["logs"]

    def test_job_id_in_response_is_original(self):
        client = make_mock_client()
        client.fetch_logs = MagicMock(return_value="output")
        result = _get_job_logs_impl(client, "train-job")
        assert result["job_id"] == "train-job"

    def test_dag_prefix_added_when_missing(self):
        """fetch_logs must query with DAG- prefix so the Master finds the right log."""
        client = make_mock_client()
        queried = []
        client.fetch_logs = lambda jid: queried.append(jid) or "output"
        _get_job_logs_impl(client, "train-job")
        assert queried[0] == "DAG-train-job"

    def test_dag_prefix_not_doubled(self):
        client = make_mock_client()
        queried = []
        client.fetch_logs = lambda jid: queried.append(jid) or "output"
        _get_job_logs_impl(client, "DAG-train-job")
        assert queried[0] == "DAG-train-job"

    def test_none_response_returns_fallback_message(self):
        client = make_mock_client()
        client.fetch_logs = MagicMock(return_value=None)
        result = _get_job_logs_impl(client, "j1")
        assert result["logs"] == "(no logs available)"

    def test_empty_string_returns_fallback_message(self):
        client = make_mock_client()
        client.fetch_logs = MagicMock(return_value="")
        result = _get_job_logs_impl(client, "j1")
        assert result["logs"] == "(no logs available)"


# ---------------------------------------------------------------------------
# approve_hitl_gate / reject_hitl_gate
# ---------------------------------------------------------------------------

class TestHitlGateImpl:

    def test_approve_sets_approved_status(self):
        client = make_mock_client()
        client.store_put = MagicMock(return_value="OK")
        _approve_hitl_gate_impl(client, "hitl-gate-train")
        client.store_put.assert_called_once_with(
            "titan:hitl:status:hitl-gate-train", "APPROVED"
        )

    def test_approve_returns_approved_status(self):
        client = make_mock_client()
        client.store_put = MagicMock(return_value="OK")
        result = _approve_hitl_gate_impl(client, "hitl-gate-train")
        assert result["status"] == "approved"

    def test_approve_returns_gate_id(self):
        client = make_mock_client()
        client.store_put = MagicMock(return_value="OK")
        result = _approve_hitl_gate_impl(client, "hitl-gate-train")
        assert result["gate_id"] == "hitl-gate-train"

    def test_reject_sets_rejected_status(self):
        client = make_mock_client()
        client.store_put = MagicMock(return_value="OK")
        _reject_hitl_gate_impl(client, "hitl-gate-preprocess")
        client.store_put.assert_called_once_with(
            "titan:hitl:status:hitl-gate-preprocess", "REJECTED"
        )

    def test_reject_returns_rejected_status(self):
        client = make_mock_client()
        client.store_put = MagicMock(return_value="OK")
        result = _reject_hitl_gate_impl(client, "hitl-gate-preprocess")
        assert result["status"] == "rejected"

    def test_reject_returns_gate_id(self):
        client = make_mock_client()
        client.store_put = MagicMock(return_value="OK")
        result = _reject_hitl_gate_impl(client, "hitl-gate-preprocess")
        assert result["gate_id"] == "hitl-gate-preprocess"

    def test_approve_key_pattern_uses_gate_id_verbatim(self):
        """Verify the TitanStore key is exactly titan:hitl:status:{gate_id}."""
        client = make_mock_client()
        client.store_put = MagicMock(return_value="OK")
        gate = "hitl-gate-custom-step"
        _approve_hitl_gate_impl(client, gate)
        key_used = client.store_put.call_args[0][0]
        assert key_used == f"titan:hitl:status:{gate}"

    def test_approve_and_reject_use_different_values(self):
        client = make_mock_client()
        client.store_put = MagicMock(return_value="OK")
        _approve_hitl_gate_impl(client, "gate-x")
        approve_value = client.store_put.call_args[0][1]
        _reject_hitl_gate_impl(client, "gate-x")
        reject_value = client.store_put.call_args[0][1]
        assert approve_value == "APPROVED"
        assert reject_value == "REJECTED"
        assert approve_value != reject_value


# ---------------------------------------------------------------------------
# store_get / store_put
# ---------------------------------------------------------------------------

class TestStoreImpl:

    def test_store_get_returns_value(self):
        client = make_mock_client()
        client.store_get = MagicMock(return_value="my-value")
        result = _store_get_impl(client, "my:key")
        assert result["value"] == "my-value"

    def test_store_get_returns_key(self):
        client = make_mock_client()
        client.store_get = MagicMock(return_value="v")
        result = _store_get_impl(client, "some:key")
        assert result["key"] == "some:key"

    def test_store_get_none_returns_null(self):
        client = make_mock_client()
        client.store_get = MagicMock(return_value=None)
        result = _store_get_impl(client, "k")
        assert result["value"] == "NULL"

    def test_store_get_empty_string_returns_null(self):
        client = make_mock_client()
        client.store_get = MagicMock(return_value="")
        result = _store_get_impl(client, "k")
        assert result["value"] == "NULL"

    def test_store_put_returns_ok(self):
        client = make_mock_client()
        client.store_put = MagicMock(return_value="OK")
        result = _store_put_impl(client, "k", "v")
        assert result["status"] == "ok"

    def test_store_put_sends_correct_key(self):
        client = make_mock_client()
        client.store_put = MagicMock(return_value="OK")
        _store_put_impl(client, "result:run1", "done")
        client.store_put.assert_called_once_with("result:run1", "done")

    def test_store_put_key_in_response(self):
        client = make_mock_client()
        client.store_put = MagicMock(return_value="OK")
        result = _store_put_impl(client, "my:key", "val")
        assert result["key"] == "my:key"


# ---------------------------------------------------------------------------
# deploy_script
# ---------------------------------------------------------------------------

class TestDeployScriptImpl:

    def test_rejects_non_py_extension(self):
        client = make_mock_client()
        result = _deploy_script_impl(client, "worker.sh", "#!/bin/bash")
        assert result["status"] == "error"
        assert ".py" in result["message"]

    def test_rejects_no_extension(self):
        client = make_mock_client()
        result = _deploy_script_impl(client, "worker", "pass")
        assert result["status"] == "error"

    def test_deploys_py_script_successfully(self):
        client = make_mock_client()
        client.deploy_script = MagicMock(return_value="DEPLOY_SUCCESS")
        result = _deploy_script_impl(client, "my_worker.py", "print('hello')")
        assert result["status"] == "deployed"

    def test_script_name_in_response(self):
        client = make_mock_client()
        client.deploy_script = MagicMock(return_value="DEPLOY_SUCCESS")
        result = _deploy_script_impl(client, "my_worker.py", "pass")
        assert result["script_name"] == "my_worker.py"

    def test_correct_basename_sent_to_master(self):
        """The file sent to client.deploy_script must have the exact script_name as basename."""
        client = make_mock_client()
        received_paths = []
        client.deploy_script = lambda path: received_paths.append(path) or "DEPLOY_SUCCESS"
        _deploy_script_impl(client, "custom_worker.py", "pass")
        assert os.path.basename(received_paths[0]) == "custom_worker.py"

    def test_temp_file_cleaned_up_after_deploy(self):
        client = make_mock_client()
        received_paths = []
        client.deploy_script = lambda path: received_paths.append(path) or "DEPLOY_SUCCESS"
        _deploy_script_impl(client, "worker.py", "pass")
        assert not os.path.exists(received_paths[0])

    def test_deploy_failure_returns_error_status(self):
        client = make_mock_client()
        client.deploy_script = MagicMock(return_value="ERROR: disk full")
        result = _deploy_script_impl(client, "worker.py", "pass")
        assert result["status"] == "error"

    def test_master_response_included(self):
        client = make_mock_client()
        client.deploy_script = MagicMock(return_value="DEPLOY_SUCCESS")
        result = _deploy_script_impl(client, "w.py", "pass")
        assert result["master_response"] == "DEPLOY_SUCCESS"
