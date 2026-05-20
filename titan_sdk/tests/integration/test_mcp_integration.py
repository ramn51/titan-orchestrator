"""
Integration tests for Titan MCP Server.

These tests require a running Titan cluster (Master + at least one Worker).
Start the cluster with: ./titan-dev.sh up

Run with:
    pytest titan_sdk/tests/integration/test_mcp_integration.py -v

Tests are marked with @pytest.mark.integration and are skipped automatically
if the Master is not reachable on TITAN_HOST:TITAN_PORT.
"""

import json
import os
import sys
import time
import socket

import pytest

# ---------------------------------------------------------------------------
# Mock MCP before importing server module
# ---------------------------------------------------------------------------
_passthrough = lambda f: f
_fastmcp_instance = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
_fastmcp_instance.tool.return_value = _passthrough

_mcp_stub = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
_mcp_stub.server = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
_mcp_stub.server.fastmcp = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock(
    FastMCP=__import__("unittest.mock", fromlist=["MagicMock"]).MagicMock(return_value=_fastmcp_instance)
)
sys.modules.setdefault("mcp", _mcp_stub)
sys.modules.setdefault("mcp.server", _mcp_stub.server)
sys.modules.setdefault("mcp.server.fastmcp", _mcp_stub.server.fastmcp)

from titan_sdk.titan_mcp_server import (
    _ping_master_impl,
    _submit_single_job_impl,
    _submit_dag_pipeline_impl,
    _get_job_status_impl,
    _get_job_logs_impl,
    _approve_hitl_gate_impl,
    _store_get_impl,
    _store_put_impl,
)
from titan_sdk.titan_sdk import TitanClient, TITAN_HOST, TITAN_PORT


# ---------------------------------------------------------------------------
# Cluster availability check — skip all tests if Master is not reachable
# ---------------------------------------------------------------------------

def _master_is_up():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((TITAN_HOST, TITAN_PORT))
        s.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _master_is_up(),
    reason=f"Titan Master not reachable at {TITAN_HOST}:{TITAN_PORT} — start cluster with ./titan-dev.sh up",
)


@pytest.fixture(scope="module")
def client():
    return TitanClient()


def _wait_for_terminal(client, job_id, timeout=30):
    """Poll get_job_status until terminal state or timeout. Returns final status.

    Accepts either the raw job_id or the full DAG-prefixed ID — _get_job_status_impl
    handles the prefix internally.
    """
    # DEAD is Titan's internal terminal state for a job that threw an exception
    terminal = {"COMPLETED", "FAILED", "REJECTED", "ERROR", "DEAD"}
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = _get_job_status_impl(client, job_id)
        status = result.get("status", "").upper()
        if status in terminal:
            return status
        time.sleep(1)
    return "TIMEOUT"


# ---------------------------------------------------------------------------
# ping_master
# ---------------------------------------------------------------------------

class TestPingMasterIntegration:

    def test_master_reachable(self):
        result = _ping_master_impl(TITAN_HOST, TITAN_PORT)
        assert result["status"] == "ok"

    def test_wrong_port_returns_error(self):
        result = _ping_master_impl(TITAN_HOST, 19999)
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# TitanStore round-trip
# ---------------------------------------------------------------------------

class TestStoreIntegration:

    def test_put_and_get_round_trip(self, client):
        key = f"mcp:test:integration:{int(time.time())}"
        value = "integration-test-value"
        put_result = _store_put_impl(client, key, value)
        assert put_result["status"] == "ok"

        get_result = _store_get_impl(client, key)
        assert get_result["value"] == value

    def test_get_missing_key_returns_null(self, client):
        key = f"mcp:test:missing:{int(time.time())}"
        result = _store_get_impl(client, key)
        assert result["value"] in ("NULL", None, "")


# ---------------------------------------------------------------------------
# submit_single_job — end to end
# ---------------------------------------------------------------------------

class TestSubmitSingleJobIntegration:

    def test_hello_world_job_completes(self, client):
        job_id = f"mcp-hello-{int(time.time())}"
        result = _submit_single_job_impl(
            client,
            job_id,
            "print('hello from MCP integration test')",
        )
        assert result["status"] == "submitted"

        final_status = _wait_for_terminal(client, job_id, timeout=30)
        assert final_status == "COMPLETED", f"Job ended with: {final_status}"

    def test_failing_job_reaches_failed_state(self, client):
        job_id = f"mcp-fail-{int(time.time())}"
        result = _submit_single_job_impl(
            client,
            job_id,
            "raise RuntimeError('intentional failure for MCP test')",
        )
        assert result["status"] == "submitted"
        final_status = _wait_for_terminal(client, job_id, timeout=30)
        # Titan reports DEAD (not FAILED) for jobs that raise an exception
        assert final_status in ("FAILED", "DEAD"), f"Expected FAILED/DEAD but got: {final_status}"

    def test_job_logs_available_after_completion(self, client):
        job_id = f"mcp-logs-{int(time.time())}"
        sentinel = f"SENTINEL_{job_id}"
        _submit_single_job_impl(client, job_id, f"print('{sentinel}')")
        _wait_for_terminal(client, job_id, timeout=30)

        log_result = _get_job_logs_impl(client, job_id)
        assert sentinel in log_result["logs"]


# ---------------------------------------------------------------------------
# submit_dag_pipeline — end to end
# ---------------------------------------------------------------------------

class TestSubmitDagPipelineIntegration:

    def test_two_job_chain_both_complete(self, client):
        ts = int(time.time())
        job_a = f"mcp-dag-a-{ts}"
        job_b = f"mcp-dag-b-{ts}"

        jobs = [
            {"job_id": job_a, "script_content": "print('job A done')"},
            {"job_id": job_b, "script_content": "print('job B done')", "parents": [job_a]},
        ]
        result = _submit_dag_pipeline_impl(client, f"mcp-chain-{ts}", json.dumps(jobs))
        assert result["status"] == "submitted"
        assert result["job_count"] == 2

        for jid in [job_a, job_b]:
            status = _wait_for_terminal(client, jid, timeout=45)
            assert status == "COMPLETED", f"{jid} ended with: {status}"

    def test_titanstore_kv_handoff_between_jobs(self, client):
        """Job A writes to TitanStore; Job B reads it. Validates cross-job state passing."""
        ts = int(time.time())
        kv_key = f"mcp:dag:handoff:{ts}"
        kv_value = f"payload-{ts}"
        job_a = f"mcp-kv-writer-{ts}"
        job_b = f"mcp-kv-reader-{ts}"

        script_a = f"""
from titan_sdk.titan_sdk import TitanClient
TitanClient().store_put("{kv_key}", "{kv_value}")
print("wrote to TitanStore")
"""
        script_b = f"""
from titan_sdk.titan_sdk import TitanClient
val = TitanClient().store_get("{kv_key}")
assert val == "{kv_value}", f"Expected {kv_value!r} but got {{val!r}}"
print(f"read from TitanStore: {{val}}")
"""
        jobs = [
            {"job_id": job_a, "script_content": script_a},
            {"job_id": job_b, "script_content": script_b, "parents": [job_a]},
        ]
        _submit_dag_pipeline_impl(client, f"mcp-kv-{ts}", json.dumps(jobs))

        for jid in [job_a, job_b]:
            status = _wait_for_terminal(client, jid, timeout=45)
            assert status == "COMPLETED", f"{jid} ended with: {status}"

    def test_gpu_requirement_job_waits_for_capable_worker(self, client):
        """A GPU job submitted with no GPU worker stays PENDING — not FAILED."""
        ts = int(time.time())
        job_id = f"mcp-gpu-pending-{ts}"
        jobs = [{"job_id": job_id, "script_content": "print('gpu job')", "requirement": "GPU"}]
        result = _submit_dag_pipeline_impl(client, f"mcp-gpu-dag-{ts}", json.dumps(jobs))
        assert result["status"] == "submitted"
        # Wait briefly — job should be PENDING (waiting for GPU worker), not FAILED
        time.sleep(3)
        status_result = _get_job_status_impl(client, job_id)
        # NULL = Master not yet aware; PENDING = queued; RUNNING/COMPLETED = GPU worker present
        assert status_result["status"] in ("PENDING", "RUNNING", "COMPLETED", "NULL"), (
            f"GPU job should not FAIL without a worker — got: {status_result['status']}"
        )


# ---------------------------------------------------------------------------
# HITL gate — approve flow
# ---------------------------------------------------------------------------

class TestHitlGateIntegration:

    def test_approve_hitl_gate_unblocks_downstream(self, client):
        """
        DAG: write → [HITL gate] → read
        Approve the gate programmatically and verify downstream completes.
        """
        ts = int(time.time())
        kv_key = f"mcp:hitl:test:{ts}"
        job_write = f"mcp-hitl-write-{ts}"
        job_read = f"mcp-hitl-read-{ts}"
        gate_id = f"hitl-gate-{job_write}"

        script_write = f"""
from titan_sdk.titan_sdk import TitanClient
TitanClient().store_put("{kv_key}", "pre-gate-value")
print("write done")
"""
        script_read = f"""
from titan_sdk.titan_sdk import TitanClient
val = TitanClient().store_get("{kv_key}")
print(f"read: {{val}}")
"""
        jobs = [
            {
                "job_id": job_write,
                "script_content": script_write,
                "hitl_message": "MCP integration test gate — approve to continue",
            },
            {
                "job_id": job_read,
                "script_content": script_read,
                "parents": [job_write],
            },
        ]
        _submit_dag_pipeline_impl(client, f"mcp-hitl-{ts}", json.dumps(jobs))

        # Wait for write job to complete
        write_status = _wait_for_terminal(client, job_write, timeout=30)
        assert write_status == "COMPLETED", f"Write job: {write_status}"

        # Gate should now be blocking — approve it
        _approve_hitl_gate_impl(client, gate_id)

        # Read job should now proceed and complete
        read_status = _wait_for_terminal(client, job_read, timeout=45)
        assert read_status == "COMPLETED", f"Read job after HITL approval: {read_status}"
