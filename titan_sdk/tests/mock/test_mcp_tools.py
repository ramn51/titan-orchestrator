"""
Mock-based tests for Titan MCP Server tool functions.

Patches TitanClient._send_request so all network I/O is bypassed.
Verifies that each MCP tool calls the correct SDK operations with
the expected opcode and payload formats.
"""

import json
import sys
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Mock the MCP package before importing the server module
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
    _approve_hitl_gate_impl,
    _reject_hitl_gate_impl,
    _store_get_impl,
    _store_put_impl,
    _get_job_status_impl,
    _get_job_logs_impl,
)
from titan_sdk.titan_sdk import (
    TitanClient,
    OP_KV_SET,
    OP_KV_GET,
    OP_GET_JOB_STATUS,
    OP_GET_LOGS,
)


# ---------------------------------------------------------------------------
# Shared mock_send fixture (mirrors existing test pattern)
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_send(monkeypatch):
    class MockSend:
        def __init__(self):
            self.calls = []
            self._response = "OK"

        def set_response(self, r):
            self._response = r

        def __call__(self, op, payload):
            self.calls.append((op, payload))
            return self._response

    m = MockSend()
    monkeypatch.setattr(TitanClient, "_send_request", lambda self, op, payload: m(op, payload))
    return m


@pytest.fixture
def client():
    return TitanClient()


# ---------------------------------------------------------------------------
# HITL gate approval uses OP_KV_SET with correct key and value
# ---------------------------------------------------------------------------

class TestHitlGateMockOpcodes:

    def test_approve_uses_kv_set_opcode(self, client, mock_send):
        _approve_hitl_gate_impl(client, "hitl-gate-train")
        assert mock_send.calls[0][0] == OP_KV_SET

    def test_approve_payload_key_format(self, client, mock_send):
        _approve_hitl_gate_impl(client, "hitl-gate-train")
        assert mock_send.calls[0][1] == "titan:hitl:status:hitl-gate-train|APPROVED"

    def test_reject_uses_kv_set_opcode(self, client, mock_send):
        _reject_hitl_gate_impl(client, "hitl-gate-preprocess")
        assert mock_send.calls[0][0] == OP_KV_SET

    def test_reject_payload_key_format(self, client, mock_send):
        _reject_hitl_gate_impl(client, "hitl-gate-preprocess")
        assert mock_send.calls[0][1] == "titan:hitl:status:hitl-gate-preprocess|REJECTED"

    def test_approve_does_not_call_reject(self, client, mock_send):
        _approve_hitl_gate_impl(client, "hitl-gate-x")
        payload = mock_send.calls[0][1]
        assert "REJECTED" not in payload

    def test_reject_does_not_call_approve(self, client, mock_send):
        _reject_hitl_gate_impl(client, "hitl-gate-x")
        payload = mock_send.calls[0][1]
        assert "APPROVED" not in payload

    def test_approve_exactly_one_network_call(self, client, mock_send):
        _approve_hitl_gate_impl(client, "hitl-gate-x")
        assert len(mock_send.calls) == 1

    def test_reject_exactly_one_network_call(self, client, mock_send):
        _reject_hitl_gate_impl(client, "hitl-gate-x")
        assert len(mock_send.calls) == 1


# ---------------------------------------------------------------------------
# TitanStore operations use correct opcodes
# ---------------------------------------------------------------------------

class TestStoreGetMockOpcodes:

    def test_store_get_uses_kv_get_opcode(self, client, mock_send):
        _store_get_impl(client, "my:key")
        assert mock_send.calls[0][0] == OP_KV_GET

    def test_store_get_sends_key_as_payload(self, client, mock_send):
        _store_get_impl(client, "agent:run:result")
        assert mock_send.calls[0][1] == "agent:run:result"

    def test_store_get_returns_master_value(self, client, mock_send):
        mock_send.set_response("my-stored-value")
        result = _store_get_impl(client, "k")
        assert result["value"] == "my-stored-value"


class TestStorePutMockOpcodes:

    def test_store_put_uses_kv_set_opcode(self, client, mock_send):
        _store_put_impl(client, "k", "v")
        assert mock_send.calls[0][0] == OP_KV_SET

    def test_store_put_payload_format(self, client, mock_send):
        _store_put_impl(client, "result:run1", "complete")
        assert mock_send.calls[0][1] == "result:run1|complete"

    def test_store_put_master_response_included(self, client, mock_send):
        mock_send.set_response("SET_OK")
        result = _store_put_impl(client, "k", "v")
        assert result["master_response"] == "SET_OK"


# ---------------------------------------------------------------------------
# get_job_status uses OP_GET_JOB_STATUS
# ---------------------------------------------------------------------------

class TestGetJobStatusMockOpcodes:

    def test_uses_get_job_status_opcode(self, client, mock_send):
        mock_send.set_response("COMPLETED")
        _get_job_status_impl(client, "my-job")
        assert mock_send.calls[0][0] == OP_GET_JOB_STATUS

    def test_sends_dag_prefixed_id_as_payload(self, client, mock_send):
        """Master expects DAG-{job_id} — impl must add prefix before querying."""
        mock_send.set_response("RUNNING")
        _get_job_status_impl(client, "train-step")
        assert mock_send.calls[0][1] == "DAG-train-step"

    def test_does_not_double_prefix(self, client, mock_send):
        mock_send.set_response("RUNNING")
        _get_job_status_impl(client, "DAG-train-step")
        assert mock_send.calls[0][1] == "DAG-train-step"

    def test_returns_status_from_master(self, client, mock_send):
        mock_send.set_response("FAILED")
        result = _get_job_status_impl(client, "j1")
        assert result["status"] == "FAILED"


# ---------------------------------------------------------------------------
# get_job_logs uses OP_GET_LOGS
# ---------------------------------------------------------------------------

class TestGetJobLogsMockOpcodes:

    def test_uses_get_logs_opcode(self, client, mock_send):
        mock_send.set_response("line1\nline2")
        _get_job_logs_impl(client, "my-job")
        assert mock_send.calls[0][0] == OP_GET_LOGS

    def test_sends_dag_prefixed_id_as_payload(self, client, mock_send):
        """Master stores logs under DAG-{job_id} — impl must add prefix."""
        mock_send.set_response("output")
        _get_job_logs_impl(client, "gpu-train")
        assert mock_send.calls[0][1] == "DAG-gpu-train"

    def test_does_not_double_prefix(self, client, mock_send):
        mock_send.set_response("output")
        _get_job_logs_impl(client, "DAG-gpu-train")
        assert mock_send.calls[0][1] == "DAG-gpu-train"

    def test_returns_log_output(self, client, mock_send):
        mock_send.set_response("Training complete. Accuracy: 0.97")
        result = _get_job_logs_impl(client, "j1")
        assert "Accuracy: 0.97" in result["logs"]
