"""Thin adapter from MCP tools to Titan's existing Python SDK."""

from __future__ import annotations

import json
import os
from typing import Any

from titan_sdk import TitanClient, TitanJob
from titan_sdk import titan_sdk as sdk_module

from .config import McpConfig


OP_STATS_JSON = 0x09
OP_STOP = 0x07


class TitanGateway:
    """Delegates every operation to an existing Titan SDK or stats contract."""

    def __init__(self, config: McpConfig):
        self.config = config
        sdk_module.TITAN_HOST = config.host
        sdk_module.TITAN_PORT = config.port
        self.client = TitanClient()

    @staticmethod
    def _checked(response: str) -> str:
        if response is None:
            raise RuntimeError("Titan returned no response")
        failure_prefixes = ("ERROR", "[FAIL]", "CONNECTION_ERROR")
        if response.startswith(failure_prefixes):
            raise RuntimeError(response)
        return response

    def stats(self, view: str = "system") -> dict[str, Any] | list[Any]:
        response = self._checked(self.client._send_request(OP_STATS_JSON, view))
        try:
            value = json.loads(response)
        except json.JSONDecodeError as error:
            raise RuntimeError("Titan returned malformed stats JSON") from error
        if not isinstance(value, (dict, list)):
            raise RuntimeError("Titan stats response must be a JSON object or array")
        return value

    def submit_job(self, spec: dict[str, Any]) -> str:
        job = self._job(spec)
        return self._checked(self.client.submit_job(job))

    def submit_dag(self, name: str, specs: list[dict[str, Any]], agent: dict[str, Any] | None) -> str:
        jobs = [self._job(item) for item in specs]
        metadata = agent or {}
        response = self.client.submit_dag(
            name,
            jobs,
            agent_run_id=metadata.get("run_id"),
            agent_stage=metadata.get("stage"),
            agent_stage_order=metadata.get("stage_order"),
            agent_run_name=metadata.get("run_name"),
        )
        return self._checked(response)

    def _job(self, spec: dict[str, Any]) -> TitanJob:
        script = self.config.resolve_read_path(spec["script_path"], self.config.max_script_bytes)
        kind = spec.get("kind", "run")
        job_type = "SERVICE" if kind == "service" else "RUN_PAYLOAD"
        return TitanJob(
            spec["id"],
            str(script),
            job_type=job_type,
            args=spec.get("args", ""),
            parents=spec.get("parents", []),
            port=spec.get("port", 0),
            priority=spec.get("priority", 1),
            delay=spec.get("delay_ms", 0),
            affinity=spec.get("affinity", False),
            requirement=spec.get("requirement", "GENERAL"),
            input_versions=spec.get("input_versions", {}),
            expected_outputs=spec.get("expected_outputs", []),
            hitl_message=spec.get("hitl_message"),
            max_wait_seconds=spec.get("max_wait_seconds", 172800),
        )

    def logs(self, job_id: str) -> str:
        return self._checked(self.client.fetch_logs(job_id))

    def status(self, job_id: str) -> str:
        return self._checked(self.client.get_job_status(job_id))

    def cancel(self, job_id: str) -> str:
        return self._checked(self.client.cancel_job(job_id))

    def resolve_service(self, service_id: str) -> str:
        return self._checked(self.client.resolve_service(service_id))

    def stop_service(self, service_id: str) -> str:
        return self._checked(self.client._send_request(OP_STOP, service_id))

    def hitl_decide(self, gate_id: str, decision: str) -> str:
        full_id = gate_id if gate_id.startswith("DAG-hitl-gate-") else "DAG-hitl-gate-" + gate_id
        key = "titan:hitl:status:" + full_id
        current = self.client.store_get(key)
        if current not in {"WAITING", "APPROVED", "REJECTED"}:
            raise RuntimeError(f"HITL gate is not waiting; current value is {current!r}")
        if current in {"APPROVED", "REJECTED"}:
            if current == decision:
                return current
            raise RuntimeError(f"HITL gate already has terminal decision {current}")
        self._checked(self.client.store_put(key, decision))
        return decision

    def kv_get(self, key: str) -> str:
        return self._checked(self.client.store_get(self.config.namespaced_key(key)))

    def kv_put(self, key: str, value: str) -> str:
        if "|" in value:
            raise ValueError("KV values cannot contain '|'")
        return self._checked(self.client.store_put(self.config.namespaced_key(key), value))

    def kv_add(self, key: str, member: str) -> int:
        if "|" in member:
            raise ValueError("KV set members cannot contain '|'")
        return self.client.store_sadd(self.config.namespaced_key(key), member)

    def kv_members(self, key: str) -> list[str]:
        return self.client.store_smembers(self.config.namespaced_key(key))

    def upload_asset(self, path: str) -> str:
        source = self.config.resolve_read_path(path, self.config.max_asset_bytes)
        return self._checked(self.client.upload_file(str(source)))

    def fetch_artifact(self, name: str, destination: str) -> str:
        if not name or os.path.basename(name) != name or name in {".", ".."}:
            raise ValueError("Artifact name must be a plain filename")
        target = self.config.resolve_write_path(destination)
        if not self.client.fetch_artifact(name, str(target)):
            raise RuntimeError(f"Could not fetch artifact {name}")
        return str(target)
