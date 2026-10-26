"""Stable, deterministic MCP tool catalog for Titan."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Callable

from .config import McpConfig
from .gateway import TitanGateway


IDENTIFIER = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


def _object(properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "object", "properties": properties, "additionalProperties": False}
    if required:
        schema["required"] = required
    return schema


def _string(description: str, max_length: int = 4096) -> dict[str, Any]:
    return {"type": "string", "description": description, "maxLength": max_length}


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: Callable[[dict[str, Any]], Any]
    write: bool = False
    destructive: bool = False

    def definition(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
            "annotations": {
                "title": self.name.replace("titan_", "Titan ").replace("_", " ").title(),
                "readOnlyHint": not self.write,
                "destructiveHint": self.destructive,
                "idempotentHint": not self.write or self.name in {"titan_cancel_job", "titan_hitl_decide"},
                "openWorldHint": self.write,
            },
        }


class ToolCatalog:
    def __init__(self, gateway: TitanGateway, config: McpConfig):
        self.gateway = gateway
        self.config = config
        self._tools = {tool.name: tool for tool in self._build()}

    def definitions(self) -> list[dict[str, Any]]:
        return [self._tools[name].definition() for name in sorted(self._tools)]

    def call(self, name: str, arguments: dict[str, Any] | None) -> Any:
        tool = self._tools.get(name)
        if tool is None:
            raise ValueError(f"Unknown Titan MCP tool: {name}")
        if tool.write and not self.config.operator_mode:
            raise PermissionError("Write tools require TITAN_MCP_OPERATOR=true")
        args = arguments or {}
        if not isinstance(args, dict):
            raise ValueError("Tool arguments must be an object")
        self._validate_value(tool.input_schema, args, "arguments")
        return tool.handler(args)

    @classmethod
    def _validate_value(cls, schema: dict[str, Any], value: Any, path: str) -> None:
        if "enum" in schema and value not in schema["enum"]:
            raise ValueError(f"{path} must be one of {schema['enum']}")
        kind = schema.get("type")
        if kind == "object":
            if not isinstance(value, dict):
                raise ValueError(f"{path} must be an object")
            properties = schema.get("properties", {})
            missing = [key for key in schema.get("required", []) if key not in value]
            if missing:
                raise ValueError(f"{path} is missing required fields: " + ", ".join(missing))
            additional = schema.get("additionalProperties", True)
            unknown = sorted(set(value) - set(properties))
            if unknown and additional is False:
                raise ValueError(f"{path} contains unknown fields: " + ", ".join(unknown))
            for key, item in value.items():
                child_schema = properties.get(key)
                if child_schema is None and isinstance(additional, dict):
                    child_schema = additional
                if child_schema is not None:
                    cls._validate_value(child_schema, item, f"{path}.{key}")
        elif kind == "array":
            if not isinstance(value, list):
                raise ValueError(f"{path} must be an array")
            if len(value) < schema.get("minItems", 0):
                raise ValueError(f"{path} contains too few items")
            if "maxItems" in schema and len(value) > schema["maxItems"]:
                raise ValueError(f"{path} contains too many items")
            item_schema = schema.get("items", {})
            for index, item in enumerate(value):
                cls._validate_value(item_schema, item, f"{path}[{index}]")
        elif kind == "string":
            if not isinstance(value, str):
                raise ValueError(f"{path} must be a string")
            if len(value) < schema.get("minLength", 0):
                raise ValueError(f"{path} is too short")
            if "maxLength" in schema and len(value) > schema["maxLength"]:
                raise ValueError(f"{path} is too long")
        elif kind == "integer":
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{path} must be an integer")
            if "minimum" in schema and value < schema["minimum"]:
                raise ValueError(f"{path} must be at least {schema['minimum']}")
            if "maximum" in schema and value > schema["maximum"]:
                raise ValueError(f"{path} must be at most {schema['maximum']}")
        elif kind == "boolean" and not isinstance(value, bool):
            raise ValueError(f"{path} must be a boolean")

    @staticmethod
    def _id(value: str, label: str = "identifier") -> str:
        if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
            raise ValueError(f"{label} must use 1-128 letters, numbers, '.', '_', ':', or '-'")
        return value

    def _stats_object(self, view: str) -> dict[str, Any]:
        value = self.gateway.stats(view)
        if not isinstance(value, dict):
            raise RuntimeError(f"Titan {view} stats must be a JSON object")
        return value

    def _system(self, _: dict[str, Any]) -> dict[str, Any]:
        data = self._stats_object("system")
        workers = data.get("workers", [])
        jobs = data.get("dag_jobs", {})
        counts: dict[str, int] = {}
        for details in jobs.values():
            status = str(details.get("status", "UNKNOWN"))
            counts[status] = counts.get(status, 0) + 1
        services = [{"id": service_id, "worker_port": worker.get("port")}
                    for worker in workers for service_id in worker.get("services", [])]
        return {
            "active_workers": data.get("active_workers", len(workers)),
            "queue_size": data.get("queue_size", 0),
            "workers": workers,
            "services": services,
            "job_counts": counts,
        }

    def _workers(self, _: dict[str, Any]) -> dict[str, Any]:
        data = self._stats_object("system")
        return {"workers": data.get("workers", []), "count": len(data.get("workers", []))}

    def _services(self, _: dict[str, Any]) -> dict[str, Any]:
        data = self._stats_object("system")
        services = [{"id": service_id, "worker_port": worker.get("port")}
                    for worker in data.get("workers", [])
                    for service_id in worker.get("services", [])]
        return {"services": services, "count": len(services)}

    def _queue(self, args: dict[str, Any]) -> dict[str, Any]:
        return self._stats_object(f"queue|{args.get('limit', 100)}")

    def _jobs(self, args: dict[str, Any]) -> dict[str, Any]:
        data = self._stats_object("system")
        jobs = data.get("dag_jobs", {})
        status = args.get("status")
        rows = [{"id": job_id, **details} for job_id, details in jobs.items()
                if not status or str(details.get("status", "")).upper() == status]
        rows.sort(key=lambda row: row["id"])
        offset, limit = args.get("offset", 0), args.get("limit", 100)
        return {"total": len(rows), "offset": offset, "jobs": rows[offset:offset + limit]}

    def _job(self, args: dict[str, Any]) -> dict[str, Any]:
        job_id = self._id(args["job_id"], "job_id")
        data = self._stats_object("system")
        details = data.get("dag_jobs", {}).get(job_id)
        spans = [span for span in data.get("execution_spans", []) if span.get("job_id") == job_id]
        return {"job_id": job_id, "status": self.gateway.status(job_id), "details": details, "spans": spans}

    def _logs(self, args: dict[str, Any]) -> dict[str, Any]:
        job_id = self._id(args["job_id"], "job_id")
        text = self.gateway.logs(job_id)
        maximum = args.get("max_chars", 20000)
        truncated = len(text) > maximum
        return {"job_id": job_id, "logs": text[-maximum:], "truncated": truncated}

    def _projection(self, view: str, args: dict[str, Any]) -> Any:
        since = args.get("since_ms", 0)
        limit = args.get("limit", 1000)
        return self.gateway.stats(f"{view}|{since}|{limit}")

    def _history(self, args: dict[str, Any]) -> Any:
        day = args["utc_date"]
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
            raise ValueError("utc_date must use YYYY-MM-DD")
        return self.gateway.stats(f"history|{day}|{args.get('limit', 1000)}")

    def _submit_job(self, args: dict[str, Any]) -> dict[str, Any]:
        spec = args["job"]
        self._validate_job(spec)
        return {"response": self.gateway.submit_job(spec), "job_id": "DAG-" + spec["id"]}

    def _submit_dag(self, args: dict[str, Any]) -> dict[str, Any]:
        name = self._id(args["name"], "DAG name")
        jobs = args["jobs"]
        if not isinstance(jobs, list) or not 1 <= len(jobs) <= 10000:
            raise ValueError("jobs must contain 1-10000 job specifications")
        for spec in jobs:
            self._validate_job(spec)
        ids = {item["id"] for item in jobs}
        if len(ids) != len(jobs):
            raise ValueError("DAG job IDs must be unique")
        for spec in jobs:
            unknown = sorted(set(spec.get("parents", [])) - ids)
            if unknown:
                raise ValueError(f"Job {spec['id']} references unknown parents: {unknown}")
        response = self.gateway.submit_dag(name, jobs, args.get("agent"))
        return {"response": response, "dag": name, "jobs": len(jobs)}

    def _validate_job(self, spec: dict[str, Any]) -> None:
        if not isinstance(spec, dict):
            raise ValueError("Each job must be an object")
        self._id(spec.get("id", ""), "job id")
        if not isinstance(spec.get("script_path"), str):
            raise ValueError("Each job requires script_path")
        if spec.get("kind", "run") not in {"run", "service"}:
            raise ValueError("job kind must be 'run' or 'service'")
        if spec.get("kind") == "service" and not 1 <= int(spec.get("port", 0)) <= 65535:
            raise ValueError("service jobs require port 1-65535")
        parents = spec.get("parents", [])
        if not isinstance(parents, list) or any(not IDENTIFIER.fullmatch(str(parent)) for parent in parents):
            raise ValueError("parents must be a list of valid job IDs")
        priority = spec.get("priority", 1)
        if isinstance(priority, bool) or not isinstance(priority, int) or not 0 <= priority <= 100:
            raise ValueError("priority must be an integer from 0 to 100")

    def _build(self) -> list[Tool]:
        integer_limit = {"type": "integer", "minimum": 1, "maximum": 5000, "default": 1000}
        job_id = _string("Exact Titan job ID, normally prefixed with DAG-", 128)
        job_spec = _object({
            "id": _string("Job identifier without the DAG- prefix", 128),
            "script_path": _string("Local script inside TITAN_MCP_ALLOWED_ROOTS"),
            "kind": {"type": "string", "enum": ["run", "service"], "default": "run"},
            "args": _string("Arguments passed to the script"),
            "parents": {"type": "array", "items": _string("Parent job ID without DAG-", 128), "default": []},
            "port": {"type": "integer", "minimum": 0, "maximum": 65535, "default": 0},
            "priority": {"type": "integer", "minimum": 0, "maximum": 100, "default": 1},
            "delay_ms": {"type": "integer", "minimum": 0, "maximum": 86400000, "default": 0},
            "affinity": {"type": "boolean", "default": False},
            "requirement": _string("Worker capability such as GENERAL or GPU", 128),
            "input_versions": {"type": "object", "additionalProperties": {"type": "string"}},
            "expected_outputs": {"type": "array", "items": {"type": "string"}},
            "hitl_message": _string("Optional human approval request", 500),
            "max_wait_seconds": {"type": "integer", "minimum": 1, "maximum": 172800},
        }, ["id", "script_path"])
        tools = [
            Tool("titan_cluster_status", "Return a compact health, capacity and queue summary.", _object({}), self._system),
            Tool("titan_list_workers", "List registered workers, capabilities, load and recent history.", _object({}), self._workers),
            Tool("titan_list_services", "List service endpoints currently known to Titan.", _object({}), self._services),
            Tool("titan_inspect_queue", "Inspect ready, dependency-blocked, delayed and running queue lanes.", _object({"limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100}}), self._queue),
            Tool("titan_list_jobs", "List known DAG jobs with optional status filtering and pagination.", _object({"status": {"type": "string", "enum": ["PENDING", "RUNNING", "COMPLETED", "FAILED", "CANCELLED", "DEAD"]}, "offset": {"type": "integer", "minimum": 0, "default": 0}, "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100}}), self._jobs),
            Tool("titan_get_job", "Get one job's current status, metadata and execution attempts.", _object({"job_id": job_id}, ["job_id"]), self._job),
            Tool("titan_get_job_logs", "Read the tail of a job's logs with a bounded response size.", _object({"job_id": job_id, "max_chars": {"type": "integer", "minimum": 100, "maximum": 100000, "default": 20000}}, ["job_id"]), self._logs),
            Tool("titan_get_timeline", "Read bounded execution spans since a timestamp.", _object({"since_ms": {"type": "integer", "minimum": 0, "default": 0}, "limit": integer_limit}), lambda a: self._projection("timeline", a)),
            Tool("titan_get_metrics", "Read bounded scheduler and fleet telemetry since a timestamp.", _object({"since_ms": {"type": "integer", "minimum": 0, "default": 0}, "limit": {"type": "integer", "minimum": 1, "maximum": 5000, "default": 200}}), lambda a: self._projection("metrics", a)),
            Tool("titan_get_span_history", "Read persisted execution spans for one UTC date.", _object({"utc_date": _string("UTC day in YYYY-MM-DD format", 10), "limit": integer_limit}, ["utc_date"]), self._history),
            Tool("titan_submit_job", "Submit one script or service through Titan's existing SDK.", _object({"job": job_spec}, ["job"]), self._submit_job, write=True),
            Tool("titan_submit_dag", "Submit a validated DAG, optionally as an agent-run stage.", _object({"name": _string("DAG name", 128), "jobs": {"type": "array", "items": job_spec, "minItems": 1, "maxItems": 10000}, "agent": _object({"run_id": _string("Agent run identifier", 128), "run_name": _string("Human-readable agent run name", 160), "stage": _string("Agent stage", 64), "stage_order": {"type": "integer", "minimum": 0, "maximum": 10000}}, ["run_id"])}, ["name", "jobs"]), self._submit_dag, write=True),
            Tool("titan_cancel_job", "Cancel a queued or running job and its dependent jobs.", _object({"job_id": job_id}, ["job_id"]), lambda a: {"response": self.gateway.cancel(self._id(a["job_id"], "job_id"))}, write=True, destructive=True),
            Tool("titan_resolve_service", "Resolve a ready Titan service to its host and port.", _object({"service_id": job_id}, ["service_id"]), lambda a: {"endpoint": self.gateway.resolve_service(self._id(a["service_id"], "service_id"))}),
            Tool("titan_stop_service", "Stop a running Titan service by its exact service ID.", _object({"service_id": job_id}, ["service_id"]), lambda a: {"response": self.gateway.stop_service(self._id(a["service_id"], "service_id"))}, write=True, destructive=True),
            Tool("titan_hitl_decide", "Approve or reject a waiting Titan human-review gate exactly once.", _object({"gate_id": job_id, "decision": {"type": "string", "enum": ["APPROVED", "REJECTED"]}}, ["gate_id", "decision"]), lambda a: {"decision": self.gateway.hitl_decide(self._id(a["gate_id"], "gate_id"), a["decision"])}, write=True, destructive=True),
            Tool("titan_kv_get", "Read workflow data from the configured MCP user namespace.", _object({"key": _string("Key relative to the MCP namespace", 256)}, ["key"]), lambda a: {"value": self.gateway.kv_get(a["key"])}),
            Tool("titan_kv_put", "Write workflow data inside the configured MCP user namespace.", _object({"key": _string("Key relative to the MCP namespace", 256), "value": _string("String value without the Titan field delimiter")}, ["key", "value"]), lambda a: {"response": self.gateway.kv_put(a["key"], a["value"])}, write=True),
            Tool("titan_kv_add", "Add a member to a workflow set inside the MCP user namespace.", _object({"key": _string("Set key relative to the MCP namespace", 256), "member": _string("Set member")}, ["key", "member"]), lambda a: {"added": self.gateway.kv_add(a["key"], a["member"])}, write=True),
            Tool("titan_kv_members", "List workflow set members inside the MCP user namespace.", _object({"key": _string("Set key relative to the MCP namespace", 256)}, ["key"]), lambda a: {"members": self.gateway.kv_members(a["key"])}),
            Tool("titan_upload_asset", "Upload one guarded local file to Titan's asset store.", _object({"path": _string("File inside TITAN_MCP_ALLOWED_ROOTS")}, ["path"]), lambda a: {"response": self.gateway.upload_asset(a["path"])}, write=True),
            Tool("titan_fetch_artifact", "Fetch one Titan artifact into a guarded local destination.", _object({"name": _string("Artifact filename", 255), "destination": _string("Destination inside TITAN_MCP_ALLOWED_ROOTS")}, ["name", "destination"]), lambda a: {"path": self.gateway.fetch_artifact(a["name"], a["destination"])}, write=True),
        ]
        return tools


def tool_result(value: Any) -> dict[str, Any]:
    text = json.dumps(value, indent=2, sort_keys=True, default=str)
    structured = value if isinstance(value, dict) else {"result": value}
    return {"content": [{"type": "text", "text": text}], "structuredContent": structured, "isError": False}


def tool_error(error: Exception) -> dict[str, Any]:
    value = {"error": type(error).__name__, "message": str(error)}
    return {"content": [{"type": "text", "text": json.dumps(value)}], "structuredContent": value, "isError": True}
