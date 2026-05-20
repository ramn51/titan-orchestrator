#  Copyright 2026 Ram Narayanan
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Titan MCP Server — exposes Titan Orchestrator as MCP tools.

Allows any MCP-compatible client (Claude Desktop, Cursor, etc.) to control
a Titan cluster through natural language: submit pipelines, monitor jobs,
approve HITL gates, and read/write TitanStore.

Usage:
    python titan_sdk/titan_mcp_server.py

    # Or as a module:
    python -m titan_sdk.titan_mcp_server

Environment variables:
    TITAN_HOST            Master host (default: 127.0.0.1)
    TITAN_PORT            Master TCP port (default: 9090)
    TITAN_DASHBOARD_PORT  Dashboard port for manifest sync (default: 5000)

Dependency:
    pip install mcp
"""

import json
import os
import socket
import sys
import tempfile
import uuid

# When run directly (e.g. by Claude Desktop), the script's own directory is on
# sys.path but the project root is not — making `titan_sdk` unimportable as a
# package.  Insert the project root (one level up from this file) explicitly.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
# Always run from project root so manifest/workspace paths resolve correctly.
os.chdir(_PROJECT_ROOT)

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    raise ImportError(
        "\n[Titan MCP] 'mcp' package not found.\n"
        "Install it with: pip install mcp\n"
    )

from titan_sdk.titan_sdk import TitanClient, TitanJob, TITAN_HOST, TITAN_PORT

# Prepended to every job script so workers can import titan_sdk regardless
# of their working directory.
_SCRIPT_PREAMBLE = f"""\
import sys as _sys, os as _os
_sys.path.insert(0, {repr(_PROJECT_ROOT)})
del _sys, _os
"""


# ---------------------------------------------------------------------------
# Implementation functions
# Pure logic — no MCP dependency, directly testable.
# Each function takes a TitanClient so tests can inject a mock.
# ---------------------------------------------------------------------------

def _ping_master_impl(host: str = TITAN_HOST, port: int = TITAN_PORT) -> dict:
    """TCP-connect to the Master and return a status dict."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((host, port))
        s.close()
        return {
            "status": "ok",
            "host": host,
            "port": port,
            "message": "Titan Master is reachable",
        }
    except Exception as e:
        return {
            "status": "error",
            "host": host,
            "port": port,
            "message": f"Cannot connect to Master: {e}",
        }


def _submit_single_job_impl(
    client: TitanClient,
    job_id: str,
    script_content: str,
    args: str = "",
    requirement: str = "GENERAL",
    priority: int = 1,
) -> dict:
    """Write script content to a temp file and submit as a single job."""
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = os.path.join(tmpdir, f"{job_id}.py")
        with open(script_path, "w") as f:
            f.write(_SCRIPT_PREAMBLE + script_content)
        job = TitanJob(
            job_id=job_id,
            filename=script_path,
            args=args,
            requirement=requirement,
            priority=priority,
        )
        result = client.submit_job(job)
    return {"status": "submitted", "job_id": job_id, "master_response": result}


def _submit_dag_pipeline_impl(
    client: TitanClient,
    dag_name: str,
    jobs_json: str,
    agent_run_id: str = "",
) -> dict:
    """Parse a JSON job list and submit as a DAG pipeline.

    Each job in jobs_json:
        {
            "job_id":        "step1",
            "script_content": "print('hello')",
            "args":           "",
            "requirement":    "GENERAL",
            "priority":       1,
            "parents":        [],
            "hitl_message":   null
        }

    Setting hitl_message to a non-null string auto-injects a HITL gate after
    that job. Downstream jobs must be approved before they proceed.
    """
    try:
        job_defs = json.loads(jobs_json)
    except json.JSONDecodeError as e:
        return {"status": "error", "message": f"Invalid jobs_json — not valid JSON: {e}"}

    if not isinstance(job_defs, list) or len(job_defs) == 0:
        return {"status": "error", "message": "jobs_json must be a non-empty JSON array"}

    jobs = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for jd in job_defs:
            job_id = jd.get("job_id", "").strip()
            if not job_id:
                return {"status": "error", "message": "Every job definition must include a non-empty 'job_id'"}

            script_path = os.path.join(tmpdir, f"{job_id}.py")
            with open(script_path, "w") as f:
                f.write(_SCRIPT_PREAMBLE + jd.get("script_content", "pass"))

            job = TitanJob(
                job_id=job_id,
                filename=script_path,
                args=jd.get("args", ""),
                requirement=jd.get("requirement", "GENERAL"),
                priority=int(jd.get("priority", 1)),
                parents=jd.get("parents", []),
                hitl_message=jd.get("hitl_message"),
            )
            jobs.append(job)

        effective_run_id = agent_run_id or f"mcp-{dag_name}-{uuid.uuid4().hex[:8]}"
        result = client.submit_dag(
            dag_name,
            jobs,
            agent_run_id=effective_run_id,
        )

    return {
        "status": "submitted",
        "dag_name": dag_name,
        "job_count": len(jobs),
        "agent_run_id": effective_run_id,
        "master_response": result,
    }


def _submit_yaml_pipeline_impl(
    client: TitanClient,
    yaml_path: str,
    agent_run_id: str = "",
) -> dict:
    """Submit an existing Titan YAML pipeline file."""
    if not os.path.exists(yaml_path):
        return {"status": "error", "message": f"YAML file not found: {yaml_path}"}
    result = client.submit_yaml(
        yaml_path,
        agent_run_id=agent_run_id if agent_run_id else None,
    )
    return {"status": "submitted", "yaml_path": yaml_path, "master_response": result}


def _get_job_status_impl(client: TitanClient, job_id: str) -> dict:
    """Query the Master for a job's current status.

    Automatically adds the 'DAG-' prefix that the Master uses internally
    so callers can pass either the raw job_id or the full DAG-prefixed ID.
    """
    full_id = job_id if job_id.startswith("DAG-") else f"DAG-{job_id}"
    status = client.get_job_status(full_id)
    return {"job_id": job_id, "status": status.strip() if status else "UNKNOWN"}


def _get_job_logs_impl(client: TitanClient, job_id: str) -> dict:
    """Fetch stdout/stderr logs for a job from the Master.

    Automatically adds the 'DAG-' prefix that the Master uses internally.
    """
    full_id = job_id if job_id.startswith("DAG-") else f"DAG-{job_id}"
    logs = client.fetch_logs(full_id)
    return {"job_id": job_id, "logs": logs if logs else "(no logs available)"}


def _approve_hitl_gate_impl(client: TitanClient, gate_id: str) -> dict:
    """Write APPROVED to TitanStore so the HITL gate unblocks downstream jobs."""
    client.store_put(f"titan:hitl:status:{gate_id}", "APPROVED")
    return {
        "status": "approved",
        "gate_id": gate_id,
        "message": "Gate approved — downstream jobs will proceed",
    }


def _reject_hitl_gate_impl(client: TitanClient, gate_id: str) -> dict:
    """Write REJECTED to TitanStore so the HITL gate stops downstream jobs."""
    client.store_put(f"titan:hitl:status:{gate_id}", "REJECTED")
    return {
        "status": "rejected",
        "gate_id": gate_id,
        "message": "Gate rejected — downstream jobs will not run",
    }


def _store_get_impl(client: TitanClient, key: str) -> dict:
    """Read a key from TitanStore."""
    value = client.store_get(key)
    return {"key": key, "value": value if value else "NULL"}


def _store_put_impl(client: TitanClient, key: str, value: str) -> dict:
    """Write a key-value pair to TitanStore."""
    result = client.store_put(key, value)
    return {"status": "ok", "key": key, "master_response": result}


def _deploy_script_impl(
    client: TitanClient,
    script_name: str,
    script_content: str,
) -> dict:
    """Deploy a script to Master perm_files/ using the specified filename.

    The deployed script persists on the Master and can be referenced by
    any future job submission or the DAG Constructor by its filename.
    """
    if not script_name.endswith(".py"):
        return {"status": "error", "message": "script_name must end with .py"}

    with tempfile.TemporaryDirectory() as tmpdir:
        # Use exact script_name so basename on the Master is correct
        script_path = os.path.join(tmpdir, script_name)
        with open(script_path, "w") as f:
            f.write(script_content)
        result = client.deploy_script(script_path)

    success = bool(result and "SUCCESS" in result)
    return {
        "status": "deployed" if success else "error",
        "script_name": script_name,
        "master_response": result,
    }


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "Titan Orchestrator",
    instructions=(
        "You are connected to a Titan distributed cluster. "
        "Use these tools to submit DAG pipelines, monitor job execution, "
        "approve or reject HITL gates, and manage TitanStore state. "
        "HITL gate IDs always follow the pattern: hitl-gate-{job_id}. "
        "Job IDs are case-sensitive and must be unique within a DAG."
    ),
)

_client = TitanClient()


@mcp.tool()
def ping_master() -> str:
    """Check whether the Titan Master node is reachable.

    Returns a JSON object with status 'ok' or 'error', host, port, and message.
    Call this first to confirm the cluster is up before submitting work.
    """
    try:
        return json.dumps(_ping_master_impl())
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


@mcp.tool()
def submit_single_job(
    job_id: str,
    script_content: str,
    args: str = "",
    requirement: str = "GENERAL",
    priority: int = 1,
) -> str:
    """Submit a single Python script as a job to the Titan cluster.

    Args:
        job_id: Unique identifier for this job (e.g. 'data-fetch', 'train-model')
        script_content: Full Python script to execute on the worker node
        args: Space-separated arguments passed to the script via sys.argv
        requirement: Worker capability required — GENERAL, GPU, or HIGH_MEM
        priority: Scheduling priority (1 = normal; higher values run first)

    Returns JSON: {"status": "submitted", "job_id": "...", "master_response": "..."}
    """
    try:
        return json.dumps(
            _submit_single_job_impl(_client, job_id, script_content, args, requirement, priority)
        )
    except Exception as e:
        return json.dumps({"status": "error", "job_id": job_id, "message": str(e)})


@mcp.tool()
def submit_dag_pipeline(
    dag_name: str,
    jobs_json: str,
    agent_run_id: str = "",
) -> str:
    """Submit a multi-job DAG pipeline to the Titan cluster.

    Args:
        dag_name: Display name for this pipeline (shown in the dashboard)
        jobs_json: JSON array of job definitions. Each job:
            {
                "job_id":         "step1",
                "script_content": "print('step 1')",
                "args":           "",
                "requirement":    "GENERAL",
                "priority":       1,
                "parents":        [],
                "hitl_message":   null
            }
            - parents: list of job_ids this job depends on (empty = runs immediately)
            - hitl_message: set to a string to insert a HITL gate after this job;
              downstream jobs will block until a human approves via the dashboard
              or the approve_hitl_gate tool
        agent_run_id: Optional string — links all DAG stages of one agent run
            together in the Agent Runs dashboard view

    Returns JSON with dag_name, job_count, and master_response.
    """
    try:
        return json.dumps(_submit_dag_pipeline_impl(_client, dag_name, jobs_json, agent_run_id))
    except Exception as e:
        return json.dumps({"status": "error", "dag_name": dag_name, "message": str(e)})


@mcp.tool()
def submit_yaml_pipeline(yaml_path: str, agent_run_id: str = "") -> str:
    """Submit an existing Titan YAML pipeline file to the cluster.

    Args:
        yaml_path: Absolute path to a Titan YAML pipeline file on this machine
        agent_run_id: Optional — links this to an agent run for dashboard grouping

    Returns JSON with submission status and master_response.
    """
    try:
        return json.dumps(_submit_yaml_pipeline_impl(_client, yaml_path, agent_run_id))
    except Exception as e:
        return json.dumps({"status": "error", "yaml_path": yaml_path, "message": str(e)})


@mcp.tool()
def get_job_status(job_id: str) -> str:
    """Query the execution status of a job.

    Args:
        job_id: The job ID to query — do NOT include the 'DAG-' prefix

    Returns JSON: {"job_id": "...", "status": "PENDING|RUNNING|COMPLETED|FAILED|REJECTED|UNKNOWN"}
    """
    try:
        return json.dumps(_get_job_status_impl(_client, job_id))
    except Exception as e:
        return json.dumps({"job_id": job_id, "status": "ERROR", "message": str(e)})


@mcp.tool()
def get_job_logs(job_id: str) -> str:
    """Fetch stdout/stderr logs from a job.

    Args:
        job_id: The job ID to fetch logs for — do NOT include the 'DAG-' prefix

    Returns JSON: {"job_id": "...", "logs": "..."}
    """
    try:
        return json.dumps(_get_job_logs_impl(_client, job_id))
    except Exception as e:
        return json.dumps({"job_id": job_id, "logs": "", "error": str(e)})


@mcp.tool()
def approve_hitl_gate(gate_id: str) -> str:
    """Approve a Human-in-the-Loop (HITL) gate so downstream jobs can proceed.

    When a job has a hitl_message, Titan automatically inserts a gate job
    that pauses execution and waits for human approval. This tool performs
    that approval programmatically.

    HITL gate IDs follow the pattern: hitl-gate-{job_id}
    Example: job 'preprocess' with hitl_message → gate_id is 'hitl-gate-preprocess'

    Args:
        gate_id: The HITL gate ID to approve

    Returns JSON confirmation.
    """
    try:
        return json.dumps(_approve_hitl_gate_impl(_client, gate_id))
    except Exception as e:
        return json.dumps({"status": "error", "gate_id": gate_id, "message": str(e)})


@mcp.tool()
def reject_hitl_gate(gate_id: str) -> str:
    """Reject a Human-in-the-Loop (HITL) gate, stopping all downstream jobs.

    HITL gate IDs follow the pattern: hitl-gate-{job_id}

    Args:
        gate_id: The HITL gate ID to reject

    Returns JSON confirmation.
    """
    try:
        return json.dumps(_reject_hitl_gate_impl(_client, gate_id))
    except Exception as e:
        return json.dumps({"status": "error", "gate_id": gate_id, "message": str(e)})


@mcp.tool()
def store_get(key: str) -> str:
    """Read a value from TitanStore (the distributed key-value store).

    TitanStore is shared across all workers and persists across job runs.
    Use it to pass results between jobs or read agent state.

    Args:
        key: The TitanStore key to read

    Returns JSON: {"key": "...", "value": "..."} — value is "NULL" if key not set.
    """
    try:
        return json.dumps(_store_get_impl(_client, key))
    except Exception as e:
        return json.dumps({"key": key, "value": "NULL", "error": str(e)})


@mcp.tool()
def store_put(key: str, value: str) -> str:
    """Write a value to TitanStore (the distributed key-value store).

    Args:
        key: The TitanStore key to write
        value: The string value to store (use JSON-encoded strings for complex data)

    Returns JSON: {"status": "ok", "key": "...", "master_response": "..."}
    """
    try:
        return json.dumps(_store_put_impl(_client, key, value))
    except Exception as e:
        return json.dumps({"status": "error", "key": key, "message": str(e)})


@mcp.tool()
def deploy_script(script_name: str, script_content: str) -> str:
    """Deploy a Python script to the Titan Master's perm_files/ directory.

    Scripts deployed here persist on the Master and can be referenced by
    filename in any future job submission or via the DAG Constructor UI.
    Deploy a script once; reference it by name in many DAGs.

    Args:
        script_name: Filename on the Master (must end with .py, e.g. 'my_worker.py')
        script_content: Full Python script content to deploy

    Returns JSON: {"status": "deployed"|"error", "script_name": "...", "master_response": "..."}
    """
    try:
        return json.dumps(_deploy_script_impl(_client, script_name, script_content))
    except Exception as e:
        return json.dumps({"status": "error", "script_name": script_name, "message": str(e)})


if __name__ == "__main__":
    mcp.run()
