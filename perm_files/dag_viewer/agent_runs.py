"""Build a stage-level view across related agent DAG submissions.

Agent execution remains a sequence of normal DAGs.  The SDK records lightweight
grouping metadata with each submitted DAG and this module joins that metadata to
the DAG catalog.  A narrow fallback recognizes Titan's existing AGENTBENCH runs
so results created before the metadata field was introduced remain visible.
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import quote

from titan_sdk.dag_journal import current_runs


_BENCHMARK_NAME = re.compile(r"^AGENTBENCH_(?P<case>.+)_(?P<stage>[^_]+)_(?P<pass>\d+)$")
_BENCHMARK_TOKEN = re.compile(r"^DAG-(?P<token>ab\d+)")
_ACTIVE = {"WAITING", "PENDING", "RUNNING"}


def _first_metadata(record):
    for value in (record.get("jobs") or {}).values():
        if isinstance(value, dict):
            return value
    return {}


def _fallback_metadata(dag, record):
    """Recognize only the benchmark's deliberate naming contract.

    Keeping this fallback narrow avoids grouping ordinary DAGs merely because
    their names happen to share underscores or numeric suffixes.
    """
    match = _BENCHMARK_NAME.match(dag["name"])
    if not match:
        return None
    token = None
    for job_id in (record.get("jobs") or {}):
        token = _BENCHMARK_TOKEN.match(job_id)
        if token:
            break
    if token is None:
        return None
    case = match.group("case")
    stage = match.group("stage")
    return {
        "agent_run_id": f"{case}:{token.group('token')}",
        "agent_run_name": case.replace("-", " ").replace("_", " ").title(),
        "agent_stage": stage,
        # The concurrent-client benchmark submits these independent stages
        # together. Preserve that parallel shape for pre-metadata results.
        "agent_stage_order": 0 if re.fullmatch(r"CLIENT\d+", stage) else None,
    }


def _run_status(stages):
    statuses = [stage["status"] for stage in stages]
    if any(status == "FAILED" for status in statuses):
        return "FAILED"
    if any(status == "RUNNING" for status in statuses):
        return "RUNNING"
    if statuses and all(status == "COMPLETED" for status in statuses):
        return "COMPLETED"
    if any(status == "CANCELLED" for status in statuses):
        return "CANCELLED"
    if any(status in _ACTIVE for status in statuses):
        return "WAITING"
    return "UNKNOWN"


class AgentRunCatalog:
    """Project DAG snapshots into agent invocations and their ordered stages."""

    def __init__(self, manifest_path):
        self.manifest_path = Path(manifest_path)

    def snapshot(self, dags, records=None):
        # The DAG catalog has already loaded and cached these immutable records
        # for the same request. Accepting them here avoids a second journal walk.
        records = records if records is not None else current_runs(self.manifest_path)
        grouped = {}
        for dag in dags:
            record = records.get(dag["name"], {})
            metadata = _first_metadata(record)
            run_id = metadata.get("agent_run_id")
            if run_id:
                values = {
                    "agent_run_id": str(run_id),
                    "agent_run_name": str(metadata.get("agent_run_name") or run_id),
                    "agent_stage": str(metadata.get("agent_stage") or dag["name"]),
                    "agent_stage_order": metadata.get("agent_stage_order"),
                }
            else:
                values = _fallback_metadata(dag, record)
            if values is None:
                continue
            run = grouped.setdefault(values["agent_run_id"], {
                "id": values["agent_run_id"],
                "name": values["agent_run_name"],
                "stages": [],
            })
            try:
                requested_order = int(values["agent_stage_order"])
            except (TypeError, ValueError):
                requested_order = None
            run["stages"].append({
                "id": dag["id"],
                "dag_name": dag["name"],
                "label": values["agent_stage"],
                "order": requested_order,
                "submitted": int(dag.get("submitted") or record.get("run_ts") or 0),
                "status": dag["status"],
                "completed": int(dag.get("completed") or 0),
                "total": int(dag.get("total") or 0),
                "url": "/dags/" + quote(dag["id"], safe=""),
            })

        result = []
        for run in grouped.values():
            stages = sorted(run["stages"], key=lambda item: (
                item["order"] is None,
                item["order"] if item["order"] is not None else item["submitted"],
                item["submitted"], item["label"]))
            implicit_order = -1
            last_submitted = None
            for stage in stages:
                if stage["order"] is None:
                    if stage["submitted"] != last_submitted:
                        implicit_order += 1
                        last_submitted = stage["submitted"]
                    stage["order"] = implicit_order
            run["stages"] = stages
            run["status"] = _run_status(stages)
            run["submitted"] = min((stage["submitted"] for stage in stages), default=0)
            run["updated"] = max((stage["submitted"] for stage in stages), default=0)
            run["completed_stages"] = sum(stage["status"] == "COMPLETED" for stage in stages)
            run["total_stages"] = len(stages)
            run["total_jobs"] = sum(stage["total"] for stage in stages)
            run["completed_jobs"] = sum(stage["completed"] for stage in stages)
            result.append(run)
        return sorted(result, key=lambda run: (run["updated"], run["id"]), reverse=True)
