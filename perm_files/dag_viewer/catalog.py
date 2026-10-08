"""Combine submission metadata with the master's live execution snapshot."""

import json
import re
import threading
import time
from pathlib import Path

from .submission import read_manifest, write_manifest

try:
    import yaml
except ImportError:  # SDK-only installs can still show manifest-backed DAGs.
    yaml = None

TERMINAL = {"COMPLETED", "FAILED", "DEAD", "CANCELLED"}


def full_id(value):
    value = str(value)
    return value if value.startswith("DAG-") else "DAG-" + value


def overall(jobs):
    states = [job["status"] for job in jobs]
    if not states:
        return "WAITING"
    if any(s in ("FAILED", "DEAD") for s in states):
        return "FAILED"
    if any(s == "RUNNING" for s in states):
        return "RUNNING"
    if all(s == "COMPLETED" for s in states):
        return "COMPLETED"
    if any(s == "CANCELLED" for s in states):
        return "CANCELLED"
    if any(s == "UNKNOWN" for s in states):
        return "UNKNOWN"
    return "WAITING"


class DagCatalog:
    def __init__(self, manifest_path, yaml_root):
        self.manifest_path = Path(manifest_path)
        self.yaml_root = Path(yaml_root)
        self._yaml_cache = ({}, 0.0)
        self._status_path = self.manifest_path.with_name(self.manifest_path.name + ".status.json")
        self._status_lock = threading.Lock()

    def _remember_statuses(self, updates):
        with self._status_lock:
            saved = read_manifest(self._status_path)
            changed = False
            for jid, result in updates.items():
                previous = saved.get(jid, {})
                if isinstance(previous, dict) and previous.get("run_ts") == result["run_ts"]:
                    if not result.get("attempts"):
                        result["attempts"] = previous.get("attempts", 0)
                    if result["status"] in ("FAILED", "DEAD") and not result.get("last_error"):
                        result["last_error"] = previous.get("last_error", "")
                        result["exit_code"] = previous.get("exit_code")
                if saved.get(jid) != result:
                    saved[jid] = result
                    changed = True
                history_key = f"__run__{result['run_ts']}__{jid}"
                if saved.get(history_key) != result:
                    saved[history_key] = result
                    changed = True
            if changed:
                try:
                    write_manifest(self._status_path, saved)
                except OSError:
                    pass  # Live status remains usable if the status cache is read-only.
            return saved

    def _definitions(self):
        definitions = {}
        now = time.monotonic()
        if now - self._yaml_cache[1] > 30 and yaml is not None and self.yaml_root.exists():
            found = {}
            for path in (*self.yaml_root.rglob("*.yaml"), *self.yaml_root.rglob("*.yml")):
                try:
                    doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
                    if not isinstance(doc, dict) or not isinstance(doc.get("jobs"), list):
                        continue
                    name = str(doc.get("name") or path.stem)
                    for item in doc["jobs"]:
                        if isinstance(item, dict) and item.get("id"):
                            jid = full_id(item["id"])
                            found[jid] = {"dag": name, "deps": [full_id(p) for p in item.get("depends_on", [])], "run_ts": 0}
                except (OSError, ValueError, TypeError, yaml.YAMLError):
                    continue
            self._yaml_cache = (found, now)
        definitions.update(self._yaml_cache[0])
        try:
            saved = json.loads(self.manifest_path.read_text(encoding="utf-8"))
            legacy_services = {}
            for key, payload_info in saved.items():
                if not key.startswith("__payload__") or not isinstance(payload_info, dict):
                    continue
                for line in str(payload_info.get("dag_payload") or "").split(" ; "):
                    fields = line.split("|", 5)
                    if len(fields) > 4 and fields[1] in ("DEPLOY_PAYLOAD", "START_ARCHIVE_SERVICE"):
                        legacy_services[full_id(fields[0])] = fields[4]
            for jid, info in saved.items():
                if not jid.startswith("__") and isinstance(info, dict) and info.get("dag"):
                    record = dict(info)
                    spec = dict(record.get("spec") or {})
                    if not spec.get("job_type") and jid in legacy_services:
                        spec["job_type"] = "service"
                        try:
                            spec["port"] = int(legacy_services[jid])
                        except ValueError:
                            pass
                    record["spec"] = spec
                    definitions[jid] = record  # submitted metadata takes precedence
        except (OSError, ValueError):
            pass
        return definitions

    def snapshot(self, stats, service_endpoints=None):
        definitions = self._definitions()
        service_endpoints = {entry.get("id"): entry for entry in (service_endpoints or [])
                             if isinstance(entry, dict) and entry.get("id")}
        spans_by_job = {}
        for span in (stats or {}).get("execution_spans", []):
            if isinstance(span, dict) and span.get("job_id"):
                spans_by_job.setdefault(span["job_id"], []).append(span)
        observed = {}
        for worker in (stats or {}).get("workers", []):
            address = f"{worker.get('host') or '127.0.0.1'}:{worker.get('port', '?')}"
            for entry in worker.get("history", []):
                jid = entry.get("id")
                if jid:
                    observed[jid] = {"status": entry.get("status", "WAITING"), "worker": address,
                                     "duration": entry.get("time", ""), "completed_at": entry.get("completed_at", 0)}
            for jid in [worker.get("active_job"), *(worker.get("services") or [])]:
                if jid:
                    service = service_endpoints.get(jid)
                    observed[jid] = {"status": "COMPLETED" if service else "RUNNING",
                                     "worker": address, "duration": "",
                                     "completed_at": int(service.get("since") or 0) if service else 0}

        # Current execution records take precedence over the worker's recent-history
        # deque, which can still contain completed entries from an earlier run.
        for jid, entry in (stats or {}).get("dag_jobs", {}).items():
            if jid.startswith("DAG-") and isinstance(entry, dict):
                previous = observed.get(jid, {})
                observed[jid] = {**previous, "status": entry.get("status", "WAITING"),
                                 "worker": entry.get("worker") or None,
                                 "duration": entry.get("time", previous.get("duration", "")),
                                 "completed_at": entry.get("completed_at", 0)}

        for jid in observed:
            if jid.startswith("DAG-") and jid not in definitions:
                name = jid[4:].split("_", 1)[0]
                definitions[jid] = {"dag": name, "deps": [], "run_ts": 0}

        latest = {}
        for info in definitions.values():
            name = str(info.get("dag", ""))
            latest[name] = max(latest.get(name, 0), int(info.get("run_ts") or 0))

        active_runs = set()
        for jid, info in definitions.items():
            name = str(info.get("dag", ""))
            run_ts = latest.get(name, 0)
            seen = observed.get(jid, {})
            completed = int(seen.get("completed_at") or 0)
            if seen.get("status") in {"RUNNING", "WAITING", "PENDING"} or (
                    seen.get("status") in TERMINAL and completed >= run_ts):
                active_runs.add(name)

        confirmed = {}
        for jid, info in definitions.items():
            run_ts = latest.get(str(info.get("dag", "")), 0)
            seen = observed.get(jid, {})
            completed = int(seen.get("completed_at") or 0)
            if run_ts and seen.get("status") in TERMINAL and completed >= run_ts:
                attempts = [span for span in spans_by_job.get(jid, [])
                            if int(span.get("started_at") or 0) >= run_ts
                            and span.get("status") != "REJECTED"]
                latest_attempt = max(attempts, key=lambda span: int(span.get("started_at") or 0), default={})
                reason = str(latest_attempt.get("reason") or "")
                exit_match = re.match(r"COMPLETED\|(-?\d+)\|", reason)
                error_lines = [line.strip() for line in reason.splitlines() if line.strip()]
                confirmed[jid] = {"run_ts": run_ts, "status": seen["status"],
                                  "completed_at": completed, "worker": seen.get("worker"),
                                  "duration": seen.get("duration", ""),
                                  "attempts": len(attempts),
                                  "exit_code": int(exit_match.group(1)) if exit_match else None,
                                  "last_error": error_lines[-1][:300] if error_lines and seen["status"] in ("FAILED", "DEAD") else ""}
        saved_statuses = self._remember_statuses(confirmed)

        grouped = {}
        for jid, info in definitions.items():
            name = str(info.get("dag", ""))
            if not name:
                continue
            run_ts = latest[name]
            seen = observed.get(jid, {})
            fallback = "WAITING" if name in active_runs or not run_ts else "UNKNOWN"
            status = seen.get("status", fallback)
            completed = int(seen.get("completed_at") or 0)
            # A terminal record without a timestamp cannot prove it belongs to this run.
            if run_ts and status in TERMINAL and (not completed or completed < run_ts):
                seen = {}
                status = fallback
            remembered = saved_statuses.get(jid, {})
            if not isinstance(remembered, dict):
                remembered = {}
            if status == "UNKNOWN" and remembered.get("run_ts") == run_ts:
                seen = remembered
                status = remembered["status"]
            if status == "DEAD":
                status = "FAILED"
            dag = grouped.setdefault(name, {"id": "DAG-" + name, "name": name,
                                            "submitted": run_ts, "jobs": [],
                                            "can_redeploy": False})
            attempts = [span for span in spans_by_job.get(jid, [])
                        if int(span.get("started_at") or 0) >= run_ts
                        and span.get("status") != "REJECTED"]
            latest_attempt = max(attempts, key=lambda span: int(span.get("started_at") or 0), default={})
            reason = str(latest_attempt.get("reason") or "") if latest_attempt.get("status") in ("FAILED", "DEAD") else ""
            exit_match = re.match(r"COMPLETED\|(-?\d+)\|", reason)
            error_lines = [line.strip() for line in reason.splitlines() if line.strip()]
            archived = remembered if remembered.get("run_ts") == run_ts else {}
            blocked_by = [full_id(parent) for parent in info.get("deps", [])
                          if observed.get(full_id(parent), {}).get("status") != "COMPLETED"] if status in ("WAITING", "PENDING") else []
            spec = dict(info.get("spec") or {})
            endpoint = service_endpoints.get(jid)
            if endpoint:
                spec.setdefault("job_type", "service")
                spec.setdefault("port", endpoint.get("port"))
            dag["jobs"].append({"id": jid, "status": status,
                                "worker": seen.get("worker"), "duration": seen.get("duration", ""),
                                "parents": [full_id(p) for p in info.get("deps", [])],
                                "blocked_by": blocked_by, "attempts": len(attempts) or archived.get("attempts", 0),
                                "retry_count": max(0, (len(attempts) or archived.get("attempts", 0)) - 1),
                                "exit_code": int(exit_match.group(1)) if exit_match else archived.get("exit_code"),
                                "last_error": error_lines[-1][:300] if error_lines else archived.get("last_error", ""),
                                "spec": spec, "service": {
                                    "host": endpoint.get("host"), "port": endpoint.get("port"),
                                    "url": f"http://{endpoint.get('host')}:{endpoint.get('port')}"
                                } if endpoint and int(endpoint.get("since") or 0) >= run_ts else None})

        result = []
        saved = {}
        try:
            saved = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            pass
        for dag in grouped.values():
            dag["can_redeploy"] = bool(saved.get(f"__payload__{dag['name']}", {}).get("dag_payload"))
            dag["jobs"].sort(key=lambda j: j["id"])
            by_id = {job["id"]: job for job in dag["jobs"]}
            for job in dag["jobs"]:
                if job["status"] in ("WAITING", "PENDING"):
                    job["blocked_by"] = [parent for parent in job["parents"]
                                         if by_id.get(parent, {}).get("status") != "COMPLETED"]
            dag["status"] = overall(dag["jobs"])
            dag["completed"] = sum(j["status"] == "COMPLETED" for j in dag["jobs"])
            dag["total"] = len(dag["jobs"])
            result.append(dag)
        return sorted(result, key=lambda d: (d["submitted"], d["name"]), reverse=True)

    def runs_for(self, name):
        saved = read_manifest(self.manifest_path)
        runs = saved.get(f"__runs__{name}", [])
        return sorted([run for run in runs if isinstance(run, dict) and isinstance(run.get("jobs"), dict)
                       and isinstance(run.get("run_ts"), int)], key=lambda run: run["run_ts"], reverse=True)

    def historical_snapshot(self, name, run_ts):
        run = next((item for item in self.runs_for(name) if item["run_ts"] == run_ts), None)
        if run is None:
            return None
        saved_statuses = read_manifest(self._status_path)
        jobs = []
        for jid, info in run["jobs"].items():
            seen = saved_statuses.get(f"__run__{run_ts}__{jid}") or saved_statuses.get(jid, {})
            if seen.get("run_ts") != run_ts:
                seen = {}
            status = seen.get("status", "UNKNOWN")
            jobs.append({"id": jid, "status": "FAILED" if status == "DEAD" else status,
                         "worker": seen.get("worker"), "duration": seen.get("duration", ""),
                         "parents": info.get("deps", []), "blocked_by": [],
                         "attempts": seen.get("attempts", 0),
                         "retry_count": max(0, seen.get("attempts", 0) - 1),
                         "exit_code": seen.get("exit_code"), "last_error": seen.get("last_error", ""),
                         "spec": info.get("spec") or {}, "service": None})
        jobs.sort(key=lambda job: job["id"])
        return {"id": "DAG-" + name, "name": name, "submitted": run_ts,
                "jobs": jobs, "status": overall(jobs), "completed": sum(job["status"] == "COMPLETED" for job in jobs),
                "total": len(jobs), "can_redeploy": False, "historical": True}
