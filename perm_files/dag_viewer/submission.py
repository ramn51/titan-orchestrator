"""Validate constructor graphs and submit them using Titan's existing protocol."""

import base64
import hashlib
import json
import os
import re
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path

NAME = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
ACCEPTED = "DAG_ACCEPTED"
_manifest_locks_guard = threading.Lock()
_manifest_locks = {}


class GraphError(ValueError):
    pass


@contextmanager
def manifest_lock(path, timeout=60.0):
    """Serialize manifest read-modify-write transactions across threads and processes."""
    path = Path(path)
    lock_path = Path(str(path) + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with _manifest_locks_guard:
        local_lock = _manifest_locks.setdefault(str(lock_path.resolve()), threading.RLock())
    with local_lock, lock_path.open("a+b") as lock_file:
        if lock_file.tell() == 0:
            lock_file.write(b"0")
            lock_file.flush()
        deadline = time.monotonic() + timeout
        while True:
            try:
                lock_file.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out waiting for manifest lock: {lock_path}")
                time.sleep(0.01)
        try:
            yield
        finally:
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def normalize(body, scripts_root):
    if not isinstance(body, dict):
        raise GraphError("Expected a JSON graph.")
    name = str(body.get("name", "")).strip()
    if not NAME.fullmatch(name):
        raise GraphError("DAG name must use 1–64 letters, digits, _ or -.")
    jobs = body.get("jobs")
    if not isinstance(jobs, list) or not jobs or len(jobs) > 100:
        raise GraphError("Provide 1–100 jobs.")
    ids = set()
    result = []
    root = Path(scripts_root).resolve()
    for raw in jobs:
        if not isinstance(raw, dict):
            raise GraphError("Each job must be an object.")
        jid = str(raw.get("id", "")).strip()
        if not NAME.fullmatch(jid) or jid in ids:
            raise GraphError(f"Invalid or duplicate job ID: {jid!r}.")
        ids.add(jid)
        filename = str(raw.get("filename", "")).strip()
        path = (root / filename).resolve()
        if (not filename or any(ch in filename for ch in "|;\r\n") or
                path.suffix != ".py" or not path.is_relative_to(root) or not path.is_file()):
            raise GraphError(f"Script for {jid} must be a Python file under {root}.")
        parents = raw.get("depends_on") or []
        if not isinstance(parents, list) or len(parents) != len(set(parents)):
            raise GraphError(f"Invalid dependencies for {jid}.")
        input_versions = raw.get("input_versions") or {}
        expected_outputs = raw.get("expected_outputs") or []
        if (not isinstance(input_versions, dict) or len(input_versions) > 30 or
                any(not isinstance(key, str) or not isinstance(value, str) or len(key) > 100 or len(value) > 200
                    for key, value in input_versions.items()) or
                not isinstance(expected_outputs, list) or len(expected_outputs) > 50 or
                any(not isinstance(value, str) or len(value) > 200 for value in expected_outputs)):
            raise GraphError(f"Invalid input versions or expected outputs for {jid}.")
        args = str(raw.get("args") or "")
        requirement = str(raw.get("requirement") or "GENERAL")
        job_type = str(raw.get("job_type") or "run").lower()
        if job_type not in ("run", "service"):
            raise GraphError(f"Unsupported job type for {jid}.")
        if raw.get("hitl_message"):
            raise GraphError("HITL gates are unavailable in this local engine version.")
        if raw.get("is_archive"):
            raise GraphError("Archive jobs are unavailable in the constructor.")
        if any(ch in args + requirement for ch in "|;\n\r"):
            raise GraphError(f"Unsupported delimiter in settings for {jid}.")
        if requirement not in ("GENERAL", "GPU", "HIGH_MEM", "PYTHON"):
            raise GraphError(f"Unsupported requirement for {jid}.")
        try:
            priority = int(raw.get("priority", 1))
            delay = int(raw.get("delay", 0))
            port = int(raw.get("port", 0))
        except (ValueError, TypeError):
            raise GraphError(f"Priority, delay and port must be integers for {jid}.")
        if not 0 <= priority <= 10 or not 0 <= delay <= 3600000 or not 0 <= port <= 65535:
            raise GraphError(f"Priority, delay or port is out of range for {jid}.")
        result.append({"id": jid, "filename": str(path.relative_to(root)),
                       "path": path, "depends_on": parents, "args": args,
                       "requirement": requirement, "priority": priority, "delay": delay,
                       "job_type": job_type, "port": port, "affinity": bool(raw.get("affinity")),
                       "input_versions": input_versions, "expected_outputs": expected_outputs})
    by_id = {job["id"]: job for job in result}
    for job in result:
        if any(parent not in by_id or parent == job["id"] for parent in job["depends_on"]):
            raise GraphError(f"Unknown or self dependency for {job['id']}.")
    visited = set()
    active = set()

    def visit(jid):
        if jid in active:
            raise GraphError("The dependency graph contains a cycle.")
        if jid in visited:
            return
        active.add(jid)
        for parent in by_id[jid]["depends_on"]:
            visit(parent)
        active.remove(jid)
        visited.add(jid)

    for jid in by_id:
        visit(jid)
    return name, result


def payload_for(jobs):
    lines = []
    for job in jobs:
        encoded = base64.b64encode(job["path"].read_bytes()).decode("ascii")
        parents = ",".join(job["depends_on"])
        if job["job_type"] == "service":
            body = f"DEPLOY_PAYLOAD|{job['path'].name}|{encoded}|{job['port']}|{job['requirement']}"
        else:
            body = f"RUN_PAYLOAD|{job['path'].name}|{job['args']}|{encoded}|{job['requirement']}"
        affinity = "|AFFINITY" if job["affinity"] else ""
        lines.append(f"{job['id']}|{body}|{job['priority']}|{job['delay']}|[{parents}]{affinity}")
    return " ; ".join(lines)


def read_manifest(path):
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def write_manifest(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".titan_dag_manifest.", suffix=".tmp",
                                         delete=False) as file:
            temp = file.name
            json.dump(data, file, indent=2)
            file.flush()
            os.fsync(file.fileno())
        for attempt in range(6):
            try:
                os.replace(temp, path)
                temp = None
                break
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(0.01 * (2 ** attempt))
    finally:
        if temp and os.path.exists(temp):
            os.unlink(temp)


def append_run_history(path, name, jobs, submitted_at):
    """Append a compact dashboard index without rereading the full manifest."""
    history = Path(os.environ.get(
        "TITAN_DAG_RUN_HISTORY", Path(path).with_name(".titan_dag_run_history.jsonl")))
    history.parent.mkdir(parents=True, exist_ok=True)
    record = {"name": name, "run_ts": submitted_at,
              "jobs": [f"DAG-{job['id']}" for job in jobs]}
    descriptor = os.open(history, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, (json.dumps(record, separators=(",", ":")) + "\n").encode("utf-8"))
    finally:
        os.close(descriptor)


def log_offsets(path, jobs):
    root = Path(os.environ.get("TITAN_DAG_WORKSPACE_DIR", Path(path).parent / "titan_workspace"))
    offsets = {}
    for job in jobs:
        log = root / "shared" / f"DAG-{job['id']}.log"
        try:
            offsets[f"DAG-{job['id']}"] = log.stat().st_size
        except OSError:
            offsets[f"DAG-{job['id']}"] = 0
    return offsets


def record_run(path, name, jobs, payload, submitted_at, offsets=None):
    with manifest_lock(path):
        data = read_manifest(path)
        run_jobs = {}
        offsets = offsets or {}
        for key, value in list(data.items()):
            if isinstance(value, dict) and value.get("dag") == name:
                del data[key]
        for job in jobs:
            record = {
                "dag": name, "deps": [f"DAG-{p}" for p in job["depends_on"]],
                "run_ts": submitted_at,
                "log_offset": offsets.get(f"DAG-{job['id']}", 0),
                "spec": {"script": job["filename"], "sha256": hashlib.sha256(job["path"].read_bytes()).hexdigest(),
                         "args": job["args"], "requirement": job["requirement"],
                         "job_type": job["job_type"], "port": job["port"],
                         "priority": job["priority"], "delay_ms": job["delay"],
                         "input_versions": job.get("input_versions") or {},
                         "expected_outputs": job.get("expected_outputs") or []},
            }
            data[f"DAG-{job['id']}"] = record
            run_jobs[f"DAG-{job['id']}"] = record
        runs = data.setdefault(f"__runs__{name}", [])
        runs.append({"run_ts": submitted_at, "jobs": run_jobs})
        data[f"__runs__{name}"] = runs[-30:]
        data[f"__payload__{name}"] = {"dag": name, "dag_payload": payload,
                                      "run_ts": submitted_at}
        write_manifest(path, data)


def submit(communicate, opcode, manifest_path, name, jobs, payload):
    submitted_at = int(time.time() * 1000)
    offsets = log_offsets(manifest_path, jobs)
    response = communicate(opcode, payload)
    if response != ACCEPTED:
        raise GraphError(f"Master rejected DAG: {response or 'No response'}")
    record_run(manifest_path, name, jobs, payload, submitted_at, offsets)
    append_run_history(manifest_path, name, jobs, submitted_at)
    return response


def redeploy(communicate, opcode, manifest_path, name):
    data = read_manifest(manifest_path)
    stored = data.get(f"__payload__{name}", {})
    payload = stored.get("dag_payload") if isinstance(stored, dict) else None
    if not payload:
        raise GraphError("No saved constructor payload for this DAG.")
    for jid, info in data.items():
        if (jid.startswith("DAG-hitl-gate-") and isinstance(info, dict)
                and info.get("dag") == name and (info.get("spec") or {}).get("hitl_message")):
            response = communicate(0x60, f"titan:hitl:status:{jid}|CLEARED")
            if response != "OK":
                raise GraphError(f"Could not reset HITL gate {jid}: {response or 'No response'}")
    submitted_at = int(time.time() * 1000)
    offsets = log_offsets(manifest_path, [{"id": jid[4:]} for jid, info in data.items()
                                          if jid.startswith("DAG-") and isinstance(info, dict)
                                          and info.get("dag") == name])
    response = communicate(opcode, payload)
    if response != ACCEPTED:
        raise GraphError(f"Master rejected DAG: {response or 'No response'}")
    with manifest_lock(manifest_path):
        # Re-read after master acknowledgement so another accepted submission
        # cannot be overwritten by the copy read before this RPC.
        data = read_manifest(manifest_path)
        for jid, value in data.items():
            if isinstance(value, dict) and value.get("dag") == name:
                value["run_ts"] = submitted_at
                value["log_offset"] = offsets.get(jid, 0)
        run_jobs = {jid: dict(info) for jid, info in data.items()
                    if jid.startswith("DAG-") and isinstance(info, dict) and info.get("dag") == name}
        runs = data.setdefault(f"__runs__{name}", [])
        runs.append({"run_ts": submitted_at, "jobs": run_jobs})
        data[f"__runs__{name}"] = runs[-30:]
        write_manifest(manifest_path, data)
    append_run_history(manifest_path, name,
                       [{"id": jid.removeprefix("DAG-")} for jid in run_jobs], submitted_at)
    return response
