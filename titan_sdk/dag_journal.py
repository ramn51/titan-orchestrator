"""Dependency-free, append-oriented persistence for submitted DAG metadata.

The legacy manifest is retained as migration input. New runs are immutable files;
only a small pointer for each DAG is replaced when its latest run changes.
"""

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time


SCHEMA_VERSION = 1
_summary_cache_lock = threading.Lock()
_summary_cache = {}


def journal_root(manifest_path):
    manifest = Path(manifest_path).resolve()
    configured = os.environ.get("TITAN_DAG_JOURNAL_DIR")
    return Path(configured).resolve() if configured else manifest.with_name(".titan_dag_journal")


def _dag_key(name):
    return hashlib.sha256(str(name).encode("utf-8")).hexdigest()[:24]


def _manifest_signature(path):
    try:
        stat = Path(path).stat()
        return {"ino": stat.st_ino, "ctime_ns": stat.st_ctime_ns,
                "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    except OSError:
        return None


def _read_json(path, default=None):
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        return value
    except (OSError, ValueError, TypeError):
        return default


def _atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".titan-journal-", suffix=".tmp",
                                         delete=False) as output:
            temporary = Path(output.name)
            json.dump(value, output, separators=(",", ":"), ensure_ascii=False)
            output.flush()
            os.fsync(output.fileno())
        for attempt in range(6):
            try:
                os.replace(temporary, path)
                temporary = None
                return
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(0.01 * (2 ** attempt))
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                pass


@contextmanager
def _journal_lock(root, timeout=60.0):
    """Small cross-process lock used only for pointer/import coordination."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".lock"
    deadline = time.monotonic() + timeout
    descriptor = None
    while descriptor is None:
        try:
            descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.write(descriptor, f"{os.getpid()}\n".encode("ascii"))
        except FileExistsError:
            try:
                if time.time() - lock.stat().st_mtime > max(120.0, timeout * 2):
                    lock.unlink()
                    continue
            except OSError:
                continue
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for DAG journal lock: {lock}")
            time.sleep(0.02)
    try:
        yield
    finally:
        try:
            os.close(descriptor)
        finally:
            try:
                lock.unlink()
            except OSError:
                pass


def _run_path(root, name, run_ts):
    return Path(root) / "runs" / _dag_key(name) / f"{int(run_ts)}.json"


def _pointer_path(root, name):
    return Path(root) / "current" / f"{_dag_key(name)}.json"


def _record_from_pointer(root, pointer):
    if not isinstance(pointer, dict) or not isinstance(pointer.get("run"), str):
        return None
    root = Path(root).resolve()
    candidate = (root / pointer["run"]).resolve()
    if not candidate.is_relative_to(root):
        return None
    record = _read_json(candidate, {})
    return record if isinstance(record, dict) else None


def _write_run_unlocked(root, name, run_ts, jobs, payload=None):
    root = Path(root)
    run_ts = int(run_ts)
    record = {"schema": SCHEMA_VERSION, "name": str(name), "run_ts": run_ts,
              "jobs": jobs if isinstance(jobs, dict) else {},
              "payload": payload if isinstance(payload, str) else None}
    destination = _run_path(root, name, run_ts)
    _atomic_json(destination, record)
    pointer_path = _pointer_path(root, name)
    previous = _read_json(pointer_path, {})
    if not isinstance(previous, dict) or int(previous.get("run_ts") or 0) <= run_ts:
        _atomic_json(pointer_path, {"schema": SCHEMA_VERSION, "name": str(name),
                                    "run_ts": run_ts,
                                    "run": destination.relative_to(root).as_posix()})
    return record


def write_run(manifest_path, name, run_ts, jobs, payload=None):
    """Persist one accepted run without reading or rewriting prior runs."""
    root = journal_root(manifest_path)
    with _journal_lock(root):
        pointer = _read_json(_pointer_path(root, name), {})
        previous_ts = int(pointer.get("run_ts") or 0) if isinstance(pointer, dict) else 0
        actual_ts = max(int(run_ts), previous_ts + 1) if previous_ts >= int(run_ts) else int(run_ts)
        normalized_jobs = {}
        for job_id, info in (jobs or {}).items():
            record = dict(info) if isinstance(info, dict) else {}
            record["run_ts"] = actual_ts
            normalized_jobs[job_id] = record
        return _write_run_unlocked(root, name, actual_ts, normalized_jobs, payload)


def _legacy_runs(manifest):
    payloads = {}
    direct = {}
    runs = []
    for key, value in manifest.items():
        if key.startswith("__payload__") and isinstance(value, dict):
            payloads[key.removeprefix("__payload__")] = value
        elif key.startswith("__runs__") and isinstance(value, list):
            name = key.removeprefix("__runs__")
            for run in value:
                if isinstance(run, dict) and isinstance(run.get("jobs"), dict):
                    runs.append((name, int(run.get("run_ts") or 0), run["jobs"]))
        elif not key.startswith("__") and isinstance(value, dict) and value.get("dag"):
            direct.setdefault(str(value["dag"]), {})[key] = value
    return payloads, direct, runs


def ensure_legacy_imported(manifest_path):
    """Import a changed legacy manifest once, leaving the source untouched."""
    manifest_path = Path(manifest_path).resolve()
    signature = _manifest_signature(manifest_path)
    root = journal_root(manifest_path)
    marker = root / "legacy-import.json"
    saved_marker = _read_json(marker, {})
    if isinstance(saved_marker, dict) and saved_marker.get("manifest") == signature:
        return False
    if signature is None:
        return False
    with _journal_lock(root):
        saved_marker = _read_json(marker, {})
        if isinstance(saved_marker, dict) and saved_marker.get("manifest") == signature:
            return False
        legacy = _read_json(manifest_path, {})
        if not isinstance(legacy, dict):
            return False
        payloads, direct, runs = _legacy_runs(legacy)
        latest = {}
        for name, run_ts, jobs in runs:
            if run_ts < 0:
                continue
            payload_info = payloads.get(name, {})
            payload = payload_info.get("dag_payload") if int(payload_info.get("run_ts") or 0) == run_ts else None
            _write_run_unlocked(root, name, run_ts, jobs, payload)
            latest[name] = max(latest.get(name, 0), run_ts)
        for name, jobs in direct.items():
            run_ts = max((int(item.get("run_ts") or 0) for item in jobs.values()), default=0)
            if run_ts < 0:
                continue
            payload_info = payloads.get(name, {})
            payload = payload_info.get("dag_payload")
            # Direct entries carry the freshest log offsets and job specifications.
            if run_ts >= latest.get(name, 0):
                _write_run_unlocked(root, name, run_ts, jobs, payload)
                latest[name] = run_ts
        previous_imports = saved_marker.get("imported", {}) if isinstance(saved_marker, dict) else {}
        for name, previous_ts in previous_imports.items():
            if name in latest:
                continue
            pointer_path = _pointer_path(root, name)
            pointer = _read_json(pointer_path, {})
            if isinstance(pointer, dict) and int(pointer.get("run_ts") or 0) == int(previous_ts or 0):
                try:
                    pointer_path.unlink()
                except OSError:
                    pass
        _atomic_json(marker, {"schema": SCHEMA_VERSION, "manifest": signature,
                              "imported": latest,
                              "imported_at": int(time.time() * 1000)})
    return True


def journal_signature(manifest_path):
    root = journal_root(manifest_path)
    signatures = []
    paths = [root / "legacy-import.json"]
    current = root / "current"
    if current.is_dir():
        paths.extend(sorted(current.glob("*.json")))
    else:
        paths.append(current)
    for path in paths:
        try:
            stat = path.stat()
            signatures.append((str(path), stat.st_mtime_ns, stat.st_size))
        except OSError:
            signatures.append((str(path), 0, 0))
    return tuple(signatures)


def current_runs(manifest_path):
    ensure_legacy_imported(manifest_path)
    root = journal_root(manifest_path)
    result = {}
    for pointer_path in (root / "current").glob("*.json") if (root / "current").is_dir() else ():
        pointer = _read_json(pointer_path, {})
        if not isinstance(pointer, dict) or not pointer.get("run"):
            continue
        record = _record_from_pointer(root, pointer)
        if isinstance(record, dict) and record.get("name") and isinstance(record.get("jobs"), dict):
            result[str(record["name"])] = record
    return result


def runs_for(manifest_path, name, limit=30):
    ensure_legacy_imported(manifest_path)
    root = journal_root(manifest_path)
    directory = root / "runs" / _dag_key(name)
    records = []
    for path in directory.glob("*.json") if directory.is_dir() else ():
        record = _read_json(path, {})
        if isinstance(record, dict) and record.get("name") == name and isinstance(record.get("jobs"), dict):
            records.append(record)
    records.sort(key=lambda item: int(item.get("run_ts") or 0), reverse=True)
    return records[:max(0, int(limit))] if limit is not None else records


def latest_run(manifest_path, name):
    ensure_legacy_imported(manifest_path)
    root = journal_root(manifest_path)
    pointer = _read_json(_pointer_path(root, name), {})
    if not isinstance(pointer, dict) or not pointer.get("run"):
        return None
    record = _record_from_pointer(root, pointer)
    return record if isinstance(record, dict) and record.get("name") == name else None


def run_summaries(manifest_path, since=0, limit=None):
    """Return compact historical run records without opening the legacy manifest."""
    ensure_legacy_imported(manifest_path)
    root = journal_root(manifest_path)
    revision = journal_signature(manifest_path)
    cache_key = str(root)
    with _summary_cache_lock:
        cached = _summary_cache.get(cache_key)
        if cached and cached[0] == revision:
            records = cached[1]
            filtered = [record for record in records if record["run_ts"] >= int(since or 0)]
            return filtered[-int(limit):] if limit else filtered
    records = []
    runs_root = root / "runs"
    for path in runs_root.glob("*/*.json") if runs_root.is_dir() else ():
        record = _read_json(path, {})
        run_ts = int(record.get("run_ts") or 0) if isinstance(record, dict) else 0
        jobs = record.get("jobs") if isinstance(record, dict) else None
        if isinstance(record, dict) and record.get("name") and isinstance(jobs, dict) and run_ts >= int(since or 0):
            records.append({"name": str(record["name"]), "run_ts": run_ts,
                            "jobs": list(jobs)})
    records.sort(key=lambda item: item["run_ts"])
    immutable = tuple(records)
    with _summary_cache_lock:
        _summary_cache[cache_key] = (revision, immutable)
    filtered = [record for record in immutable if record["run_ts"] >= int(since or 0)]
    return filtered[-int(limit):] if limit else filtered
