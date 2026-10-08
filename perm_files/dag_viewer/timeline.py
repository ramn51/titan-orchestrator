"""Run-scoped execution spans and metrics for the standalone timeline page."""

import json
import re
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=48)
def _read_matching_spans(path_text, modified_ns, size, job_ids):
    """Read one immutable journal revision and retain only the requested jobs."""
    del modified_ns, size  # They intentionally form part of the cache key.
    matches = []
    wanted = set(job_ids)
    try:
        with open(path_text, encoding="utf-8") as file:
            for line in file:
                try:
                    span = json.loads(line)
                    if not wanted or span.get("job_id") in wanted:
                        matches.append(span)
                except (ValueError, TypeError):
                    continue
    except OSError:
        pass
    return tuple(matches)


def _journal_spans(path, job_ids=()):
    try:
        stat = Path(path).stat()
    except OSError:
        return ()
    return _read_matching_spans(str(path), stat.st_mtime_ns, stat.st_size,
                                tuple(sorted(job_ids)))


def _dated_journals(directory, submitted=0, run_end=None):
    """Select UTC day journals that can overlap this run."""
    directory = Path(directory)
    if not directory.is_dir():
        return []
    start_day = (datetime.fromtimestamp(submitted / 1000, timezone.utc).date()
                 if submitted else None)
    end_day = (datetime.fromtimestamp((run_end - 1) / 1000, timezone.utc).date()
               if run_end else None)
    selected = []
    for path in directory.glob("spans-????-??-??.jsonl"):
        try:
            day = datetime.strptime(path.name[6:16], "%Y-%m-%d").date()
        except ValueError:
            continue
        if start_day and day < start_day:
            continue
        if end_day and day > end_day:
            continue
        selected.append(path)
    return sorted(selected)


def merge_persisted_spans(stats, journal, telemetry_root=None, submitted=0,
                          run_end=None, job_ids=()):
    """Combine live attempts with completed attempts retained across master restarts."""
    stats = dict(stats or {})
    spans = {}
    paths = [Path(journal)]
    if telemetry_root:
        paths.extend(_dated_journals(telemetry_root, submitted, run_end))
    for path in paths:
        for span in _journal_spans(path, job_ids):
            try:
                started = int(span["started_at"])
                if submitted and started < submitted or run_end and started >= run_end:
                    continue
                key = (span["job_id"], span["attempt"], started)
                spans[key] = span
            except (ValueError, KeyError, TypeError):
                continue
    for span in stats.get("execution_spans", []):
        if isinstance(span, dict):
            try:
                spans[(span["job_id"], span["attempt"], span["started_at"])] = span
            except KeyError:
                continue
    stats["execution_spans"] = list(spans.values())
    return stats


def timeline_for(dag, stats, run_end=None, span_limit=5000, error_offset=0, error_limit=100):
    jobs = {job["id"]: job for job in dag["jobs"]}
    submitted = int(dag.get("submitted") or 0)
    spans = []
    for raw in (stats or {}).get("execution_spans", []):
        if not isinstance(raw, dict) or raw.get("job_id") not in jobs:
            continue
        try:
            started = int(raw["started_at"])
            queued = int(raw["queued_at"])
            ended = int(raw.get("ended_at") or 0)
            attempt = int(raw.get("attempt") or 1)
        except (KeyError, ValueError, TypeError):
            continue
        if submitted and started < submitted or run_end and started >= run_end:
            continue
        reason = str(raw.get("reason") or "")
        if len(reason) > 2000:
            reason = reason[:100] + "\n…\n" + reason[-1800:]
        spans.append({"job_id": raw["job_id"], "worker": str(raw.get("worker") or "Unassigned"),
                      "queued_at": min(queued, started), "started_at": started,
                      "ended_at": ended, "attempt": attempt,
                      "status": str(raw.get("status") or "RUNNING"),
                      "reason": reason,
                      "parents": jobs[raw["job_id"]]["parents"]})
    spans.sort(key=lambda span: (span["started_at"], span["job_id"], span["attempt"]))
    now_end = max((span["ended_at"] or int(time.time() * 1000) for span in spans), default=submitted)
    start = min((span["queued_at"] for span in spans), default=submitted)
    critical = _critical_path(spans)
    wall_ms = max(0, now_end - start)
    execution_ms = sum(max(0, (span["ended_at"] or now_end) - span["started_at"]) for span in spans)
    workers = len({span["worker"] for span in spans})
    final_by_job = {span["job_id"]: span for span in spans if span["ended_at"]}
    critical_ms = sum(final_by_job[job_id]["ended_at"] - final_by_job[job_id]["started_at"]
                      for job_id in critical)
    failures = [_failure(span) for span in reversed(spans) if span["status"] in ("FAILED", "DEAD")]
    return {"dag_id": dag["id"], "name": dag["name"], "submitted": submitted,
            "spans": spans[-span_limit:] if span_limit else [], "span_total": len(spans),
            "critical_path": critical, "failures": failures[error_offset:error_offset + error_limit],
            "failure_total": len(failures),
            "metrics": {"wall_ms": wall_ms, "critical_ms": critical_ms,
                        "queue_ms": sum(span["started_at"] - span["queued_at"] for span in spans),
                        "failed_attempts": len(failures),
                        "retry_attempts": sum(span["attempt"] > 1 for span in spans),
                        "workers": workers,
                        "peak_parallel": _peak_parallel(spans),
                        "parallelism_factor": round(execution_ms / wall_ms, 2) if wall_ms else 0,
                        "jobs_per_worker": round(execution_ms / (wall_ms * workers), 2)
                        if wall_ms and workers else 0,
                        "attempts": len(spans), "jobs": len(jobs)}}


def _failure(span):
    reason = span["reason"]
    exit_match = re.match(r"COMPLETED\|(-?\d+)\|", reason)
    lines = [line.strip() for line in reason.splitlines() if line.strip()]
    return {"job_id": span["job_id"], "attempt": span["attempt"],
            "worker": span["worker"], "ended_at": span["ended_at"],
            "exit_code": int(exit_match.group(1)) if exit_match else None,
            "message": lines[-1][:500] if lines else "No error message returned."}


def _peak_parallel(spans):
    events = []
    for span in spans:
        events.extend(((span["started_at"], 1), (span["ended_at"] or int(time.time() * 1000), -1)))
    active = peak = 0
    for _, change in sorted(events, key=lambda item: (item[0], item[1])):
        active = max(0, active + change)
        peak = max(peak, active)
    return peak


def _critical_path(spans):
    """Trace the latest-finishing parent from the last finished job."""
    final = {}
    for span in spans:
        if span["ended_at"]:
            final[span["job_id"]] = span
    if not final:
        return []
    job_id = max(final, key=lambda jid: final[jid]["ended_at"])
    path = []
    while job_id in final and job_id not in path:
        path.append(job_id)
        parents = [parent for parent in final[job_id]["parents"] if parent in final]
        if not parents:
            break
        job_id = max(parents, key=lambda parent: final[parent]["ended_at"])
    return list(reversed(path))
