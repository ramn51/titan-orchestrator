"""Focused contracts for DAG grouping and live status (no running cluster needed)."""

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dag_viewer.catalog import DagCatalog
from dag_viewer.graph import PREVIEW_LIMIT, dot_graph, preview_dag, render_graph
from dag_viewer.routes import create_blueprint
from flask import Flask
from tempfile import TemporaryDirectory
from unittest.mock import patch
from dag_viewer.submission import read_manifest


def test_catalog_reuses_unchanged_manifest_and_invalidates_on_write(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "DAG-A": {"dag": "first", "deps": [], "run_ts": 1000}
    }), encoding="utf-8")
    catalog = DagCatalog(manifest, tmp_path)
    with patch("dag_viewer.catalog.read_manifest", wraps=read_manifest) as reader:
        first = catalog.snapshot({"workers": []})
        second = catalog.snapshot({"workers": []})
        assert first is second
        assert reader.call_count == 1
        manifest.write_text(json.dumps({
            "DAG-A": {"dag": "first", "deps": [], "run_ts": 1000},
            "DAG-B": {"dag": "second", "deps": [], "run_ts": 2000},
        }), encoding="utf-8")
        refreshed = catalog.snapshot({"workers": []})
        assert len(refreshed) == 2
        assert reader.call_count == 2


def test_service_ready_and_failed_statuses(tmp_path):
    manifest = tmp_path / ".titan_dag_manifest.json"
    manifest.write_text(json.dumps({
        "DAG-service": {"dag": "service-demo", "deps": [], "run_ts": 1000,
                        "spec": {"job_type": "service", "port": 8123}},
        "DAG-child": {"dag": "service-demo", "deps": ["DAG-service"], "run_ts": 1000,
                      "spec": {"job_type": "run"}},
    }))
    catalog = DagCatalog(manifest, tmp_path)
    stats = {"workers": [{"port": 8080, "services": ["DAG-service"], "history": []}],
             "dag_jobs": {"DAG-service": {"status": "COMPLETED", "completed_at": 2000},
                          "DAG-child": {"status": "COMPLETED", "completed_at": 2500}}}
    endpoints = [{"id": "DAG-service", "host": "127.0.0.1", "port": 8123, "since": 2000}]
    ready = catalog.snapshot(stats, endpoints)[0]
    assert ready["status"] == "COMPLETED"
    service = next(job for job in ready["jobs"] if job["id"] == "DAG-service")
    assert service["service"]["url"] == "http://127.0.0.1:8123"
    assert "SERVICE" in render_graph(ready)

    stats["workers"][0]["services"] = []
    stats["dag_jobs"]["DAG-service"] = {"status": "FAILED", "completed_at": 2000}
    stats["dag_jobs"]["DAG-child"] = {"status": "CANCELLED", "completed_at": 2100}
    failed = catalog.snapshot(stats)[0]
    assert failed["status"] == "FAILED"
    assert next(job for job in failed["jobs"] if job["id"] == "DAG-service")["service"] is None


def test_old_run_is_not_shown_as_complete(tmp_path):
    manifest = tmp_path / ".titan_dag_manifest.json"
    manifest.write_text(json.dumps({"DAG-A": {"dag": "demo", "deps": [], "run_ts": 2000},
                                    "DAG-B": {"dag": "demo", "deps": ["DAG-A"], "run_ts": 2000}}))
    catalog = DagCatalog(manifest, tmp_path)
    stats = {"workers": [{"port": 8080, "history": [
        {"id": "DAG-A", "status": "COMPLETED", "completed_at": 1000}]}]}
    dag = catalog.snapshot(stats)[0]
    assert dag["status"] == "UNKNOWN"
    assert dag["completed"] == 0
    assert next(j for j in dag["jobs"] if j["id"] == "DAG-B")["parents"] == ["DAG-A"]

    stats["workers"][0]["history"][0]["completed_at"] = 2500
    assert catalog.snapshot(stats)[0]["completed"] == 1


def test_terminal_status_survives_restart_but_not_a_new_run(tmp_path):
    manifest = tmp_path / ".titan_dag_manifest.json"
    manifest.write_text(json.dumps({"DAG-A": {"dag": "demo", "deps": [], "run_ts": 2000}}))
    stats = {"workers": [{"port": 8080, "history": [
        {"id": "DAG-A", "status": "COMPLETED", "completed_at": 2500}]}]}
    assert DagCatalog(manifest, tmp_path).snapshot(stats)[0]["status"] == "COMPLETED"
    restarted = DagCatalog(manifest, tmp_path)
    assert restarted.snapshot({"workers": []})[0]["status"] == "COMPLETED"

    manifest.write_text(json.dumps({"DAG-A": {"dag": "demo", "deps": [], "run_ts": 3000}}))
    new_run = DagCatalog(manifest, tmp_path).snapshot({"workers": []})[0]
    assert new_run["status"] == "UNKNOWN"
    assert new_run["completed"] == 0


def test_unstarted_child_is_waiting_during_an_active_run(tmp_path):
    manifest = tmp_path / ".titan_dag_manifest.json"
    manifest.write_text(json.dumps({
        "DAG-A": {"dag": "demo", "deps": [], "run_ts": 2000},
        "DAG-B": {"dag": "demo", "deps": ["DAG-A"], "run_ts": 2000},
    }))
    stats = {"dag_jobs": {"DAG-A": {"status": "RUNNING", "completed_at": 0}}}
    dag = DagCatalog(manifest, tmp_path).snapshot(stats)[0]
    assert dag["status"] == "RUNNING"
    assert next(job for job in dag["jobs"] if job["id"] == "DAG-B")["status"] == "WAITING"


def test_capacity_rejection_is_not_reported_as_a_job_retry(tmp_path):
    manifest = tmp_path / ".titan_dag_manifest.json"
    manifest.write_text(json.dumps({"DAG-A": {"dag": "demo", "deps": [], "run_ts": 1000}}))
    stats = {"dag_jobs": {"DAG-A": {"status": "COMPLETED", "completed_at": 3000}},
             "execution_spans": [
                 {"job_id": "DAG-A", "started_at": 1500, "status": "REJECTED"},
                 {"job_id": "DAG-A", "started_at": 2000, "status": "COMPLETED"}]}
    job = DagCatalog(manifest, tmp_path).snapshot(stats)[0]["jobs"][0]
    assert job["attempts"] == 1
    assert job["retry_count"] == 0


def test_downloaded_svg_contains_its_own_background_and_status_colors(tmp_path):
    dag = {"id": "DAG-demo", "jobs": [
        {"id": "DAG-A", "status": "COMPLETED", "parents": [], "spec": {}},
        {"id": "DAG-B", "status": "WAITING", "parents": ["DAG-A"], "spec": {}}]}
    svg = render_graph(dag, standalone=True)
    assert '<style type="text/css">' in svg
    assert 'fill="#0f1a1b"' in svg
    assert 'fill:#35ae73' in svg
    assert 'stroke:#83c6f4' in svg
    assert 'data-status="COMPLETED"' in svg


def test_yaml_definition_and_live_poll(tmp_path):
    (tmp_path / "workflow.yaml").write_text(
        "name: analysis\njobs:\n  - id: prep\n  - id: model\n    depends_on: [prep]\n")
    replies = iter([
        {"workers": [{"port": 8080, "active_job": "DAG-prep", "history": []}]},
        {"workers": [{"port": 8080, "active_job": "DAG-model", "history": [
            {"id": "DAG-prep", "status": "COMPLETED", "completed_at": 50}]}]},
    ])
    app = Flask(__name__)
    app.register_blueprint(create_blueprint(
        lambda _op, _payload: json.dumps(next(replies)), 0x09, 0x16,
        tmp_path / ".titan_dag_manifest.json", tmp_path))
    with app.test_client() as client:
        first = client.get("/api/dag_status").json["dags"][0]
        second = client.get("/api/dag_status").json["dags"][0]
    assert first["status"] == "RUNNING"
    assert second["completed"] == 1
    assert next(j for j in second["jobs"] if j["id"] == "DAG-model")["status"] == "RUNNING"


def test_current_failure_overrides_old_worker_history(tmp_path):
    manifest = tmp_path / ".titan_dag_manifest.json"
    manifest.write_text(json.dumps({"DAG-A": {"dag": "broken", "deps": [], "run_ts": 2000},
                                    "DAG-B": {"dag": "broken", "deps": ["DAG-A"], "run_ts": 2000}}))
    stats = {"workers": [{"port": 8080, "history": [
        {"id": "DAG-A", "status": "COMPLETED", "completed_at": 2500}]}],
        "dag_jobs": {"DAG-A": {"status": "DEAD", "completed_at": 2600,
                               "worker": "127.0.0.1:8080", "time": "20ms"}}}
    dag = DagCatalog(manifest, tmp_path).snapshot(stats)[0]
    assert dag["status"] == "FAILED"
    assert dag["completed"] == 0
    assert dag["jobs"][0]["status"] == "FAILED"
    assert 'width="' in render_graph(dag) and 'height="' in render_graph(dag)


def test_workspace_files_and_download_are_scoped_to_selected_job(tmp_path):
    manifest = tmp_path / ".titan_dag_manifest.json"
    run_started = int(time.time() * 1000) - 1000
    old_log = b"previous run output\n"
    current_log = b"job output"
    manifest.write_text(json.dumps({"DAG-FIRST": {"dag": "demo", "deps": [],
                                                   "run_ts": run_started, "log_offset": len(old_log)}}))
    shared = tmp_path / "titan_workspace" / "shared"
    shared.mkdir(parents=True)
    (shared / "DAG-FIRST.log").write_bytes(old_log + current_log)
    (shared / "DAG-OTHER.log").write_text("other output", encoding="utf-8")
    (shared / "result.csv").write_text("a,b\n1,2", encoding="utf-8")
    (shared / "output").mkdir()
    (shared / "output" / "summary.json").write_text('{"ok": true}', encoding="utf-8")
    artifact = shared / "artifacts" / "DAG-FIRST"
    artifact.mkdir(parents=True)
    (artifact / "result.csv").write_text("job result", encoding="utf-8")
    other_artifact = shared / "artifacts" / "DAG-OTHER"
    other_artifact.mkdir()
    (other_artifact / "private.csv").write_text("other result", encoding="utf-8")
    (shared / "stale.csv").write_text("old run", encoding="utf-8")
    os.utime(shared / "stale.csv", (1, 1))
    app = Flask(__name__)
    app.register_blueprint(create_blueprint(lambda *_: json.dumps({"workers": []}), 9, 22,
                                             manifest, tmp_path))
    with app.test_client() as client:
        response = client.get("/api/dag/files/DAG-demo/DAG-FIRST")
        assert response.status_code == 200
        files = {entry["name"]: entry for entry in response.json["files"]}
        assert set(files) == {"DAG-FIRST.log", "artifacts/DAG-FIRST/result.csv"}
        assert files["DAG-FIRST.log"]["size"] == len(current_log)
        downloaded = client.get(files["DAG-FIRST.log"]["url"])
        assert downloaded.data == current_log
        assert "attachment" in downloaded.headers["Content-Disposition"]
        downloaded.close()
        nested = client.get(files["artifacts/DAG-FIRST/result.csv"]["url"])
        assert nested.status_code == 200 and nested.data == b"job result"
        nested.close()
        shared_files = {entry["name"]: entry for entry in client.get("/api/dag/shared_files").json["files"]}
        assert set(shared_files) == {"result.csv", "output/summary.json", "stale.csv"}
        shared_download = client.get(shared_files["output/summary.json"]["url"])
        assert shared_download.status_code == 200 and shared_download.data == b'{"ok": true}'
        shared_download.close()
        assert client.get("/api/dag/files/DAG-demo/DAG-FIRST/DAG-OTHER.log").status_code == 404
        assert client.get("/api/dag/files/DAG-demo/DAG-FIRST/artifacts/DAG-OTHER/private.csv").status_code == 404
        assert client.get("/api/dag/files/DAG-demo/DAG-OTHER").status_code == 404
        assert client.get("/api/dag/files/DAG-demo/DAG-FIRST/../result.csv").status_code == 404
        assert client.get("/api/dag/shared_file/../DAG-FIRST.log").status_code == 404


def test_cancel_routes_target_only_jobs_in_selected_pipeline(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "DAG-A": {"dag": "demo", "deps": [], "run_ts": 0},
        "DAG-B": {"dag": "demo", "deps": ["DAG-A"], "run_ts": 0},
    }))
    stats = {"workers": [], "dag_jobs": {
        "DAG-A": {"status": "RUNNING", "completed_at": 0},
        "DAG-B": {"status": "WAITING", "completed_at": 0},
    }}
    calls = []

    def communicate(opcode, payload):
        if opcode == 9:
            return json.dumps(stats)
        calls.append((opcode, payload))
        return "CANCELLED:1"

    app = Flask(__name__)
    app.register_blueprint(create_blueprint(communicate, 9, 22, manifest, tmp_path))
    with app.test_client() as client:
        assert client.post("/api/dag/cancel/DAG-demo/DAG-A").json["status"] == "ok"
        assert calls == [(0x56, "DAG-A")]
        assert client.post("/api/dag/cancel/DAG-demo/DAG-OUTSIDE").status_code == 404
        assert client.post("/api/dag/cancel/DAG-demo").json["status"] == "ok"
        assert calls[1:] == [(0x56, "DAG-A"), (0x56, "DAG-B")]
        stats["dag_jobs"]["DAG-A"]["status"] = "CANCELLED"
        assert client.post("/api/dag/cancel/DAG-demo/DAG-A").status_code == 409


def test_page_fetches_logs_for_automatically_selected_job(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "DAG-FIRST": {"dag": "demo", "deps": [], "run_ts": 0},
    }))
    calls = []

    def communicate(opcode, payload):
        calls.append((opcode, payload))
        return json.dumps({"workers": []}) if opcode == 9 else "first job logs"

    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates"),
                static_folder=str(Path(__file__).resolve().parents[1] / "static"))
    app.add_url_rule("/logs/<job_id>", "view_logs", lambda job_id: job_id)
    app.register_blueprint(create_blueprint(communicate, 9, 22, manifest, tmp_path))
    with app.test_client() as client:
        response = client.get("/dags/DAG-demo")
    assert response.status_code == 200
    assert (22, "DAG-FIRST") in calls
    assert (22, None) not in calls


def test_pipeline_pages_search_and_full_graph_exports(tmp_path):
    manifest = tmp_path / "manifest.json"
    saved = {f"DAG-job-{number}": {"dag": f"pipeline-{number:02}", "deps": [],
                                   "run_ts": 1000 + number} for number in range(65)}
    saved.update({f"DAG-large-{number}": {"dag": "large", "deps":
                  [f"DAG-large-{number - 1}"] if number else [], "run_ts": 2000}
                  for number in range(PREVIEW_LIMIT + 1)})
    manifest.write_text(json.dumps(saved))
    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates"),
                static_folder=str(Path(__file__).resolve().parents[1] / "static"))
    app.add_url_rule("/logs/<job_id>", "view_logs", lambda job_id: job_id)
    app.register_blueprint(create_blueprint(lambda *_: json.dumps({"workers": []}), 9, 22,
                                            manifest, tmp_path))
    with app.test_client() as client:
        first = client.get("/dags")
        assert first.status_code == 200
        assert first.data.count(b'class="dag-link') == 30
        assert first.data.index(b"pipeline-64") < first.data.index(b"pipeline-63")
        second = client.get("/dags?page=2")
        assert second.status_code == 200 and second.data.count(b'class="dag-link') == 30
        third = client.get("/dags?page=3")
        assert third.status_code == 200 and third.data.count(b'class="dag-link') == 6
        search = client.get("/dags?q=pipeline-6")
        assert search.status_code == 200 and search.data.count(b'class="dag-link') == 5
        assert b"Page 1 of 1" in search.data
        preview = client.get("/dags/DAG-large")
        assert preview.status_code == 200
        assert b"Showing the first 120" in preview.data
        assert preview.data.count(b'<g class="node') == PREVIEW_LIMIT
        svg = client.get("/api/dag/graph/DAG-large/svg")
        assert svg.status_code == 200 and svg.data.count(b'<g class="node') == PREVIEW_LIMIT + 1
        assert "attachment" in svg.headers["Content-Disposition"]
        dot = client.get("/api/dag/graph/DAG-large/dot")
        assert dot.status_code == 200 and b'"DAG-large-120"' in dot.data
        assert b'"DAG-large-119" -> "DAG-large-120"' in dot.data
        poll = client.get("/api/dag_status?view=page&page=2&selected=DAG-large").json
        assert poll["page"] == 2 and poll["total"] == 66
        assert len(poll["dags"]) == 31
        assert len(next(dag for dag in poll["dags"] if dag["id"] == "DAG-large")["jobs"]) == PREVIEW_LIMIT + 1


def test_deep_graph_preview_avoids_recursion(tmp_path):
    jobs = [{"id": f"DAG-{number}", "parents": [f"DAG-{number - 1}"] if number else [],
             "status": "WAITING"} for number in range(1500)]
    dag = {"id": "DAG-deep", "jobs": jobs}
    assert len(preview_dag(dag)["jobs"]) == PREVIEW_LIMIT
    assert render_graph(dag).count('<g class="node') == len(jobs)
    assert dot_graph(dag).count(" -> ") == len(jobs) - 1


if __name__ == "__main__":
    for case in (test_catalog_reuses_unchanged_manifest_and_invalidates_on_write,
                 test_service_ready_and_failed_statuses,
                 test_old_run_is_not_shown_as_complete, test_terminal_status_survives_restart_but_not_a_new_run,
                 test_unstarted_child_is_waiting_during_an_active_run,
                 test_yaml_definition_and_live_poll,
                 test_current_failure_overrides_old_worker_history,
                 test_workspace_files_and_download_are_scoped_to_selected_job,
                 test_cancel_routes_target_only_jobs_in_selected_pipeline,
                 test_page_fetches_logs_for_automatically_selected_job,
                 test_pipeline_pages_search_and_full_graph_exports,
                 test_deep_graph_preview_avoids_recursion):
        with TemporaryDirectory() as directory:
            case(Path(directory))
        print(f"PASS {case.__name__}")
