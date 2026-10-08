"""Run-scoped timeline and history checks using Python's built-in unittest."""

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "perm_files"))
sys.path.insert(0, str(ROOT))

from dag_viewer.catalog import DagCatalog
from dag_viewer.timeline import merge_persisted_spans, timeline_for
from dag_viewer.routes import create_blueprint
from flask import Flask

from titan_sdk.titan_sdk import TitanClient, TitanJob


class ExecutionTimelineTests(unittest.TestCase):
    def test_day_partitioned_journal_survives_master_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            telemetry = root / "telemetry"
            telemetry.mkdir()
            submitted = 1790402053406
            wanted = {"job_id": "DAG-A", "queued_at": submitted + 10,
                      "started_at": submitted + 20, "ended_at": submitted + 30,
                      "attempt": 1, "status": "COMPLETED", "worker": "host:8080"}
            unrelated = dict(wanted, job_id="DAG-OTHER")
            journal = telemetry / "spans-2026-09-26.jsonl"
            journal.write_text(json.dumps(unrelated) + "\n" + json.dumps(wanted) + "\n",
                               encoding="utf-8")
            merged = merge_persisted_spans({}, root / ".titan_execution_timeline.jsonl",
                                           telemetry, submitted, None, ("DAG-A",))
            self.assertEqual(["DAG-A"], [span["job_id"] for span in merged["execution_spans"]])

    def test_attempts_are_scoped_to_run_and_critical_chain(self):
        dag = {"id": "DAG-demo", "name": "demo", "submitted": 2000,
               "jobs": [{"id": "DAG-A", "parents": []},
                        {"id": "DAG-B", "parents": ["DAG-A"]}]}
        def span(job_id, queued, started, ended, attempt=1, status="COMPLETED"):
            return {"job_id": job_id, "queued_at": queued, "started_at": started,
                    "ended_at": ended, "attempt": attempt, "status": status,
                    "worker": "localhost:8080"}
        stats = {"execution_spans": [span("DAG-A", 100, 100, 200),
                                     span("DAG-A", 2000, 2100, 2200, status="FAILED"),
                                     span("DAG-A", 2220, 2230, 2330, attempt=2),
                                     span("DAG-B", 2330, 2350, 2450)]}
        timeline = timeline_for(dag, stats)
        self.assertEqual(3, timeline["metrics"]["attempts"])
        self.assertEqual(1, timeline["metrics"]["failed_attempts"])
        self.assertEqual(["DAG-A", "DAG-B"], timeline["critical_path"])
        self.assertEqual(130, timeline["metrics"]["queue_ms"])

    def test_historical_statuses_do_not_take_current_run_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / ".titan_dag_manifest.json"
            manifest.write_text(json.dumps({
                "DAG-A": {"dag": "demo", "deps": [], "run_ts": 3000},
                "__runs__demo": [
                    {"run_ts": 2000, "jobs": {"DAG-A": {"deps": [], "spec": {"script": "old.py"}}}},
                    {"run_ts": 3000, "jobs": {"DAG-A": {"deps": [], "spec": {"script": "new.py"}}}},
                ]}))
            catalog = DagCatalog(manifest, root)
            catalog._remember_statuses({"DAG-A": {"run_ts": 2000, "status": "FAILED"}})
            catalog._remember_statuses({"DAG-A": {"run_ts": 3000, "status": "COMPLETED"}})
            old = catalog.historical_snapshot("demo", 2000)
            self.assertEqual("FAILED", old["status"])
            self.assertEqual("old.py", old["jobs"][0]["spec"]["script"])
            self.assertEqual("COMPLETED", catalog.historical_snapshot("demo", 3000)["status"])

    def test_failure_summary_reports_exit_code_and_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / ".titan_dag_manifest.json"
            manifest.write_text(json.dumps({"DAG-A": {"dag": "demo", "deps": [], "run_ts": 2000}}))
            spans = [{"job_id": "DAG-A", "started_at": 2100 + attempt * 100,
                      "reason": "COMPLETED|1|starting\nvalidation failed", "status": "FAILED"}
                     for attempt in range(4)]
            stats = {"execution_spans": spans,
                     "dag_jobs": {"DAG-A": {"status": "DEAD", "completed_at": 2600}}}
            job = DagCatalog(manifest, root).snapshot(stats)[0]["jobs"][0]
            self.assertEqual(4, job["attempts"])
            self.assertEqual(3, job["retry_count"])
            self.assertEqual(1, job["exit_code"])
            self.assertEqual("validation failed", job["last_error"])

    def test_five_thousand_spans_keep_full_metrics_with_bounded_response(self):
        jobs = [{"id": f"DAG-J{i}", "parents": []} for i in range(5000)]
        spans = [{"job_id": f"DAG-J{i}", "queued_at": 2000 + i * 10,
                  "started_at": 2000 + i * 10, "ended_at": 2100 + i * 10,
                  "attempt": 1, "status": "COMPLETED", "worker": "host:8080"}
                 for i in range(5000)]
        dag = {"id": "DAG-big", "name": "big", "submitted": 2000, "jobs": jobs}
        result = timeline_for(dag, {"execution_spans": spans}, span_limit=100)
        self.assertEqual(5000, result["span_total"])
        self.assertEqual(100, len(result["spans"]))
        self.assertEqual("DAG-J4900", result["spans"][0]["job_id"])
        self.assertGreater(result["metrics"]["parallelism_factor"], 1)
        self.assertEqual(0, result["failure_total"])

    def test_failed_attempts_paginate_below_timeline(self):
        dag = {"id": "DAG-errors", "name": "errors", "submitted": 2000,
               "jobs": [{"id": "DAG-A", "parents": []}]}
        spans = [{"job_id": "DAG-A", "queued_at": 2100 + i * 10,
                  "started_at": 2100 + i * 10, "ended_at": 2105 + i * 10,
                  "attempt": i + 1, "status": "FAILED", "worker": "host:8080",
                  "reason": "COMPLETED|2|bad input\nvalidation failed"} for i in range(5)]
        result = timeline_for(dag, {"execution_spans": spans}, error_offset=2, error_limit=2)
        self.assertEqual(5, result["failure_total"])
        self.assertEqual([3, 2], [error["attempt"] for error in result["failures"]])
        self.assertEqual([2, 2], [error["exit_code"] for error in result["failures"]])

    def test_timeline_is_its_own_page_and_api_uses_run_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / ".titan_dag_manifest.json"
            manifest.write_text(json.dumps({"DAG-A": {"dag": "demo", "deps": [], "run_ts": 3000},
                "__runs__demo": [
                    {"run_ts": 2000, "jobs": {"DAG-A": {"deps": []}}},
                    {"run_ts": 3000, "jobs": {"DAG-A": {"deps": []}}}]}))
            stats = {"execution_spans": [
                {"job_id": "DAG-A", "queued_at": 2100, "started_at": 2200,
                 "ended_at": 2300, "attempt": 1, "status": "COMPLETED"},
                {"job_id": "DAG-A", "queued_at": 3100, "started_at": 3200,
                 "ended_at": 3300, "attempt": 1, "status": "FAILED"}]}
            app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates"))
            app.register_blueprint(create_blueprint(lambda *_: json.dumps(stats), 9, 22, manifest, root))
            with app.test_client() as client:
                page = client.get("/timeline?dag=DAG-demo")
                self.assertEqual(200, page.status_code)
                self.assertIn(b"Dispatch attempts", page.data)
                old = client.get("/api/dag/timeline/DAG-demo?run=2000").json
                current = client.get("/api/dag/timeline/DAG-demo").json
                self.assertEqual([2200], [span["started_at"] for span in old["spans"]])
                self.assertEqual([3200], [span["started_at"] for span in current["spans"]])

    def test_first_sdk_submission_can_redeploy_from_viewer(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "work.py"
            script.write_text("print('sdk')\n", encoding="utf-8")
            manifest = root / ".titan_dag_manifest.json"
            shared = root / "titan_workspace" / "shared"
            shared.mkdir(parents=True)
            log = shared / "DAG-SDK_A.log"
            log.write_bytes(b"previous run\n")
            calls = []
            sdk = TitanClient()
            sdk._send_request = lambda opcode, payload: calls.append((opcode, payload)) or "DAG_ACCEPTED"
            with patch.dict(os.environ, {"TITAN_DAG_MANIFEST": str(manifest)}):
                job = TitanJob("SDK_A", str(script), input_versions={"dataset": "v3"},
                               expected_outputs=["report.csv"])
                self.assertEqual("DAG_ACCEPTED", sdk.submit_dag("SDK_PIPE", [job]))
            saved = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual(calls[0][1], saved["__payload__SDK_PIPE"]["dag_payload"])
            self.assertEqual(1, len(saved["__runs__SDK_PIPE"]))
            self.assertEqual(len(b"previous run\n"), saved["DAG-SDK_A"]["log_offset"])
            self.assertEqual({"dataset": "v3"}, saved["DAG-SDK_A"]["spec"]["input_versions"])
            self.assertEqual(["report.csv"], saved["DAG-SDK_A"]["spec"]["expected_outputs"])
            self.assertTrue(DagCatalog(manifest, root).snapshot({"workers": []})[0]["can_redeploy"])
            def communicate(opcode, payload):
                if opcode == 9:
                    return json.dumps({"workers": []})
                calls.append((opcode, payload))
                return "DAG_ACCEPTED"
            app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates"))
            app.add_url_rule("/logs/<job_id>", "view_logs", lambda job_id: job_id)
            app.register_blueprint(create_blueprint(communicate, 9, 22, manifest, root))
            with app.test_client() as client:
                self.assertIn(b'id="redeploy"', client.get("/dags/DAG-SDK_PIPE").data)
                saved["DAG-SDK_A"]["spec"].pop("expected_outputs")
                manifest.write_text(json.dumps(saved), encoding="utf-8")
                log.write_bytes(b"previous run\nfirst run\n")
                self.assertEqual(200, client.get("/dags/DAG-SDK_PIPE").status_code)
                self.assertEqual(200, client.post("/api/dag/redeploy/DAG-SDK_PIPE").status_code)
            self.assertEqual(calls[0][1], calls[-1][1])
            redeployed = json.loads(manifest.read_text())
            self.assertEqual(2, len(redeployed["__runs__SDK_PIPE"]))
            self.assertEqual(len(b"previous run\nfirst run\n"), redeployed["DAG-SDK_A"]["log_offset"])

    def test_sdk_deploy_script_stages_encoded_file(self):
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "worker.py"
            script.write_bytes(b"print('ready')\n")
            calls = []
            sdk = TitanClient()
            sdk._send_request = lambda opcode, payload: calls.append((opcode, payload)) or "SCRIPT_STAGED:worker.py"
            self.assertEqual("SCRIPT_STAGED:worker.py", sdk.deploy_script(str(script)))
            self.assertEqual(0x57, calls[0][0])
            name, encoded = calls[0][1].split("|", 1)
            self.assertEqual("worker.py", name)
            import base64
            self.assertEqual(script.read_bytes(), base64.b64decode(encoded))


if __name__ == "__main__":
    unittest.main()
