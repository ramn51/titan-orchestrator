"""Cluster Monitor RPC wiring and run-boundary checks."""

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cluster_monitor.routes import create_blueprint, _history_days
from cluster_monitor.demos import PRESETS, build, recover_latest
from dag_viewer.submission import normalize


class ClusterMonitorTests(unittest.TestCase):
    def test_presets_are_bounded_valid_dags(self):
        scripts = Path(__file__).resolve().parents[1] / "dag_samples"
        self.assertEqual({"basic", "fan_out", "deep_dag", "saturation_burst",
                          "long_saturation", "load"}, set(PRESETS))
        for preset in PRESETS:
            with self.subTest(preset=preset):
                name, graph = build(preset)
                normalized_name, jobs = normalize({"name": name, "jobs": graph}, scripts)
                self.assertEqual(name, normalized_name)
                self.assertLessEqual(len(jobs), 36)
                self.assertTrue(all(job["path"].is_file() for job in jobs))

    def test_demo_endpoint_submits_and_reports_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            calls = []

            def communicate(opcode, payload="", retries=1):
                calls.append((opcode, payload))
                if opcode == 0x09 and payload == "metrics|0|1":
                    return '{"samples":[]}'
                if opcode == 0x09 and payload.startswith("status|"):
                    return json.dumps({job_id: {"status": "PENDING"}
                                       for job_id in payload.removeprefix("status|").split(",")})
                if opcode == 0x04:
                    return "DAG_ACCEPTED"
                raise AssertionError(opcode)

            app = Flask(__name__)
            app.register_blueprint(create_blueprint(communicate, Path(directory) / "manifest.json"))
            client = app.test_client()
            self.assertEqual(400, client.post("/api/cluster/demo", json={"preset": "unknown"}).status_code)
            response = client.post("/api/cluster/demo", json={"preset": "fan_out"})
            self.assertEqual(201, response.status_code)
            self.assertEqual(10, len(response.json["demo"]["job_ids"]))
            self.assertEqual(409, client.post("/api/cluster/demo", json={"preset": "load"}).status_code)
            self.assertEqual("fan_out", client.get("/api/cluster/demo").json["demo"]["preset"])
            self.assertTrue(any(opcode == 0x04 for opcode, _ in calls))
            self.assertEqual("fan_out", recover_latest(Path(directory) / "manifest.json")["preset"])

    def test_snapshot_reads_bounded_engine_views(self):
        with tempfile.TemporaryDirectory() as directory:
            calls = []

            since = int(datetime.now(timezone.utc).timestamp() * 1000) - 60_000
            def recent_communicate(opcode, payload="", retries=1):
                calls.append((opcode, payload, retries))
                return {"": '{"active_workers":2}',
                        "queue|500": '{"counts":{"ready":1},"jobs":[]}',
                        f"metrics|{since}|900": '{"samples":[{"at":10}],"events":[]}',
                        f"timeline|{since}|5000": '[{"job_id":"sample","started_at":10}]'}[payload]

            app = Flask(__name__)
            app.register_blueprint(create_blueprint(recent_communicate, Path(directory) / "manifest.json"))
            response = app.test_client().get(f"/api/cluster?mode=live&since={since}")
            self.assertEqual(200, response.status_code)
            self.assertTrue(response.json["online"])
            self.assertEqual(1, response.json["board"]["counts"]["ready"])
            self.assertEqual("sample", response.json["spans"][0]["job_id"])
            self.assertIn((0x09, f"metrics|{since}|900", 1), calls)
            self.assertFalse(any(payload.startswith("history|") for _, payload, _ in calls))
            self.assertTrue(all(opcode == 0x09 for opcode, _, _ in calls))

    def test_wide_live_window_merges_durable_span_history(self):
        with tempfile.TemporaryDirectory() as directory:
            since = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp() * 1000)
            days = _history_days(since)
            calls = []

            def communicate(opcode, payload="", retries=1):
                calls.append(payload)
                if payload == "": return '{}'
                if payload == "queue|500": return '{}'
                if payload == f"metrics|{since}|900": return '{"samples":[]}'
                if payload == f"timeline|{since}|5000": return '[]'
                if payload.startswith("history|"):
                    return json.dumps([{"job_id": "historical", "started_at": since + 1,
                                        "attempt": 1, "status": "COMPLETED"}])
                raise AssertionError(payload)

            app = Flask(__name__)
            app.register_blueprint(create_blueprint(communicate, Path(directory) / "manifest.json"))
            result = app.test_client().get(f"/api/cluster?mode=live&since={since}").json
            self.assertTrue(result["history"]["included"])
            self.assertEqual(days, result["history"]["days"])
            self.assertEqual(["historical"], [span["job_id"] for span in result["spans"]])
            self.assertTrue(any(payload.startswith("history|") for payload in calls))

    def test_analytics_includes_manifest_runs_and_deduplicates_history(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "manifest.json"
            today = datetime.now(timezone.utc).date()
            yesterday = today - timedelta(days=1)
            since = int(datetime.combine(yesterday, datetime.min.time(),
                                         tzinfo=timezone.utc).timestamp() * 1000)
            first_ts = since + 100
            second_ts = since + 200
            manifest.write_text(json.dumps({"__runs__demo": [
                {"run_ts": first_ts, "jobs": {"DAG-A": {"deps": []}}}]}), encoding="utf-8")
            (Path(directory) / ".titan_dag_run_history.jsonl").write_text(
                json.dumps({"name": "indexed", "run_ts": second_ts,
                            "jobs": ["DAG-B"]}) + "\n", encoding="utf-8")
            old_span = {"job_id": "DAG-A", "started_at": first_ts, "attempt": 1}
            new_span = {"job_id": "DAG-B", "started_at": second_ts, "attempt": 1}

            def communicate(opcode, payload="", retries=1):
                return {"": '{}', "queue|500": '{}', f"metrics|{since}|900": '{}',
                        f"timeline|{since}|5000": json.dumps([new_span]),
                        f"history|{yesterday.isoformat()}|5000": json.dumps([old_span]),
                        f"history|{today.isoformat()}|5000": json.dumps([new_span])}[payload]

            app = Flask(__name__)
            app.register_blueprint(create_blueprint(communicate, manifest))
            result = app.test_client().get(f"/api/cluster?mode=analytics&since={since}").json
            self.assertEqual(["DAG-A", "DAG-B"], [span["job_id"] for span in result["spans"]])
            self.assertEqual([first_ts, second_ts], [run["run_ts"] for run in result["runs"]])
            self.assertEqual(["DAG-B"], result["runs"][1]["jobs"])


if __name__ == "__main__":
    unittest.main()
