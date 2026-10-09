"""The documented SDK → worker gate → TitanStore HITL contract."""

import base64
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "perm_files"))
sys.path.insert(0, str(ROOT))

from titan_sdk.titan_sdk import TitanClient, TitanJob, OP_KV_SET, OP_SUBMIT_DAG
from titan_sdk.dag_journal import latest_run
from titan_sdk.hitl_gate import run
from dag_viewer.routes import create_blueprint
from dag_viewer.submission import redeploy
from flask import Flask


class FakeStore:
    def __init__(self):
        self.values = {}
        self.members = set()

    def get(self, key):
        return self.values.get(key, "NULL")

    def put(self, key, value):
        self.values[key] = value

    def add(self, key, member):
        self.members.add((key, member))


class HitlTest(unittest.TestCase):
    def test_sdk_injects_gate_rewires_children_and_clears_old_decision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "job.py"
            script.write_text("print('done')\n", encoding="utf-8")
            manifest = root / "manifest.json"
            source = TitanJob("preprocess", str(script), hitl_message="Review data", max_wait_seconds=90)
            child = TitanJob("train", str(script), parents=["preprocess"])
            calls = []
            sdk = TitanClient()
            sdk._send_request = lambda op, payload: calls.append((op, payload)) or (
                "OK" if op == OP_KV_SET else "DAG_ACCEPTED")
            with patch.dict(os.environ, {"TITAN_DAG_MANIFEST": str(manifest)}):
                self.assertEqual("DAG_ACCEPTED", sdk.submit_dag("review", [source, child]))
            self.assertEqual(["preprocess"], child.parents)  # User objects are reusable.
            self.assertEqual((OP_KV_SET, "titan:hitl:status:DAG-hitl-gate-preprocess|CLEARED"), calls[0])
            self.assertEqual(OP_SUBMIT_DAG, calls[1][0])
            saved = latest_run(manifest, "review")["jobs"]
            self.assertEqual(["DAG-hitl-gate-preprocess"], saved["DAG-train"]["deps"])
            self.assertEqual(["DAG-preprocess"], saved["DAG-hitl-gate-preprocess"]["deps"])
            self.assertEqual("Review data", saved["DAG-hitl-gate-preprocess"]["spec"]["hitl_message"])
            self.assertEqual(3, len(saved))
            self.assertIn("DAG-hitl-gate-preprocess 90 " + base64.b64encode(b"Review data").decode(),
                          calls[1][1])

    def test_gate_approval_rejection_timeout_and_retry_safety(self):
        key = "titan:hitl:status:DAG-hitl-gate-preprocess"
        store = FakeStore()
        ticks = [100.0]
        def sleep(seconds):
            ticks[0] += seconds
            store.put(key, "APPROVED")
        self.assertEqual(0, run(store, "DAG-hitl-gate-preprocess", "Review", 30,
                                now=lambda: ticks[0], sleep=sleep))
        self.assertEqual("APPROVED", store.get(key))
        self.assertEqual(0, run(store, "DAG-hitl-gate-preprocess", "Review", 30,
                                now=lambda: ticks[0], sleep=sleep))
        store.put(key, "REJECTED")
        self.assertEqual(1, run(store, "DAG-hitl-gate-preprocess", "Review", 30,
                                now=lambda: ticks[0], sleep=sleep))
        store.put(key, "CLEARED")
        self.assertEqual(1, run(store, "DAG-hitl-gate-preprocess", "Review", 1,
                                now=lambda: ticks[0], sleep=lambda seconds: ticks.__setitem__(0, ticks[0] + seconds)))
        self.assertEqual("TIMEOUT", store.get(key))

    def test_viewer_decision_targets_only_a_running_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "DAG-hitl-gate-preprocess": {"dag": "review", "deps": [], "run_ts": 1000,
                                             "spec": {"hitl_message": "Review data"}},
                "DAG-other": {"dag": "review", "deps": [], "run_ts": 1000},
            }))
            status = {"titan:hitl:status:DAG-hitl-gate-preprocess": "WAITING"}
            calls = []
            def communicate(op, payload):
                calls.append((op, payload))
                if op == 9:
                    return json.dumps({"dag_jobs": {"DAG-hitl-gate-preprocess": {
                        "status": "RUNNING", "completed_at": 0}}, "workers": []})
                if op == 0x61:
                    return status.get(payload, "NULL")
                if op == 0x60:
                    key, value = payload.split("|", 1)
                    status[key] = value
                    return "OK"
                return ""
            app = Flask(__name__)
            app.register_blueprint(create_blueprint(communicate, 9, 22, manifest, root))
            with app.test_client() as client:
                gates = client.get("/api/dag/hitl/DAG-review").json["gates"]
                self.assertEqual("WAITING", gates[0]["decision"])
                self.assertEqual(404, client.post("/api/dag/hitl/DAG-review/DAG-other",
                                                   json={"decision": "APPROVED"}).status_code)
                response = client.post("/api/dag/hitl/DAG-review/DAG-hitl-gate-preprocess",
                                       json={"decision": "APPROVED"})
                self.assertEqual(200, response.status_code)
                self.assertEqual((0x60, "titan:hitl:status:DAG-hitl-gate-preprocess|APPROVED"), calls[-1])
                self.assertEqual(409, client.post("/api/dag/hitl/DAG-review/DAG-hitl-gate-preprocess",
                                                   json={"decision": "REJECTED"}).status_code)

    def test_redeploy_clears_previous_gate_decision_before_submitting(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "manifest.json"
            manifest.write_text(json.dumps({
                "DAG-hitl-gate-preprocess": {"dag": "review", "deps": [], "run_ts": 1000,
                                             "spec": {"hitl_message": "Review data"}},
                "__payload__review": {"dag": "review", "dag_payload": "saved payload", "run_ts": 1000},
            }))
            calls = []
            def communicate(op, payload):
                calls.append((op, payload))
                return "OK" if op == 0x60 else "DAG_ACCEPTED"
            self.assertEqual("DAG_ACCEPTED", redeploy(communicate, 4, manifest, "review"))
            self.assertEqual((0x60, "titan:hitl:status:DAG-hitl-gate-preprocess|CLEARED"), calls[0])
            self.assertEqual((4, "saved payload"), calls[1])


if __name__ == "__main__":
    unittest.main()
