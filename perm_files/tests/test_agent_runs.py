"""Contracts for SDK agent metadata and the stage-level Agent Runs view."""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from flask import Flask

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dag_viewer.agent_runs import AgentRunCatalog
from dag_viewer.routes import create_blueprint
from titan_sdk import TitanClient, TitanJob


def dag(name, submitted, status="COMPLETED", jobs=1):
    return {"id": "DAG-" + name, "name": name, "submitted": submitted,
            "status": status, "completed": jobs if status == "COMPLETED" else 0,
            "total": jobs, "jobs": []}


class AgentRunViewTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.manifest = self.root / "manifest.json"
        self.environment = patch.dict(os.environ, {
            "TITAN_DAG_MANIFEST": str(self.manifest),
            "TITAN_DAG_JOURNAL_DIR": str(self.root / "journal"),
            "TITAN_DAG_RUN_HISTORY": str(self.root / "history.jsonl"),
        })
        self.environment.start()

    def tearDown(self):
        self.environment.stop()
        self.temporary.cleanup()

    def test_sdk_records_agent_metadata_without_changing_wire_payload(self):
        sent = []
        client = TitanClient()
        job = TitanJob("plan", __file__)
        with patch.object(client, "_send_request",
                          side_effect=lambda opcode, payload: sent.append(payload) or "DAG_ACCEPTED"):
            result = client.submit_dag("research_plan", [job], agent_run_id="research:42",
                                       agent_stage="PLAN", agent_stage_order=0,
                                       agent_run_name="Research 42")
        self.assertEqual("DAG_ACCEPTED", result)
        from titan_sdk.dag_journal import current_runs
        metadata = current_runs(self.manifest)["research_plan"]["jobs"]["DAG-plan"]
        self.assertEqual("research:42", metadata["agent_run_id"])
        self.assertEqual("PLAN", metadata["agent_stage"])
        self.assertEqual(0, metadata["agent_stage_order"])
        self.assertNotIn("research:42", sent[0])

    def test_agent_catalog_groups_explicit_stages_and_parallel_columns(self):
        client = TitanClient()
        for name, job_id, stage, order, timestamp in (
                ("plan", "a", "PLAN", 0, 1000),
                ("search-a", "b", "SEARCH A", 1, 2000),
                ("search-b", "c", "SEARCH B", 1, 2001),
                ("synth", "d", "SYNTH", 2, 3000)):
            client._write_dag_manifest(name, [TitanJob(job_id, __file__)], timestamp,
                                       agent_metadata={"agent_run_id": "run-1",
                                                       "agent_run_name": "Research run",
                                                       "agent_stage": stage,
                                                       "agent_stage_order": order})
        dags = [dag("plan", 1000), dag("search-a", 2000), dag("search-b", 2001),
                dag("synth", 3000, "RUNNING")]
        run = AgentRunCatalog(self.manifest).snapshot(dags)[0]
        self.assertEqual("run-1", run["id"])
        self.assertEqual([0, 1, 1, 2], [stage["order"] for stage in run["stages"]])
        self.assertEqual("RUNNING", run["status"])
        self.assertEqual(3, run["completed_stages"])

    def test_existing_agentbench_runs_are_visible_without_broad_name_guessing(self):
        client = TitanClient()
        client._write_dag_manifest("AGENTBENCH_RESEARCH_PLAN_1", [TitanJob("ab12345_rp", __file__)], 1000)
        client._write_dag_manifest("AGENTBENCH_RESEARCH_SYNTH_1", [TitanJob("ab12345_rs", __file__)], 2000)
        client._write_dag_manifest("NORMAL_PIPELINE_1", [TitanJob("ordinary", __file__)], 3000)
        runs = AgentRunCatalog(self.manifest).snapshot([
            dag("AGENTBENCH_RESEARCH_PLAN_1", 1000),
            dag("AGENTBENCH_RESEARCH_SYNTH_1", 2000),
            dag("NORMAL_PIPELINE_1", 3000)])
        self.assertEqual(1, len(runs))
        self.assertEqual(["PLAN", "SYNTH"], [stage["label"] for stage in runs[0]["stages"]])

    def test_existing_concurrent_client_stages_share_one_parallel_column(self):
        client = TitanClient()
        dags = []
        for index in range(4):
            name = f"AGENTBENCH_CONCURRENT-CLIENTS_CLIENT{index}_1"
            client._write_dag_manifest(name, [TitanJob(f"ab98765_mc{index}", __file__)],
                                       1000 + index)
            dags.append(dag(name, 1000 + index))
        run = AgentRunCatalog(self.manifest).snapshot(dags)[0]
        self.assertEqual([0, 0, 0, 0], [stage["order"] for stage in run["stages"]])

    def test_agent_page_links_each_stage_to_existing_dag_view(self):
        client = TitanClient()
        client._write_dag_manifest("plan", [TitanJob("a", __file__)], 1000,
                                   agent_metadata={"agent_run_id": "run-1", "agent_run_name": "Run one",
                                                   "agent_stage": "PLAN", "agent_stage_order": 0})
        stats = {"dag_jobs": {"DAG-a": {"status": "COMPLETED", "completed_at": 2000}},
                 "workers": []}
        app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates"),
                    static_folder=str(Path(__file__).resolve().parents[1] / "static"))
        app.add_url_rule("/logs/<job_id>", "view_logs", lambda job_id: job_id)
        app.register_blueprint(create_blueprint(
            lambda *_: json.dumps(stats), 9, 22, self.manifest, self.root))
        with app.test_client() as browser:
            page = browser.get("/agents/run-1")
            api = browser.get("/api/agents/run-1")
        self.assertEqual(200, page.status_code)
        self.assertIn(b"Agent stage graph", page.data)
        self.assertIn(b"/dags/DAG-plan", page.data)
        self.assertEqual(200, api.status_code)
        self.assertEqual("/dags/DAG-plan", api.json["stages"][0]["url"])


if __name__ == "__main__":
    unittest.main()
