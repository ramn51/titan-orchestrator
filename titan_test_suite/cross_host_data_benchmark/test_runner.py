from __future__ import annotations

import unittest
from pathlib import Path
import tempfile
from unittest.mock import patch

from titan_test_suite.cross_host_data_benchmark.runner import build_jobs, materialize_dashboard_artifacts


class CrossHostGraphTest(unittest.TestCase):
    def test_producers_and_consumers_are_forced_to_different_capabilities(self):
        jobs = build_jobs("unit")
        self.assertEqual(6, len(jobs))
        by_id = {job.id: job for job in jobs}
        for lane in ("kv", "asset", "local"):
            producer = by_id[f"xhost_unit_{lane}_producer"]
            consumer = by_id[f"xhost_unit_{lane}_consumer"]
            self.assertEqual("HOST_A", producer.requirement)
            self.assertEqual("GENERAL", consumer.requirement)
            self.assertEqual([producer.id], consumer.parents)

    def test_verified_files_are_materialized_in_dashboard_job_namespaces(self):
        jobs = build_jobs("unit")
        evidence = {job.id: {"sha256": "abc", "operation": job.id} for job in jobs}
        with tempfile.TemporaryDirectory() as directory:
            with patch("titan_test_suite.cross_host_data_benchmark.runner.REPO", Path(directory)):
                names = materialize_dashboard_artifacts("unit", jobs, evidence)
            artifact_root = Path(directory) / "titan_workspace" / "shared" / "artifacts"
            self.assertEqual(11, len(names))
            self.assertTrue((artifact_root / "DAG-xhost_unit_asset_consumer"
                             / "received-payload.bin").is_file())
            self.assertTrue((artifact_root / "DAG-xhost_unit_local_consumer"
                             / "transfer-evidence.json").is_file())


if __name__ == "__main__":
    unittest.main()
