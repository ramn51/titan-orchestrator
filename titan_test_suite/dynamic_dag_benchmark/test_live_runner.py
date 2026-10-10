"""Unit tests for live benchmark evidence verification."""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from .live_runner import Case, Spec, configure_dag_persistence, verify_cases
from .live_reliability_runner import dashboard_status, worker_command


class LiveEvidenceVerifierTest(unittest.TestCase):
    def _case_and_dir(self):
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        case = Case("sample", 1, started_ms=1000, ended_ms=1200)
        case.expected = {
            "root": Spec("root"),
            "child": Spec("child", ("root",)),
        }
        case.statuses = {"DAG-root": "COMPLETED", "DAG-child": "COMPLETED"}
        return temporary, root, case

    @staticmethod
    def _span(job_id, parents, started, ended):
        return {"job_id": job_id, "worker": "127.0.0.1:8080",
                "queued_at": started - 10, "eligible_at": started - 5,
                "started_at": started, "ended_at": ended, "attempt": 1,
                "priority": 5, "status": "COMPLETED", "reason": "",
                "parents": parents}

    def test_accepts_complete_ordered_evidence(self):
        temporary, root, case = self._case_and_dir()
        self.addCleanup(temporary.cleanup)
        spans = [self._span("DAG-root", [], 10, 20),
                 self._span("DAG-child", ["DAG-root"], 20, 30)]
        (root / "spans-test.jsonl").write_text(
            "".join(json.dumps(span) + "\n" for span in spans), encoding="utf-8")
        result = verify_cases([case], root)
        self.assertTrue(result["passed"])
        self.assertEqual(2, result["cases"][0]["jobs"])

    def test_rejects_child_start_before_parent_completion(self):
        temporary, root, case = self._case_and_dir()
        self.addCleanup(temporary.cleanup)
        spans = [self._span("DAG-root", [], 10, 25),
                 self._span("DAG-child", ["DAG-root"], 20, 30)]
        (root / "spans-test.jsonl").write_text(
            "".join(json.dumps(span) + "\n" for span in spans), encoding="utf-8")
        result = verify_cases([case], root)
        self.assertFalse(result["passed"])
        self.assertTrue(any("started before" in error
                            for error in result["cases"][0]["errors"]))

    def test_rejects_missing_span(self):
        temporary, root, case = self._case_and_dir()
        self.addCleanup(temporary.cleanup)
        (root / "spans-test.jsonl").write_text(
            json.dumps(self._span("DAG-root", [], 10, 20)) + "\n", encoding="utf-8")
        result = verify_cases([case], root)
        self.assertFalse(result["passed"])
        self.assertTrue(any("expected one COMPLETED span" in error
                            for error in result["cases"][0]["errors"]))

    def test_isolated_persistence_stays_in_output_directory(self):
        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {}, clear=False):
            output = Path(temporary) / "result"
            configured = configure_dag_persistence(output, False, Path("unused.json"))
            self.assertEqual((output / "manifest.json").resolve(),
                             Path(configured["TITAN_DAG_MANIFEST"]))
            self.assertEqual((output / "dag-journal").resolve(),
                             Path(configured["TITAN_DAG_JOURNAL_DIR"]))

    def test_dashboard_visible_persistence_uses_dashboard_journal(self):
        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {}, clear=False):
            manifest = Path(temporary) / "dashboard" / ".titan_dag_manifest.json"
            configured = configure_dag_persistence(Path(temporary) / "result", True, manifest)
            self.assertEqual(manifest.resolve(), Path(configured["TITAN_DAG_MANIFEST"]))
            self.assertEqual(manifest.resolve().with_name(".titan_dag_journal"),
                             Path(configured["TITAN_DAG_JOURNAL_DIR"]))
            self.assertEqual(manifest.resolve().with_name(".titan_dag_run_history.jsonl"),
                             Path(configured["TITAN_DAG_RUN_HISTORY"]))

    def test_exhausted_retry_state_matches_dashboard_failure_label(self):
        self.assertEqual("FAILED", dashboard_status("DEAD"))
        self.assertEqual("CANCELLED", dashboard_status("CANCELLED"))

    def test_recovery_worker_command_uses_main_entrypoint(self):
        command = worker_command(Path("java"), Path("titan.jar"), 8095, "RECOVERY", True)
        self.assertEqual(["java", "-cp", "titan.jar", "titan.Main", "WORKER"], command[:5])
        self.assertEqual(["8095", "127.0.0.1", "9090", "RECOVERY", "true"], command[5:])


if __name__ == "__main__":
    unittest.main()
