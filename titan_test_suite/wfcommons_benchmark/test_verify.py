"""Focused tests for persisted WfCommons benchmark verification."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from benchmark import submission_plan
from verify import _summary_bar_chart, read_spans, verify_case


class BenchmarkVerificationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.instance = self.root / "montage-fixture.json"
        payload = {"workflow": {"specification": {"tasks": [
            {"id": "root", "parents": []},
            {"id": "left", "parents": ["root"]},
            {"id": "right", "parents": ["root"]},
            {"id": "join", "parents": ["left", "right"]},
        ]}}}
        self.instance.write_text(json.dumps(payload), encoding="utf-8")
        sha = hashlib.sha256(self.instance.read_bytes()).hexdigest()
        self.result = self.root / "result.json"
        self.result.write_text(json.dumps({
            "instance": {"sha256": sha},
            "sample": {"tasks": 4, "edges": 4},
            "live": {"dag": "WFC_MONTAGE_4_12345", "tasks": 4,
                     "status": "COMPLETED", "completed": 4,
                     "wall_seconds": 1.2, "jobs_per_second": 3.33},
            "submission": {"jobs": [
                {"source_task": "root", "job_id": self.job(0), "parents": []},
                {"source_task": "left", "job_id": self.job(1), "parents": [self.job(0)]},
                {"source_task": "right", "job_id": self.job(2), "parents": [self.job(0)]},
                {"source_task": "join", "job_id": self.job(3),
                 "parents": [self.job(1), self.job(2)]},
            ]},
        }), encoding="utf-8")
        self.spans = [
            self.span(0, [], 10, 20),
            self.span(1, [0], 21, 30),
            self.span(2, [0], 21, 31),
            self.span(3, [1, 2], 32, 40),
        ]

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def job(index):
        return f"DAG-wfc_{index:04d}_12345"

    def span(self, index, parents, started, ended, attempt=1, status="COMPLETED"):
        return {"job_id": self.job(index), "parents": [self.job(p) for p in parents],
                "started_at": started, "ended_at": ended, "attempt": attempt,
                "status": status, "reason": "", "worker": "worker:1"}

    def test_complete_graph_passes(self):
        audit = verify_case(self.instance, self.result, self.spans)
        self.assertTrue(audit.passed, audit.errors)
        self.assertEqual(4, audit.observed_edges)
        self.assertEqual(1, audit.capacity["observed_workers"])
        self.assertEqual(2, audit.capacity["peak_slots_used"])
        self.assertEqual(0.03, audit.capacity["span_window_seconds"])

    def test_fleet_telemetry_is_scoped_to_run(self):
        telemetry = {"metrics": {"samples": [
            {"at": 5, "workers": [{"id": "old", "capacity": 20, "load": 10}]},
            {"at": 22, "workers": [{"id": "worker:1", "capacity": 4, "load": 2}]},
            {"at": 50, "workers": []},
        ]}}
        audit = verify_case(self.instance, self.result, self.spans, telemetry=telemetry)
        self.assertTrue(audit.passed, audit.errors)
        self.assertTrue(audit.capacity["telemetry_available"])
        self.assertEqual([{"at": 22, "workers": 1, "capacity": 4, "used": 2}],
                         audit.capacity["fleet_series"])

    def test_parent_mismatch_and_early_child_fail(self):
        spans = list(self.spans)
        spans[-1] = self.span(3, [1], 25, 40)
        audit = verify_case(self.instance, self.result, spans)
        self.assertFalse(audit.passed)
        self.assertEqual(1, len(audit.parent_mismatches))
        self.assertEqual(2, len(audit.ordering_violations))

    def test_retry_is_strict_by_default_and_optional(self):
        retry = dict(self.spans[1], attempt=2)
        strict = verify_case(self.instance, self.result, self.spans + [retry])
        allowed = verify_case(self.instance, self.result, self.spans + [retry], allow_retries=True)
        self.assertFalse(strict.passed)
        self.assertTrue(allowed.passed, allowed.errors)
        self.assertEqual(1, allowed.retry_jobs)

    def test_jsonl_reader_reports_bad_line(self):
        journal = self.root / "spans.jsonl"
        journal.write_text(json.dumps(self.spans[0]) + "\nnot-json\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "spans.jsonl:2"):
            read_spans([journal])

    def test_submission_plan_is_stable(self):
        graph = {"root": (), "left": ("root",), "right": ("root",)}
        first, _ = submission_plan(graph, ["root", "left", "right"], "44")
        second, _ = submission_plan(graph, ["root", "left", "right"], "44")
        self.assertEqual(first, second)

    def test_report_accepts_descriptive_result_filenames(self):
        rendered = _summary_bar_chart(
            [{"result": str(self.root / "montage-result.json"), "value": 1}],
            "Runtime", lambda case: case["value"], lambda value: str(value),
        )
        self.assertIn("montage-result", rendered)
        self.assertNotIn("rotate(", rendered)

    def test_report_uses_short_readable_dag_label(self):
        rendered = _summary_bar_chart(
            [{
                "result": str(self.root / "titan-montage-9805-correctness-2026-09-25" / "montage-9805-result.json"),
                "dag": "WFC_MONTAGE_9805_1790316677",
                "value": 437.461,
            }],
            "Runtime", lambda case: case["value"], lambda value: f"{value:.1f}s",
        )
        self.assertIn("MONTAGE 9,805", rendered)
        self.assertIn("titan-montage-9805-correctness-2026-09-25", rendered)
        self.assertNotIn("rotate(", rendered)


if __name__ == "__main__":
    unittest.main()
