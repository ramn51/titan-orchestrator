"""Concurrency and invalidation contracts for the dashboard manifest cache."""

import json
import os
import sys
import unittest
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dag_viewer.catalog import DagCatalog
from dag_viewer.submission import record_run, write_manifest
from titan_sdk.titan_sdk import TitanClient, TitanJob
from titan_sdk.dag_journal import (current_runs, run_summaries,
                                   runs_for as journal_runs_for, write_run)


def _sdk_record(arguments):
    index, manifest, script = arguments
    os.environ["TITAN_DAG_MANIFEST"] = manifest
    job = TitanJob(f"sdk-{index}", script)
    TitanClient()._write_dag_manifest(f"sdk-pipeline-{index}", [job], 2000 + index,
                                     job.to_string(), {})


class ManifestCacheTests(unittest.TestCase):
    def test_concurrent_cold_requests_parse_one_manifest_revision(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                f"DAG-{index}": {"dag": "wide", "deps": [], "run_ts": 1000}
                for index in range(2000)
            }), encoding="utf-8")
            catalog = DagCatalog(manifest, root)
            with ThreadPoolExecutor(max_workers=8) as executor:
                results = list(executor.map(lambda _: catalog.snapshot({"workers": []}), range(8)))
            self.assertTrue(all(len(result[0]["jobs"]) == 2000 for result in results))

    def test_atomic_same_size_replacement_invalidates_cache(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            first = {"DAG-A": {"dag": "first", "deps": [], "run_ts": 1000}}
            second = {"DAG-B": {"dag": "other", "deps": [], "run_ts": 1000}}
            first_text = json.dumps(first)
            second_text = json.dumps(second)
            self.assertEqual(len(first_text), len(second_text))
            manifest.write_text(first_text, encoding="utf-8")
            original_mtime = manifest.stat().st_mtime_ns
            catalog = DagCatalog(manifest, root)
            self.assertEqual("DAG-first", catalog.snapshot({"workers": []})[0]["id"])

            replacement = root / "replacement.tmp"
            replacement.write_text(second_text, encoding="utf-8")
            os.utime(replacement, ns=(original_mtime, original_mtime))
            os.replace(replacement, manifest)

            refreshed = catalog.snapshot({"workers": []})
            self.assertEqual("DAG-other", refreshed[0]["id"])
            self.assertEqual("DAG-B", refreshed[0]["jobs"][0]["id"])

    def test_live_state_change_invalidates_snapshot_without_manifest_reread(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "DAG-A": {"dag": "demo", "deps": [], "run_ts": 1000}
            }), encoding="utf-8")
            catalog = DagCatalog(manifest, root)
            waiting = catalog.snapshot({"dag_jobs": {"DAG-A": {"status": "WAITING"}}})
            running = catalog.snapshot({"dag_jobs": {"DAG-A": {"status": "RUNNING"}}})
            self.assertEqual("WAITING", waiting[0]["status"])
            self.assertEqual("RUNNING", running[0]["status"])

    def test_large_manifest_warm_snapshot_avoids_reparse(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                f"DAG-{index}": {"dag": "large", "deps": [f"DAG-{index - 1}"] if index else [],
                                  "run_ts": 1000}
                for index in range(10_000)
            }), encoding="utf-8")
            catalog = DagCatalog(manifest, root)
            first = catalog.snapshot({"workers": []})
            second = catalog.snapshot({"workers": []})
            self.assertIs(first, second)
            self.assertEqual(10_000, first[0]["total"])

    def test_repeated_atomic_replacement_never_exposes_partial_json(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"DAG-0": {"dag": "run-0", "deps": [], "run_ts": 0}}),
                                encoding="utf-8")
            catalog = DagCatalog(manifest, root)
            failures = []

            def writer():
                for revision in range(1, 51):
                    write_manifest(manifest, {
                        f"DAG-{revision}": {"dag": f"run-{revision}", "deps": [], "run_ts": revision}
                    })

            def reader():
                try:
                    for _ in range(100):
                        result = catalog.snapshot({"workers": []})
                        if not result or result[0]["total"] != 1:
                            failures.append(result)
                except Exception as error:  # failure detail belongs in the assertion
                    failures.append(error)

            with ThreadPoolExecutor(max_workers=5) as executor:
                tasks = [executor.submit(reader) for _ in range(4)]
                tasks.append(executor.submit(writer))
                for task in tasks:
                    task.result()
            final = catalog.snapshot({"workers": []})
            self.assertFalse(failures)
            self.assertEqual("DAG-run-50", final[0]["id"])

    def test_concurrent_run_transactions_do_not_lose_submissions(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            script = root / "task.py"
            script.write_text("print('ok')\n", encoding="utf-8")

            def record(index):
                job = {"id": f"job-{index}", "depends_on": [], "filename": script.name,
                       "path": script, "args": "", "requirement": "GENERAL",
                       "job_type": "run", "port": 0, "priority": 1, "delay": 0}
                record_run(manifest, f"pipeline-{index}", [job], f"payload-{index}", 1000 + index)

            with ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(record, range(24)))

            saved = current_runs(manifest)
            for index in range(24):
                run = saved[f"pipeline-{index}"]
                self.assertIn(f"DAG-job-{index}", run["jobs"])
                self.assertEqual(f"payload-{index}", run["payload"])
                self.assertEqual(1, len(journal_runs_for(manifest, f"pipeline-{index}")))

    def test_sdk_processes_do_not_lose_manifest_updates(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            script = root / "task.py"
            script.write_text("print('ok')\n", encoding="utf-8")
            arguments = [(index, str(manifest), str(script)) for index in range(8)]
            with ProcessPoolExecutor(max_workers=4) as executor:
                list(executor.map(_sdk_record, arguments))
            saved = current_runs(manifest)
            for index in range(8):
                self.assertIn(f"DAG-sdk-{index}", saved[f"sdk-pipeline-{index}"]["jobs"])
                self.assertEqual(1, len(journal_runs_for(manifest, f"sdk-pipeline-{index}")))

    def test_run_summary_cache_invalidates_after_append(self):
        with TemporaryDirectory() as directory:
            manifest = Path(directory) / "manifest.json"
            write_run(manifest, "demo", 1000,
                      {"DAG-A": {"dag": "demo", "deps": [], "run_ts": 1000}}, "payload")
            first = run_summaries(manifest)
            second = run_summaries(manifest)
            self.assertEqual(first, second)
            write_run(manifest, "demo", 2000,
                      {"DAG-A": {"dag": "demo", "deps": [], "run_ts": 2000}}, "payload")
            refreshed = run_summaries(manifest)
            self.assertEqual([1000, 2000], [record["run_ts"] for record in refreshed])

    def test_frontend_keeps_slow_polling_after_terminal_or_unknown_state(self):
        script = (Path(__file__).resolve().parents[1] / "static" / "dag_viewer.js").read_text(encoding="utf-8")
        self.assertNotIn("'UNKNOWN'].includes(status)", script)
        self.assertIn("terminal(status) ? 30000", script)
        self.assertIn("status === 'UNKNOWN' ? 10000", script)
        self.assertIn("setTimeout(refresh, refreshDelay(selectedStatus))", script)


if __name__ == "__main__":
    unittest.main()
