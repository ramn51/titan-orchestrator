from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from titan_test_suite.external_agent_benchmark.runner import build_jobs, encoded, load_case
from titan_test_suite.external_agent_benchmark.stage import main as run_stage
from titan_test_suite.external_agent_benchmark.bfcl_runner import (
    build_jobs as build_bfcl_jobs, load_case as load_bfcl_case,
)
from titan_test_suite.external_agent_benchmark.model_planner import GeminiFunctionPlanner
from titan_test_suite.external_agent_benchmark.bfcl_stage import main as run_bfcl_stage
from titan_test_suite.external_agent_benchmark.llm_dynamic_runner import (
    function_calls_to_frontier, validate_frontier,
)
from titan_test_suite.external_agent_benchmark.bfcl_multiturn_runner import (
    GeminiTurnPlanner, canonical as canonical_multiturn,
)
from titan_test_suite.external_agent_benchmark.bfcl_multiturn_stage import execute_tool


class ParallelQaAdapterTest(unittest.TestCase):
    def test_official_case_builds_parallel_tool_graph(self):
        source, case = load_case("1")
        jobs = build_jobs("unit", case)
        self.assertEqual("LLMCompiler ParallelQA", source["benchmark"])
        self.assertEqual(7, len(jobs))
        by_id = {job.id: job for job in jobs}
        self.assertEqual(["pqa_unit_plan"], by_id["pqa_unit_lookup0"].parents)
        self.assertEqual(["pqa_unit_plan"], by_id["pqa_unit_lookup1"].parents)
        self.assertEqual({"pqa_unit_transform0", "pqa_unit_transform1"},
                         set(by_id["pqa_unit_compare"].parents))
        self.assertEqual(["pqa_unit_compare"], by_id["pqa_unit_answer"].parents)

    def test_pinned_task_has_stable_identity(self):
        _, case = load_case("1")
        normalized = json.dumps(case, sort_keys=True, separators=(",", ":")).encode()
        self.assertEqual(64, len(hashlib.sha256(normalized).hexdigest()))
        self.assertEqual("Mariana Trench", case["answer"])

    def test_branch_scale_cases_build_expected_width(self):
        for case_id, branch in (("1", 2), ("22", 3), ("53", 4), ("84", 5)):
            with self.subTest(case=case_id):
                _, case = load_case(case_id)
                jobs = build_jobs("unit", case)
                self.assertEqual(branch, case["branch"])
                self.assertEqual(2 * branch + 3, len(jobs))

    def test_stage_pipeline_produces_verified_answer_artifact(self):
        _, case = load_case("1")
        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                self.assertEqual(0, run_stage(["unit", "plan", "plan", encoded(case)]))
                for index, entity in enumerate(case["entities"]):
                    self.assertEqual(0, run_stage(
                        ["unit", "lookup", f"lookup{index}", encoded(entity)]))
                    self.assertEqual(0, run_stage([
                        "unit", "transform", f"transform{index}",
                        encoded({"source": f"lookup{index}", "factor": entity["factor"]}),
                    ]))
                self.assertEqual(0, run_stage([
                    "unit", "compare", "compare",
                    encoded({"sources": ["transform0", "transform1"]}),
                ]))
                self.assertEqual(0, run_stage([
                    "unit", "answer", "answer",
                    encoded({"source": "compare", "question": case["question"],
                             "expected": case["answer"]}),
                ]))
                artifact = json.loads(Path(
                    "artifacts/DAG-answer/parallelqa-result.json").read_text(encoding="utf-8"))
                self.assertTrue(artifact["correct"])
                self.assertEqual("Mariana Trench", artifact["answer"])
            finally:
                os.chdir(previous)


class BfclAdapterTest(unittest.TestCase):
    def test_parallel_case_builds_independent_function_calls_and_join(self):
        source, case = load_bfcl_case("parallel_0")
        jobs = build_bfcl_jobs("unit", case)
        self.assertEqual("Berkeley Function Calling Leaderboard V4", source["benchmark"])
        self.assertEqual(4, len(jobs))
        by_id = {job.id: job for job in jobs}
        self.assertEqual(["bfcl_unit_plan"], by_id["bfcl_unit_tool0"].parents)
        self.assertEqual(["bfcl_unit_plan"], by_id["bfcl_unit_tool1"].parents)
        self.assertEqual({"bfcl_unit_tool0", "bfcl_unit_tool1"},
                         set(by_id["bfcl_unit_verify"].parents))

    def test_model_call_validation_accepts_parallel_calls(self):
        _, case = load_bfcl_case("parallel_0")
        planned = GeminiFunctionPlanner.validate_calls(
            {"calls": case["ground_truth"]}, case["functions"])
        self.assertEqual(case["ground_truth"], planned)

    def test_model_call_validation_rejects_unknown_function(self):
        _, case = load_bfcl_case("parallel_0")
        with self.assertRaisesRegex(ValueError, "unknown function"):
            GeminiFunctionPlanner.validate_calls(
                {"calls": [{"name": "shell.exec", "arguments": {}}]}, case["functions"])

    def test_model_call_validation_rejects_missing_arguments(self):
        _, case = load_bfcl_case("parallel_0")
        with self.assertRaisesRegex(ValueError, "missing arguments"):
            GeminiFunctionPlanner.validate_calls(
                {"calls": [{"name": "spotify.play", "arguments": {}}]}, case["functions"])

    def test_all_pilot_cases_have_parallel_calls_and_join(self):
        for case_id in ("parallel_0", "parallel_1", "parallel_2"):
            with self.subTest(case=case_id):
                _, case = load_bfcl_case(case_id)
                jobs = build_bfcl_jobs(case_id, case)
                self.assertEqual(4, len(jobs))
                self.assertEqual(2, len(case["ground_truth"]))
                self.assertEqual(2, len(jobs[-1].parents))

    def test_all_pilot_mock_tools_execute_and_verify_ground_truth(self):
        store = {}

        def fake_request(opcode, payload):
            if opcode == 0x60:
                key, value = payload.split("|", 1)
                store[key] = value
                return "OK"
            if opcode == 0x61:
                return store.get(payload, "")
            raise AssertionError(f"unexpected opcode {opcode}")

        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                with patch(
                    "titan_test_suite.external_agent_benchmark.bfcl_stage.request",
                    side_effect=fake_request,
                ):
                    for case_id in ("parallel_0", "parallel_1", "parallel_2"):
                        _, case = load_bfcl_case(case_id)
                        run_id = case_id
                        for index, call in enumerate(case["ground_truth"]):
                            self.assertEqual(0, run_bfcl_stage([
                                run_id, "tool", f"tool{index}", encoded({"call": call}),
                            ]))
                        self.assertEqual(0, run_bfcl_stage([
                            run_id, "verify", "verify",
                            encoded({"case_id": case_id, "sources": ["tool0", "tool1"],
                                     "expected": case["ground_truth"]}),
                        ]))
            finally:
                os.chdir(previous)


class DynamicLlmPlannerTest(unittest.TestCase):
    def test_accepts_parallel_lookup_frontier(self):
        plan = validate_frontier({"action": "SUBMIT", "reason": "need facts", "jobs": [
            {"id": "lookup_a", "tool": "lookup", "arguments": {"entity": "A"}, "parents": []},
            {"id": "lookup_b", "tool": "lookup", "arguments": {"entity": "B"}, "parents": []},
        ]}, set(), {"A", "B"})
        self.assertEqual(2, len(plan["jobs"]))

    def test_accepts_observation_driven_child_frontier(self):
        plan = validate_frontier({"action": "SUBMIT", "reason": "transform facts", "jobs": [
            {"id": "scale_a", "tool": "scale",
             "arguments": {"source": "lookup_a", "factor": 0.8},
             "parents": ["lookup_a"]},
        ]}, {"lookup_a"}, {"A"})
        self.assertEqual(["lookup_a"], plan["jobs"][0]["parents"])

    def test_rejects_unobserved_same_frontier_dependency(self):
        with self.assertRaisesRegex(ValueError, "completed prior jobs"):
            validate_frontier({"action": "SUBMIT", "reason": "invalid", "jobs": [
                {"id": "lookup_a", "tool": "lookup",
                 "arguments": {"entity": "A"}, "parents": []},
                {"id": "scale_a", "tool": "scale",
                 "arguments": {"source": "lookup_a", "factor": 0.8},
                 "parents": ["lookup_a"]},
            ]}, set(), {"A"})

    def test_rejects_arguments_unused_by_selected_tool(self):
        with self.assertRaisesRegex(ValueError, "unused arguments"):
            validate_frontier({"action": "SUBMIT", "reason": "polluted", "jobs": [
                {"id": "scale_a", "tool": "scale",
                 "arguments": {"source": "lookup_a", "factor": 0.8,
                               "sources": ["lookup_a"]},
                 "parents": ["lookup_a"]},
            ]}, {"lookup_a"}, {"A"})

    def test_native_function_calls_define_parallel_frontier(self):
        plan = function_calls_to_frontier([
            {"name": "lookup", "args": {"entity": "A"}},
            {"name": "lookup", "args": {"entity": "B"}},
        ], set(), {"A", "B"})
        self.assertEqual(["job_0", "job_1"], [job["id"] for job in plan["jobs"]])
        self.assertTrue(all(not job["parents"] for job in plan["jobs"]))

    def test_native_function_call_sources_become_dag_parents(self):
        plan = function_calls_to_frontier([
            {"name": "scale", "args": {"source": "job_0", "factor": 0.8}},
        ], {"job_0"}, {"A"})
        self.assertEqual(["job_0"], plan["jobs"][0]["parents"])

    def test_native_compare_preserves_requested_selection(self):
        plan = function_calls_to_frontier([
            {"name": "compare", "args": {
                "sources": ["job_0", "job_1"], "selection": "max"}},
        ], {"job_0", "job_1"}, {"A"})
        self.assertEqual("max", plan["jobs"][0]["arguments"]["selection"])
        self.assertEqual(["job_0", "job_1"], plan["jobs"][0]["parents"])


class BfclMultiTurnAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).with_name("bfcl_multiturn_case.json")
        cls.case = json.loads(path.read_text(encoding="utf-8"))["case"]
        cls.tools = cls.case["tools"]

    def test_official_case_has_six_turns_and_ten_calls(self):
        self.assertEqual("multi_turn_base_97", self.case["id"])
        self.assertEqual(6, len(self.case["turns"]))
        self.assertEqual(10, sum(map(len, self.case["ground_truth"])))

    def test_model_choice_is_not_rewritten_by_adapter_policy(self):
        calls = [{"name": "estimate_distance", "args": {
            "cityA": "83214", "cityB": "74532"}}]
        self.assertEqual(1, len(GeminiTurnPlanner.validate(
            calls, self.tools, self.case["initial_config"], [])))
        calls = [
            {"name": "get_zipcode_based_on_city", "args": {"city": "Rivermist"}},
            {"name": "get_zipcode_based_on_city", "args": {"city": "Stonebrook"}},
            {"name": "estimate_distance", "args": {"cityA": "83214", "cityB": "74532"}},
        ]
        self.assertEqual(3, len(GeminiTurnPlanner.validate(
            calls, self.tools, self.case["initial_config"], [])))

    def test_parallel_zipcode_calls_are_valid_frontier(self):
        calls = [
            {"name": "get_zipcode_based_on_city", "args": {"city": "Rivermist"}},
            {"name": "get_zipcode_based_on_city", "args": {"city": "Stonebrook"}},
        ]
        planned = GeminiTurnPlanner.validate(
            calls, self.tools, self.case["initial_config"], [])
        self.assertEqual(2, len(planned))

    def test_advance_turn_cannot_hide_other_calls(self):
        calls = [{"name": "advance_turn", "args": {"reason": "done"}},
                 {"name": "fillFuelTank", "args": {"fuelAmount": 45.0}}]
        with self.assertRaisesRegex(ValueError, "alone"):
            GeminiTurnPlanner.validate(calls, self.tools, self.case["initial_config"], [])

    def test_successful_duplicate_call_is_rejected(self):
        calls = [{"name": "pressBrakePedal", "args": {"pedalPosition": 1.0}}]
        observations = [{"call": {
            "name": "pressBrakePedal", "arguments": {"pedalPosition": 1.0}},
            "output": {"brakePedalStatus": "pressed"}}]
        with self.assertRaisesRegex(ValueError, "already completed"):
            GeminiTurnPlanner.validate(
                calls, self.tools, self.case["initial_config"], observations)

    def test_failed_duplicate_call_can_be_repaired(self):
        calls = [{"name": "estimate_distance", "args": {
            "cityA": "Rivermist", "cityB": "Stonebrook"}}]
        observations = [{"call": {
            "name": "estimate_distance", "arguments": {
                "cityA": "Rivermist", "cityB": "Stonebrook"}},
            "output": {"error": "ValueError", "message": "bad pair"}}]
        self.assertEqual(1, len(GeminiTurnPlanner.validate(
            calls, self.tools, self.case["initial_config"], observations)))

    def test_mock_tool_outputs_match_official_arguments(self):
        state = self.case["initial_config"]
        self.assertEqual("83214", execute_tool(
            "get_zipcode_based_on_city", {"city": "Rivermist"}, state)["zipcode"])
        self.assertEqual(750.0, execute_tool(
            "estimate_distance", {"cityA": "83214", "cityB": "74532"}, state)["distance"])
        self.assertEqual(("fillFuelTank", '{"fuelAmount":45}'),
                         canonical_multiturn({"name": "fillFuelTank",
                                              "arguments": {"fuelAmount": 45.0}}))

    def test_invalid_model_arguments_are_tool_errors_not_process_failures(self):
        with self.assertRaisesRegex(ValueError, "city pair"):
            execute_tool("estimate_distance", {
                "cityA": "Rivermist", "cityB": "Stonebrook"}, self.case["initial_config"])


if __name__ == "__main__":
    unittest.main()
