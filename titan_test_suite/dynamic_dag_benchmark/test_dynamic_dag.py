"""Focused correctness and concurrency tests for the mutation contract."""

from concurrent.futures import ThreadPoolExecutor
import copy
import unittest

from .model import GraphRun, Mutation, MutationRejected, NodeSpec
from .scenarios import run_scenario
from .verify import verify_events


class DynamicDagContractTest(unittest.TestCase):
    def test_invalid_cycle_is_rejected_atomically(self):
        run = GraphRun("cycle")
        with self.assertRaisesRegex(MutationRejected, "cycle"):
            run.apply(Mutation.create("bad", [
                NodeSpec.create("a", ["b"]), NodeSpec.create("b", ["a"]),
            ], expected_version=0))
        self.assertEqual(0, run.version)
        self.assertEqual({}, run.nodes)

    def test_missing_parent_is_rejected_atomically(self):
        run = GraphRun("missing")
        with self.assertRaisesRegex(MutationRejected, "unknown parents"):
            run.apply(Mutation.create("bad", [NodeSpec.create("child", ["missing"])],
                                       expected_version=0))
        self.assertFalse(run.nodes)

    def test_mutation_retry_is_idempotent_but_conflicting_reuse_fails(self):
        run = GraphRun("retry")
        mutation = Mutation.create("m1", [NodeSpec.create("root")], expected_version=0)
        self.assertTrue(run.apply(mutation)["applied"])
        self.assertTrue(run.apply(mutation)["duplicate"])
        self.assertEqual(1, run.version)
        with self.assertRaisesRegex(MutationRejected, "different content"):
            run.apply(Mutation.create("m1", [NodeSpec.create("other")]))

    def test_stale_version_does_not_partially_apply(self):
        run = GraphRun("stale")
        run.apply(Mutation.create("m1", [NodeSpec.create("root")], expected_version=0))
        with self.assertRaisesRegex(MutationRejected, "stale version"):
            run.apply(Mutation.create("m2", [NodeSpec.create("child", ["root"])],
                                       expected_version=0))
        self.assertNotIn("child", run.nodes)
        self.assertEqual(1, run.version)

    def test_parent_can_complete_before_child_mutation(self):
        run = run_scenario("late-child", scale=50, seed=1)
        snapshot = run.snapshot()
        self.assertEqual({"COMPLETED": 51}, snapshot["status"])
        self.assertEqual([], snapshot["duplicate_ready"])
        self.assertTrue(verify_events(run.events, snapshot)["passed"])

    def test_hundreds_of_concurrent_parent_completions_queue_join_once(self):
        run = GraphRun("fan-in-race")
        parent_count = 500
        parents = [NodeSpec.create(f"p-{index:04d}") for index in range(parent_count)]
        run.apply(Mutation.create("parents", parents, expected_version=0))
        run.apply(Mutation.create("join", [NodeSpec.create(
            "join", [node.node_id for node in parents])], expected_version=1))

        def execute_one(_):
            node_id = run.pop_ready()
            if node_id:
                run.complete(node_id)

        with ThreadPoolExecutor(max_workers=32) as pool:
            list(pool.map(execute_one, range(parent_count)))
        self.assertEqual("join", run.pop_ready())
        run.complete("join")
        self.assertEqual(1, run.nodes["join"].ready_count)
        self.assertTrue(verify_events(run.events, run.snapshot())["passed"])

    def test_concurrent_producers_lose_no_mutations(self):
        run = run_scenario("concurrent-mutation", scale=256, seed=3)
        snapshot = run.snapshot()
        self.assertEqual(258, snapshot["nodes"])
        self.assertEqual({"COMPLETED": 258}, snapshot["status"])
        self.assertTrue(verify_events(run.events, snapshot)["passed"])

    def test_cancellation_cascades_and_stale_queue_entries_do_not_run(self):
        run = run_scenario("cancellation", scale=20, seed=5)
        self.assertEqual("CANCELLED", run.nodes["branch-0000"].status)
        self.assertEqual("CANCELLED", run.nodes["tail-0000"].status)
        self.assertEqual(2, run.snapshot()["status"]["CANCELLED"])
        self.assertTrue(verify_events(run.events, run.snapshot())["passed"])

    def test_scenario_graph_is_deterministic(self):
        first = run_scenario("conditional-agent", scale=16, seed=42)
        second = run_scenario("conditional-agent", scale=16, seed=42)
        self.assertEqual(first.graph_hash(), second.graph_hash())

    def test_conditional_agent_admits_exactly_one_branch(self):
        run = run_scenario("conditional-agent", scale=16, seed=42)
        snapshot = run.snapshot()
        quality = [node_id for node_id in run.nodes if node_id.startswith("quality-")]
        speed = [node_id for node_id in run.nodes if node_id.startswith("speed-")]
        self.assertIn(len(quality), (0, 16))
        self.assertIn(len(speed), (0, 16))
        self.assertEqual(16, len(quality) + len(speed))
        self.assertEqual(16, len(run.nodes["report"].spec.parents))
        self.assertEqual({"COMPLETED": 18}, snapshot["status"])
        self.assertTrue(verify_events(run.events, snapshot)["passed"])

    def test_research_agent_deepens_then_synthesizes(self):
        run = run_scenario("research-agent-loop", scale=64, seed=11)
        snapshot = run.snapshot()
        self.assertEqual(8, snapshot["version"])
        self.assertEqual({"COMPLETED": snapshot["nodes"]}, snapshot["status"])
        self.assertEqual(
            ("research-evaluator-3",),
            run.nodes["research-report"].spec.parents,
        )
        self.assertTrue(verify_events(run.events, snapshot)["passed"])

    def test_code_review_repair_only_spawns_flagged_fixers(self):
        run = run_scenario("code-review-repair", scale=48, seed=2)
        snapshot = run.snapshot()
        fixer_ids = sorted(node_id for node_id in run.nodes if node_id.startswith("fixer-"))
        self.assertEqual(
            ["fixer-001", "fixer-004", "fixer-007", "fixer-010"],
            fixer_ids,
        )
        self.assertEqual(tuple(fixer_ids), run.nodes["review-2"].spec.parents)
        self.assertEqual({"COMPLETED": snapshot["nodes"]}, snapshot["status"])
        self.assertTrue(verify_events(run.events, snapshot)["passed"])

    def test_document_agent_runtime_fanout_joins_every_file(self):
        run = run_scenario("document-agent-fanout", scale=40, seed=4)
        snapshot = run.snapshot()
        self.assertEqual(42, snapshot["nodes"])
        self.assertEqual(40, len(run.nodes["documentation-index"].spec.parents))
        self.assertEqual({"COMPLETED": 42}, snapshot["status"])
        self.assertTrue(verify_events(run.events, snapshot)["passed"])

    def test_recursive_agent_admits_one_successor_per_attempt(self):
        run = run_scenario("recursive-agent", scale=128, seed=9)
        snapshot = run.snapshot()
        self.assertEqual(9, snapshot["nodes"])
        self.assertEqual(9, snapshot["version"])
        self.assertEqual(("attempt-6",), run.nodes["attempt-7"].spec.parents)
        self.assertEqual(("attempt-7",), run.nodes["escalate-or-finish"].spec.parents)
        self.assertEqual([], snapshot["duplicate_ready"])
        self.assertTrue(verify_events(run.events, snapshot)["passed"])

    def test_independent_verifier_detects_early_readiness(self):
        run = run_scenario("fan-out-in", scale=8, seed=8)
        events = copy.deepcopy(run.events)
        worker_ready = next(event for event in events
                            if event["type"] == "node_ready" and event["node_id"].startswith("worker-"))
        worker_ready["seq"] = 1
        result = verify_events(events, run.snapshot())
        self.assertFalse(result["passed"])
        self.assertTrue(any("sequence" in error for error in result["errors"]))


if __name__ == "__main__":
    unittest.main()
