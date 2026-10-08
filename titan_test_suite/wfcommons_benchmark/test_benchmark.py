"""Structural checks for the WfFormat adapter and bounded sample selector."""
import unittest

from benchmark import family_of, inspect, order_graph, sample_graph


class BenchmarkGraphTest(unittest.TestCase):
    def setUp(self):
        self.graph = {
            "root_a": (), "root_b": (),
            "merge": ("root_a", "root_b"),
            "finish": ("merge",),
        }

    def test_topological_order_preserves_dependencies(self):
        order, _ = order_graph(self.graph)
        index = {job: position for position, job in enumerate(order)}
        self.assertTrue(all(index[parent] < index[job]
                            for job, parents in self.graph.items() for parent in parents))
        self.assertEqual(3, inspect(self.graph, "test", "montage-fixture.json")["critical_path_tasks"])

    def test_sample_is_parent_closed_and_full_limit_keeps_every_task(self):
        for limit in (3, 4):
            selected = sample_graph(self.graph, limit)
            self.assertLessEqual(len(selected), limit)
            self.assertTrue(all(parent in selected
                                for job in selected for parent in self.graph[job]))
        self.assertEqual(set(self.graph), set(sample_graph(self.graph, 4)))

    def test_source_family_is_derived_from_official_filename(self):
        for family in ("montage", "seismology", "soykb", "1000genome"):
            filename = f"{family}-chameleon-example.json"
            self.assertEqual(family, family_of(filename))
            self.assertIn(f"/{family}/{filename}", inspect(self.graph, "hash", filename)["source"])


if __name__ == "__main__":
    unittest.main()
