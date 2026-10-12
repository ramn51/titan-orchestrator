"""Dependency-free unit checks for data-flow hashing and graph models."""
from __future__ import annotations

import hashlib
import unittest

from .runner import expected_outputs, patterns
from .stage import deterministic_bytes, transformed


class DataFlowModelTests(unittest.TestCase):
    def test_deterministic_payload(self):
        self.assertEqual(deterministic_bytes("run", 4096), deterministic_bytes("run", 4096))
        self.assertNotEqual(deterministic_bytes("run", 4096), deterministic_bytes("other", 4096))

    def test_transform_is_order_sensitive(self):
        self.assertNotEqual(transformed("x", [b"a", b"b"]), transformed("x", [b"b", b"a"]))

    def test_all_patterns_are_acyclic_and_resolvable(self):
        for name, graph in patterns().items():
            seen = set()
            for node in graph:
                self.assertTrue(set(node.parents) <= seen, name)
                seen.add(node.label)
            values = expected_outputs("fixed", graph, 1024)
            self.assertEqual(set(values), seen)
            final = graph[-1]
            digest = hashlib.sha256(b"".join(values[p] for p in final.parents)).hexdigest()
            self.assertEqual(len(digest), 64)

    def test_same_content_seed_reproduces_final_artifact(self):
        graph = patterns()["diamond"]
        first = expected_outputs("diamond_v1", graph, 4096)
        repeated = expected_outputs("diamond_v1", graph, 4096)
        self.assertEqual(first[graph[-1].label], repeated[graph[-1].label])


if __name__ == "__main__":
    unittest.main()
