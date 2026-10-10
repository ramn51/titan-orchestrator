"""Dependency-free reference model for atomic, versioned DAG mutations.

This is deliberately separate from Titan's scheduler implementation.  It is an
oracle for benchmark verification, not a second scheduler used by production.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import hashlib
import json
import threading
import time
from typing import Iterable


TERMINAL = frozenset({"COMPLETED", "FAILED", "CANCELLED"})


class MutationRejected(ValueError):
    """Raised when a graph mutation cannot be applied atomically."""


@dataclass(frozen=True, slots=True)
class NodeSpec:
    node_id: str
    parents: tuple[str, ...] = ()
    kind: str = "task"

    @classmethod
    def create(cls, node_id: str, parents: Iterable[str] = (), kind: str = "task"):
        return cls(node_id=node_id, parents=tuple(dict.fromkeys(parents)), kind=kind)

    def as_dict(self) -> dict:
        return {"id": self.node_id, "parents": list(self.parents), "kind": self.kind}


@dataclass(frozen=True, slots=True)
class Mutation:
    mutation_id: str
    add: tuple[NodeSpec, ...] = ()
    cancel: tuple[str, ...] = ()
    expected_version: int | None = None

    @classmethod
    def create(
        cls,
        mutation_id: str,
        add: Iterable[NodeSpec] = (),
        cancel: Iterable[str] = (),
        expected_version: int | None = None,
    ):
        return cls(mutation_id, tuple(add), tuple(dict.fromkeys(cancel)), expected_version)

    def canonical(self) -> str:
        body = {
            "mutation_id": self.mutation_id,
            "add": [node.as_dict() for node in self.add],
            "cancel": list(self.cancel),
            "expected_version": self.expected_version,
        }
        return json.dumps(body, sort_keys=True, separators=(",", ":"))


@dataclass(slots=True)
class NodeState:
    spec: NodeSpec
    status: str = "PENDING"
    ready_count: int = 0


class GraphRun:
    """Thread-safe reference state machine with atomic mutation application."""

    def __init__(self, run_id: str):
        self.run_id = run_id
        self.version = 0
        self.nodes: dict[str, NodeState] = {}
        self.children: dict[str, set[str]] = defaultdict(set)
        self._ready = deque()
        self._applied: dict[str, tuple[str, int]] = {}
        self.events: list[dict] = []
        self._sequence = 0
        self._lock = threading.RLock()

    def _record(self, event_type: str, **fields) -> None:
        self._sequence += 1
        self.events.append({
            "seq": self._sequence,
            "at_ns": time.perf_counter_ns(),
            "run_id": self.run_id,
            "type": event_type,
            **fields,
        })

    @staticmethod
    def _validate_id(value: str, label: str) -> None:
        if not value or len(value) > 160 or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for ch in value):
            raise MutationRejected(f"invalid {label}: {value!r}")

    @staticmethod
    def _find_cycle(parents: dict[str, tuple[str, ...]]) -> list[str]:
        visiting: set[str] = set()
        visited: set[str] = set()
        trail: list[str] = []

        def visit(node_id: str) -> list[str]:
            if node_id in visiting:
                start = trail.index(node_id)
                return trail[start:] + [node_id]
            if node_id in visited:
                return []
            visiting.add(node_id)
            trail.append(node_id)
            for parent_id in parents[node_id]:
                found = visit(parent_id)
                if found:
                    return found
            trail.pop()
            visiting.remove(node_id)
            visited.add(node_id)
            return []

        for candidate in parents:
            found = visit(candidate)
            if found:
                return found
        return []

    def apply(self, mutation: Mutation) -> dict:
        """Validate a complete mutation, then commit it under one lock."""
        self._validate_id(mutation.mutation_id, "mutation ID")
        fingerprint = hashlib.sha256(mutation.canonical().encode("utf-8")).hexdigest()
        with self._lock:
            prior = self._applied.get(mutation.mutation_id)
            if prior:
                if prior[0] != fingerprint:
                    raise MutationRejected("mutation ID was reused with different content")
                self._record("mutation_duplicate", mutation_id=mutation.mutation_id,
                             version=prior[1], fingerprint=fingerprint)
                return {"applied": False, "duplicate": True, "version": prior[1]}
            if mutation.expected_version is not None and mutation.expected_version != self.version:
                raise MutationRejected(
                    f"stale version: expected {mutation.expected_version}, current {self.version}")

            new_ids = [node.node_id for node in mutation.add]
            if len(new_ids) != len(set(new_ids)):
                raise MutationRejected("mutation contains duplicate node IDs")
            for node in mutation.add:
                self._validate_id(node.node_id, "node ID")
                if node.node_id in self.nodes:
                    raise MutationRejected(f"node already exists: {node.node_id}")
                for parent_id in node.parents:
                    self._validate_id(parent_id, "parent ID")
            available = set(self.nodes) | set(new_ids)
            for node in mutation.add:
                missing = set(node.parents) - available
                if missing:
                    raise MutationRejected(
                        f"node {node.node_id} has unknown parents: {sorted(missing)}")
            unknown_cancel = set(mutation.cancel) - available
            if unknown_cancel:
                raise MutationRejected(f"cannot cancel unknown nodes: {sorted(unknown_cancel)}")

            prospective = {node_id: state.spec.parents for node_id, state in self.nodes.items()}
            prospective.update({node.node_id: node.parents for node in mutation.add})
            cycle = self._find_cycle(prospective)
            if cycle:
                raise MutationRejected("cycle detected: " + " -> ".join(cycle))

            before = self.version
            for spec in mutation.add:
                self.nodes[spec.node_id] = NodeState(spec=spec)
                for parent_id in spec.parents:
                    self.children[parent_id].add(spec.node_id)
            self.version += 1
            self._applied[mutation.mutation_id] = (fingerprint, self.version)
            self._record(
                "mutation_applied",
                mutation_id=mutation.mutation_id,
                fingerprint=fingerprint,
                version_before=before,
                version_after=self.version,
                add=[node.as_dict() for node in mutation.add],
                cancel=list(mutation.cancel),
            )
            cancelled = self._cancel_locked(mutation.cancel)
            for spec in mutation.add:
                if self._parents_completed(spec) and self.nodes[spec.node_id].status == "PENDING":
                    self._mark_ready_locked(spec.node_id)
            return {"applied": True, "duplicate": False, "version": self.version,
                    "added": len(mutation.add), "cancelled": len(cancelled)}

    def _parents_completed(self, spec: NodeSpec) -> bool:
        return all(self.nodes[parent].status == "COMPLETED" for parent in spec.parents)

    def _mark_ready_locked(self, node_id: str) -> None:
        state = self.nodes[node_id]
        if state.status != "PENDING":
            return
        state.status = "READY"
        state.ready_count += 1
        self._ready.append(node_id)
        self._record("node_ready", node_id=node_id, ready_count=state.ready_count)

    def pop_ready(self) -> str | None:
        with self._lock:
            while self._ready:
                node_id = self._ready.popleft()
                state = self.nodes[node_id]
                if state.status != "READY":
                    continue
                state.status = "RUNNING"
                self._record("node_running", node_id=node_id)
                return node_id
            return None

    def complete(self, node_id: str) -> None:
        with self._lock:
            state = self.nodes.get(node_id)
            if state is None:
                raise MutationRejected(f"unknown completion: {node_id}")
            if state.status == "COMPLETED":
                self._record("completion_duplicate", node_id=node_id)
                return
            if state.status != "RUNNING":
                raise MutationRejected(f"cannot complete {node_id} from {state.status}")
            state.status = "COMPLETED"
            self._record("node_completed", node_id=node_id)
            for child_id in sorted(self.children.get(node_id, ())):
                child = self.nodes[child_id]
                if child.status == "PENDING" and self._parents_completed(child.spec):
                    self._mark_ready_locked(child_id)

    def fail(self, node_id: str, reason: str = "injected failure") -> None:
        with self._lock:
            state = self.nodes.get(node_id)
            if state is None or state.status != "RUNNING":
                raise MutationRejected(f"cannot fail {node_id}")
            state.status = "FAILED"
            self._record("node_failed", node_id=node_id, reason=reason)
            self._cancel_locked(self.children.get(node_id, ()))

    def cancel(self, node_ids: Iterable[str]) -> list[str]:
        with self._lock:
            return self._cancel_locked(node_ids)

    def _cancel_locked(self, node_ids: Iterable[str]) -> list[str]:
        pending = deque(node_ids)
        visited: set[str] = set()
        cancelled = []
        while pending:
            node_id = pending.popleft()
            if node_id in visited or node_id not in self.nodes:
                continue
            visited.add(node_id)
            pending.extend(self.children.get(node_id, ()))
            state = self.nodes[node_id]
            if state.status in TERMINAL:
                continue
            state.status = "CANCELLED"
            cancelled.append(node_id)
            self._record("node_cancelled", node_id=node_id)
        return cancelled

    def drain_successfully(self) -> int:
        completed = 0
        while True:
            node_id = self.pop_ready()
            if node_id is None:
                return completed
            self.complete(node_id)
            completed += 1

    def graph_hash(self) -> str:
        with self._lock:
            canonical = [self.nodes[node_id].spec.as_dict() for node_id in sorted(self.nodes)]
        return hashlib.sha256(json.dumps(canonical, sort_keys=True,
                                         separators=(",", ":")).encode("utf-8")).hexdigest()

    def snapshot(self) -> dict:
        with self._lock:
            status_counts: dict[str, int] = defaultdict(int)
            duplicate_ready = []
            for node_id, state in self.nodes.items():
                status_counts[state.status] += 1
                if state.ready_count > 1:
                    duplicate_ready.append(node_id)
            edges = sum(len(state.spec.parents) for state in self.nodes.values())
            return {
                "run_id": self.run_id,
                "version": self.version,
                "nodes": len(self.nodes),
                "edges": edges,
                "status": dict(sorted(status_counts.items())),
                "duplicate_ready": sorted(duplicate_ready),
                "graph_hash": self.graph_hash(),
                "event_count": len(self.events),
            }
