"""Deterministic runtime DAG shapes used by the local benchmark runner."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import random

from .model import GraphRun, Mutation, MutationRejected, NodeSpec


SCENARIOS = (
    "late-child",
    "fan-out-in",
    "iterative-agent",
    "conditional-agent",
    "research-agent-loop",
    "code-review-repair",
    "document-agent-fanout",
    "recursive-agent",
    "nested-expansion",
    "concurrent-mutation",
    "cancellation",
)


def _apply(run: GraphRun, mutation_id: str, nodes, cancel=(), expected=True):
    version = run.version if expected else None
    return run.apply(Mutation.create(mutation_id, nodes, cancel, version))


def late_child(run: GraphRun, scale: int, seed: int) -> None:
    _apply(run, "bootstrap", [NodeSpec.create("observe")])
    run.drain_successfully()
    children = [NodeSpec.create(f"late-{i:04d}", ["observe"]) for i in range(scale)]
    _apply(run, "add-after-parent-completed", children)
    run.drain_successfully()


def fan_out_in(run: GraphRun, scale: int, seed: int) -> None:
    _apply(run, "bootstrap", [NodeSpec.create("source")])
    run.drain_successfully()
    workers = [NodeSpec.create(f"worker-{i:04d}", ["source"]) for i in range(scale)]
    _apply(run, "expand-workers", workers)
    join = NodeSpec.create("join", [node.node_id for node in workers])
    _apply(run, "add-join-before-workers-finish", [join])
    run.drain_successfully()


def iterative_agent(run: GraphRun, scale: int, seed: int) -> None:
    _apply(run, "bootstrap", [NodeSpec.create("plan-0000", kind="agent")])
    run.drain_successfully()
    parent = "plan-0000"
    for index in range(scale):
        tool = NodeSpec.create(f"tool-{index:04d}", [parent])
        reflect = NodeSpec.create(f"reflect-{index:04d}", [tool.node_id], kind="agent")
        _apply(run, f"iteration-{index:04d}", [tool, reflect])
        run.drain_successfully()
        parent = reflect.node_id


def conditional_agent(run: GraphRun, scale: int, seed: int) -> None:
    rng = random.Random(seed)
    _apply(run, "bootstrap", [NodeSpec.create("classifier", kind="agent")])
    run.drain_successfully()
    chosen = "quality" if rng.randrange(2) else "speed"
    branch = [NodeSpec.create(f"{chosen}-{i:04d}", ["classifier"]) for i in range(scale)]
    _apply(run, f"choose-{chosen}", branch)
    _apply(run, "add-report", [NodeSpec.create(
        "report", [node.node_id for node in branch], kind="agent")])
    run.drain_successfully()


def research_agent_loop(run: GraphRun, scale: int, seed: int) -> None:
    """Planner -> parallel researchers -> evaluator -> deepen/synthesize."""
    _apply(run, "research-plan", [NodeSpec.create("research-planner", kind="agent")])
    run.drain_successfully()
    prior = "research-planner"
    base_width = max(2, min(16, scale // 8))
    for iteration in range(1, 4):
        width = max(1, base_width // iteration)
        researchers = [NodeSpec.create(
            f"research-{iteration}-{index:03d}", [prior], kind="agent")
            for index in range(width)
        ]
        _apply(run, f"research-expand-{iteration}", researchers)
        run.drain_successfully()
        evaluator = NodeSpec.create(
            f"research-evaluator-{iteration}",
            [node.node_id for node in researchers], kind="agent")
        _apply(run, f"research-evaluate-{iteration}", [evaluator])
        run.drain_successfully()
        prior = evaluator.node_id
    _apply(run, "research-synthesize", [
        NodeSpec.create("research-report", [prior], kind="agent")])
    run.drain_successfully()


def code_review_repair(run: GraphRun, scale: int, seed: int) -> None:
    """Planner -> generators -> review -> only flagged fixers -> integrate."""
    _apply(run, "code-plan", [NodeSpec.create("code-planner", kind="agent")])
    run.drain_successfully()
    width = max(3, min(24, scale // 4))
    generators = [NodeSpec.create(f"generator-{index:03d}", ["code-planner"], kind="agent")
                  for index in range(width)]
    _apply(run, "code-generate", generators)
    run.drain_successfully()
    reviewer = NodeSpec.create("review-1", [node.node_id for node in generators], kind="agent")
    _apply(run, "code-review-1", [reviewer])
    run.drain_successfully()

    flagged = [index for index in range(width) if (index + seed) % 3 == 0]
    fixers = [NodeSpec.create(f"fixer-{index:03d}",
                              ["review-1", f"generator-{index:03d}"], kind="agent")
              for index in flagged]
    _apply(run, "targeted-fix", fixers)
    run.drain_successfully()
    review_parents = [node.node_id for node in fixers] or ["review-1"]
    _apply(run, "code-review-2", [NodeSpec.create("review-2", review_parents, kind="agent")])
    run.drain_successfully()
    _apply(run, "code-integrate", [NodeSpec.create("integrated-program", ["review-2"], kind="agent")])
    run.drain_successfully()


def document_agent_fanout(run: GraphRun, scale: int, seed: int) -> None:
    """Repository scan chooses a runtime width, then document workers fan in."""
    _apply(run, "document-scan", [NodeSpec.create("scan-source-tree", kind="agent")])
    run.drain_successfully()
    width = max(4, min(128, scale))
    workers = [NodeSpec.create(f"document-file-{index:04d}", ["scan-source-tree"], kind="agent")
               for index in range(width)]
    _apply(run, "document-workers", workers)
    _apply(run, "document-index", [NodeSpec.create(
        "documentation-index", [node.node_id for node in workers], kind="agent")])
    run.drain_successfully()


def recursive_agent(run: GraphRun, scale: int, seed: int) -> None:
    """A worker uses shared state and admits its successor after each attempt."""
    _apply(run, "recursive-start", [NodeSpec.create("attempt-0", kind="agent")])
    run.drain_successfully()
    prior = "attempt-0"
    attempts = max(2, min(8, scale // 16))
    for index in range(1, attempts):
        successor = NodeSpec.create(f"attempt-{index}", [prior], kind="agent")
        _apply(run, f"recursive-respawn-{index}", [successor])
        run.drain_successfully()
        prior = successor.node_id
    _apply(run, "recursive-terminal", [NodeSpec.create("escalate-or-finish", [prior], kind="agent")])
    run.drain_successfully()


def nested_expansion(run: GraphRun, scale: int, seed: int) -> None:
    _apply(run, "bootstrap", [NodeSpec.create("supervisor", kind="agent")])
    run.drain_successfully()
    leaves = []
    width = max(2, min(scale, 32))
    depth = max(2, min(8, scale // 2 + 1))
    for group in range(width):
        nodes = []
        parent = "supervisor"
        for level in range(depth):
            node = NodeSpec.create(f"g{group:03d}-step{level:02d}", [parent],
                                   kind="agent" if level == 0 else "task")
            nodes.append(node)
            parent = node.node_id
        leaves.append(parent)
        _apply(run, f"nested-group-{group:03d}", nodes)
    _apply(run, "nested-join", [NodeSpec.create("nested-result", leaves)])
    run.drain_successfully()


def concurrent_mutation(run: GraphRun, scale: int, seed: int) -> None:
    _apply(run, "bootstrap", [NodeSpec.create("root")])
    run.drain_successfully()
    count = max(4, scale)

    def mutate(index: int):
        # No expected version here: independent producers serialize through the
        # run lock.  Versioned producers are covered by the stale-write tests.
        return run.apply(Mutation.create(
            f"producer-{index:04d}",
            [NodeSpec.create(f"producer-node-{index:04d}", ["root"], kind="agent")],
        ))

    with ThreadPoolExecutor(max_workers=min(32, count)) as pool:
        list(pool.map(mutate, range(count)))
    _apply(run, "concurrent-join", [NodeSpec.create(
        "concurrent-result", [f"producer-node-{index:04d}" for index in range(count)])])
    run.drain_successfully()


def cancellation(run: GraphRun, scale: int, seed: int) -> None:
    _apply(run, "bootstrap", [NodeSpec.create("root")])
    run.drain_successfully()
    branches = []
    for index in range(max(2, scale)):
        head = NodeSpec.create(f"branch-{index:04d}", ["root"])
        tail = NodeSpec.create(f"tail-{index:04d}", [head.node_id])
        branches.extend([head, tail])
    _apply(run, "add-branches", branches)
    _apply(run, "cancel-first-branch", [], cancel=["branch-0000"])
    run.drain_successfully()


HANDLERS = {
    "late-child": late_child,
    "fan-out-in": fan_out_in,
    "iterative-agent": iterative_agent,
    "conditional-agent": conditional_agent,
    "research-agent-loop": research_agent_loop,
    "code-review-repair": code_review_repair,
    "document-agent-fanout": document_agent_fanout,
    "recursive-agent": recursive_agent,
    "nested-expansion": nested_expansion,
    "concurrent-mutation": concurrent_mutation,
    "cancellation": cancellation,
}


def run_scenario(name: str, scale: int, seed: int, run_id: str | None = None) -> GraphRun:
    if name not in HANDLERS:
        raise MutationRejected(f"unknown scenario: {name}")
    run = GraphRun(run_id or f"{name}-seed-{seed}")
    HANDLERS[name](run, scale, seed)
    return run
