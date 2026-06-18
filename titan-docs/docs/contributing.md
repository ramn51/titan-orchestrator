# Contributing

Titan is a solo-built experimental runtime. Contributions are welcome — bug fixes, docs, tests, new examples, and new capabilities (execution drivers, scheduling policies, integration layers) are all useful. If you've built something on top of Titan that you think belongs in the core, open a PR. Large architectural changes are best discussed first in a [GitHub Discussion](https://github.com/ramn51/titan-orchestrator/discussions) before starting work.

---

## Before You Start

- Check the [Roadmap to v2.0](index.md#roadmap-to-v20) — if you want to work on a major feature (Raft, mTLS, Docker drivers), open a Discussion first so we can coordinate
- Look for issues tagged `good first issue` or `help wanted`
- If you're unsure whether something is worth a PR, ask in [Discussions → Ideas](https://github.com/ramn51/titan-orchestrator/discussions/categories/ideas)

---

## Local Development Setup

```bash
# 1. Clone
git clone https://github.com/ramn51/titan-orchestrator.git
cd titan-orchestrator

# 2. Build the engine JAR (requires Java 17+ and Maven)
mvn clean package -DskipTests

# 3. Install the Python SDK in editable mode
pip install -e .

# 4. Start the full local cluster
./titan-dev.sh up
```

The cluster is live when you see:
```
[OK] SchedulerServer Listening on port 9090
```

Dashboard at `http://localhost:5000`.

---

## Running Tests

### SDK unit tests (no cluster needed)
```bash
pytest titan_sdk/tests/unit/ -v
pytest titan_sdk/tests/mock/ -v
```

### End-to-end cluster tests (cluster must be running)
```bash
# Basic DAG execution
python titan_test_suite/examples/dynamic_dag_custom/complex_etl_pipeline/etl_pipeline.py

# TitanStore KV
python titan_test_suite/examples/titan_store_example/test_suite.py

# YAML pipeline
python titan_sdk/titan_cli.py deploy titan_test_suite/examples/yaml_based_static_tests/dag_structure_test/agent.yaml
```

### Java engine tests
```bash
mvn test
```

!!! warning "Java test coverage is limited"
    The Java tests were written during early PoC and some may be flaky. The primary validation path is the Python end-to-end examples above. Improving Java test coverage is a good first contribution.

---

## Making Changes

### Engine (Java — `src/main/java/titan/`)

- `Scheduler.java` — core dispatch loop, DAG resolution, worker registry
- `RpcWorkerServer.java` — TCP server handling client connections
- `AssetManager.java` — file upload and staging
- `TitanStore` (`perm_files/TitanStore.jar`) — embedded AOF-backed KV store

After any Java change, rebuild and replace the Worker.jar:
```bash
mvn clean package -DskipTests
cp target/titan-orchestrator-1.0-SNAPSHOT.jar perm_files/Worker.jar
```

### SDK (Python — `titan_sdk/`)

- `titan_sdk.py` — `TitanClient`, `TitanJob`, all RPC operations
- `titan_yaml_parser.py` — YAML pipeline parser
- `titan_cli.py` — CLI entry point

### Dashboard (Python — `perm_files/server_dashboard.py`)

Self-contained Flask app. Restart it after changes:
```bash
pkill -f server_dashboard && python3 perm_files/server_dashboard.py &
```

---

## Submitting a PR

1. Fork the repo and create a branch: `git checkout -b fix/your-fix` or `feature/your-feature`
2. Make your change and test it end-to-end with the cluster running
3. Add or update the relevant example in `titan_test_suite/` if it's a behaviour change
4. Open a PR with:
   - What changed and why
   - How you tested it (which example ran, what you observed)
   - Any known limitations or follow-up work

PRs that change the core dispatch loop or RPC protocol should include a short description of the failure modes you considered.

---

## Testing Strategy (Honest Assessment)

Titan's test coverage reflects its research origins:

| Layer | Coverage | Notes |
|---|---|---|
| SDK unit tests | Moderate | `titan_sdk/tests/unit/` — job construction, manifest, HITL injection |
| SDK mock tests | Light | `titan_sdk/tests/mock/` — client method smoke tests |
| Engine Java tests | Light | Written during PoC, may be flaky |
| End-to-end | Good (manual) | `titan_test_suite/` examples cover most codepaths |
| Integration (cluster up) | Light | `titan_sdk/tests/integration/` — TitanStore only |

Improving test coverage — especially Java unit tests for `Scheduler` and `AssetManager` — is one of the most impactful contributions you can make.

---

## Code of Conduct

Be respectful, constructive, and direct. This is a technical project — disagreements about implementation choices are fine; personal criticism is not.

---

## License

Apache 2.0. See `LICENSE` in the repository root.

Built and maintained by Ram Narayanan A S. © 2026.
