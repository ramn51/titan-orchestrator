# 🤝 Contributing & Testing

**Found a bug?** Please log an issue on GitHub! Since this is a custom-built distributed system, edge cases are expected.

## Testing Strategy

> **⚠️ Note to Contributors:** Titan is a research prototype built to explore the "Hard Parts" of Distributed Systems. As such, the test coverage focuses on integration rather than unit purity.

* **Java Tests (`src/test/java`):** These are essentially **Integration Tests** designed during the initial Proof-of-Concept phase to validate the core loop. Some of these may be outdated or flaky due to the rapid pace of architectural changes.
* **Python Validation:** The primary method for validating system stability currently lies in the `titan_test_suite` folder. Scripts like `complex_dag_test.py` and the YAML examples perform end-to-end black-box testing of the cluster.
* **Future Plan:** A comprehensive Engine Test Suite and proper Unit Tests are planned for the near future to improve stability.

## License & Attribution

Titan Orchestrator is licensed under the Apache License 2.0. See the `LICENSE` file in the root repository for full details.

**Created and Maintained by Ram Narayanan A S**
© 2026 Titan Orchestrator. Open for contributions.

*Engineered from first principles to deconstruct the fundamental primitives of distributed orchestration.*