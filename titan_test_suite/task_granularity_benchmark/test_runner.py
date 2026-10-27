from titan_test_suite.task_granularity_benchmark.runner import (
    TaskSpec, bootstrap_median_ci, critical_path_ms, make_graph, max_concurrency,
    median_rows, observed_metg, percentile, render_report, summarize_case,
)


def span(job, start, end, worker="w1"):
    return {"job_id": "DAG-" + job, "status": "COMPLETED", "worker": worker,
            "eligible_at": start, "started_at": start, "ended_at": end}


def test_graph_shapes_and_critical_paths():
    bag = make_graph("bag", 4, 100, "b")
    chain = make_graph("chain", 4, 100, "c")
    fanin = make_graph("fanin", 4, 100, "f")
    assert critical_path_ms(bag) == 100
    assert critical_path_ms(chain) == 400
    assert critical_path_ms(fanin) == 200
    assert len(fanin[-1].parents) == 3


def test_concurrency_treats_end_before_start_at_same_timestamp():
    spans = [span("a", 0, 10), span("b", 10, 20), span("c", 5, 15)]
    assert max_concurrency(spans) == 2


def test_summary_detects_dependency_violation_and_worker_control():
    specs = [TaskSpec("a", (), 100), TaskSpec("b", ("a",), 100)]
    spans = {"DAG-a": [span("a", 0, 100, "w1")],
             "DAG-b": [span("b", 90, 200, "w2")],
             "DAG-sink": [span("sink", 200, 201, "w2")]}
    result = summarize_case("x", "patterns", "chain", specs + [TaskSpec("sink", ("b",), 0)],
                            "sink", spans, 4, .25, "DAG_ACCEPTED", 1, 4)
    assert not result["valid"]
    assert result["dependency_order_violations"] == 1
    assert any("observed 2 workers" in error for error in result["errors"])
    assert result["slot_utilization"] == .25


def test_metg_returns_first_fully_valid_threshold():
    rows = [
        {"requested_duration_ms": 10, "efficiency": .2, "passes": 3, "valid_passes": 3},
        {"requested_duration_ms": 100, "efficiency": .55, "passes": 3, "valid_passes": 3},
        {"requested_duration_ms": 1000, "efficiency": .8, "passes": 3, "valid_passes": 3},
    ]
    result = observed_metg(rows)
    assert result["observed_ms"] == 100
    assert result["previous_tested_ms"] == 10


def test_percentiles_use_consistent_linear_interpolation():
    values = [0, 10, 20, 30]
    assert percentile(values, .50) == 15
    assert round(percentile(values, .95), 6) == 28.5
    assert round(percentile(values, .99), 6) == 29.7
    assert bootstrap_median_ci([1, 2, 3, 4, 5]) == [1.0, 5.0]


def test_aggregate_reports_tail_latency_and_variability():
    cases = []
    for pass_index, throughput in enumerate((8.0, 10.0, 12.0), 1):
        cases.append({"suite": "granularity", "requested_duration_ms": 100,
                      "valid": True, "efficiency": .5, "end_to_end_efficiency": .4,
                      "execution_window_seconds": 3.2, "throughput_tasks_per_second": throughput,
                      "queue_ms_samples": [pass_index, pass_index + 10],
                      "execution_ms_samples": [100, 110]})
    row = median_rows(cases, "granularity", "requested_duration_ms")[0]
    assert row["passes"] == 3
    assert row["queue_p50_ms"] == 7
    assert row["queue_p99_ms"] > row["queue_p95_ms"]
    assert row["execution_p99_ms"] == 110
    assert row["variability"]["throughput"]["min"] == 8
    assert row["variability"]["throughput"]["max"] == 12


def test_report_supports_auxiliary_only_run(tmp_path):
    summary = {"granularity_summary": [], "metg50": observed_metg([]), "results": [],
               "configuration": {"configured_workers": 1, "slots_per_worker": 4,
                                 "passes": 5, "warmup_passes": 1},
               "source": {"branch": "test", "commit": "0" * 40},
               "validity": {}, "generated_at": "now"}
    destination = tmp_path / "report.html"
    render_report(summary, destination)
    assert "not measured" in destination.read_text(encoding="utf-8")
