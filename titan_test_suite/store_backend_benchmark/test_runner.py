from titan_test_suite.store_backend_benchmark.runner import comparable_configuration, percentile


def test_percentile_and_configuration_guard():
    assert abs(percentile([0, 10, 20, 30], .95) - 28.5) < 1e-9
    case = {"configuration": {"configured_workers": 1, "slots_per_worker": 4,
                               "passes": 5, "warmup_passes": 1,
                               "durations_ms": [10, 100], "tasks": 32}}
    controlled = comparable_configuration({"titanstore": case, "redis": case})
    assert controlled["passes"] == 5


def test_configuration_guard_rejects_uncontrolled_comparison():
    titanstore = {"configuration": {"configured_workers": 1, "slots_per_worker": 4,
                                     "passes": 5, "warmup_passes": 1,
                                     "durations_ms": [10, 100], "tasks": 32}}
    redis = {"configuration": dict(titanstore["configuration"], slots_per_worker=8)}
    try:
        comparable_configuration({"titanstore": titanstore, "redis": redis})
    except ValueError as error:
        assert "scheduler configurations differ" in str(error)
    else:
        raise AssertionError("uncontrolled scheduler comparison was accepted")

