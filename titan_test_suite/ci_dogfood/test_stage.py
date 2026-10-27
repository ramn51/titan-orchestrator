from __future__ import annotations

import pytest

from .stage import safe_name


@pytest.mark.parametrize("value", ["preflight", "mcp", "benchmarks", "dashboard", "suite-1"])
def test_safe_name_accepts_bounded_identifiers(value):
    assert safe_name(value) == value


@pytest.mark.parametrize("value", ["", "../suite", "suite name", "UPPER", "a/b"])
def test_safe_name_rejects_unsafe_identifiers(value):
    with pytest.raises(ValueError):
        safe_name(value)
