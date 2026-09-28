from __future__ import annotations

import pytest

from agrobr.incra import _temporal


@pytest.mark.parametrize(
    "value",
    [
        "",
        "2026-09-07",
        "2023-02-29T00:00:00Z",
        "0000-01-01T00:00:00Z",
        "2026-09-07T24:00:01Z",
        "2026-09-07T24:00:00.001Z",
        "2026-09-07T25:00:00Z",
        "2026-09-07T23:60:00Z",
        "2026-09-07T23:59:60Z",
        "2026-09-07T23:59:59.Z",
        "2026-09-07T00:00:00+14:01",
        "2026-09-07T00:00:00-14:01",
        "2026-09-07 00:00:00Z",
        "2026-09-07t00:00:00z",
        " 2026-09-07T00:00:00Z",
        "2026-09-07T00:00:00Z ",
        "2026-09-07T00:00:00Z\t",
        '<!DOCTYPE value SYSTEM "file:///etc/passwd"><value>2026-09-07</value>',
    ],
)
def test_datetime_rejects_invalid_xsd_literal(value):
    with pytest.raises(ValueError):
        _temporal.validate_datetime(value)


@pytest.mark.parametrize("value", [None, True, 20260907, 1.5, b"2026-09-07"])
@pytest.mark.parametrize("validator", [_temporal.validate_date, _temporal.validate_datetime])
def test_temporal_rejects_nonstring_without_coercion(value, validator):
    with pytest.raises(ValueError, match="string"):
        validator(value)
