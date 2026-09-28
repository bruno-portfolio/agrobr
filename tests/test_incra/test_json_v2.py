from __future__ import annotations

import pytest

from agrobr.incra import _json


@pytest.mark.parametrize(
    "body",
    [
        b'{"a":1,"a":2}',
        b'{"a":{"x":1,"x":2}}',
        b'{"a":NaN}',
        b'{"a":Infinity}',
        b'{"a":-Infinity}',
        b'{"a":"\xff"}',
        b'{"a":01}',
        b"[] trailing",
    ],
)
def test_decode_rejects_malformed_json_and_duplicate_members(body):
    with pytest.raises(ValueError):
        _json.decode(body)


@pytest.mark.parametrize(
    "value",
    [
        True,
        "1",
        None,
        _json.Number("1.1"),
        _json.Number(str(2**31)),
        _json.Number(str(-(2**31) - 1)),
        _json.Number("١"),
        _json.Number("+1"),
        _json.Number("01"),
    ],
)
def test_xsd_integer_rejects_coercion_fraction_and_overflow(value):
    with pytest.raises(ValueError):
        _json.integer(value, bits=32)


@pytest.mark.parametrize(
    "value",
    [
        _json.Number("-0"),
        _json.Number("1.0"),
        _json.Number("1e0"),
        _json.Number("-1"),
        True,
        "1",
        1.0,
        _json.Number("١"),
    ],
)
def test_count_requires_nonnegative_integer_token(value):
    with pytest.raises(ValueError):
        _json.count(value)
