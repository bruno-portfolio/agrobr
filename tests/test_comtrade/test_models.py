from __future__ import annotations

import pytest

from agrobr.comtrade import models, query
from agrobr.exceptions import InvalidParameterError


@pytest.mark.parametrize(
    "name,expected",
    [
        ("BR", 76),
        ("BRA", 76),
        ("brazil", 76),
        (" cn ", 156),
        ("842", 842),
        ("world", 0),
        ("mundo", 0),
    ],
)
def test_country_aliases(name, expected):
    assert models.resolve_pais(name) == expected


def build(**changes):
    return query.build_query(
        **{
            "reporter": 76,
            "partner": 156,
            "hs_codes": ["1201"],
            "flow": "X",
            "period": "2023",
            "freq": "A",
            **changes,
        }
    )


@pytest.mark.parametrize(
    "period,freq,expected",
    [
        (2023, "A", ["2023"]),
        ("2022-2023", "A", ["2022", "2023"]),
        ("2023,2022,2023", "A", ["2022", "2023"]),
        ("202312-202402", "M", ["202312", "202401", "202402"]),
        ("202401,202403", "M", ["202401", "202403"]),
        ("2023", "M", [f"2023{i:02d}" for i in range(1, 13)]),
        (
            "2022-2023",
            "M",
            [f"{year}{month:02d}" for year in (2022, 2023) for month in range(1, 13)],
        ),
    ],
)
def test_query_expands_calendar_before_partitioning(period, freq, expected):
    result = build(period=period, freq=freq)
    assert result.periods == expected
    assert "api_key" not in result.model_dump()


@pytest.mark.parametrize(
    "changes",
    [
        {"period": True},
        {"period": 2023.0},
        {"period": ""},
        {"period": []},
        {"period": "2023-2022"},
        {"period": "202301-202212", "freq": "M"},
        {"period": "202300", "freq": "M"},
        {"period": "202313", "freq": "M"},
        {"period": "2023,202301", "freq": "M"},
        {"period": "202301-2023", "freq": "M"},
        {"period": "202301", "freq": "A"},
        {"freq": "Q"},
        {"flow": "RX"},
        {"reporter": 0},
        {"reporter": True},
        {"partner": -1},
        {"hs_codes": ["123"]},
    ],
)
def test_query_rejects_ambiguous_or_invalid_selection(changes):
    with pytest.raises(InvalidParameterError):
        build(**changes)
