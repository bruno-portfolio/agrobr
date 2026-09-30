from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from agrobr.contracts import Column, ColumnType


@pytest.mark.parametrize(
    "kind, values",
    [
        (ColumnType.FLOAT, [True]),
        (ColumnType.FLOAT, [1 + 2j]),
        (ColumnType.FLOAT, [float("inf")]),
        (ColumnType.FLOAT, [float("-inf")]),
        (ColumnType.INTEGER, [1 + 0j]),
        (ColumnType.INTEGER, [float("inf")]),
        (ColumnType.DATE, [20250101]),
        (ColumnType.DATETIME, [True]),
        (ColumnType.DATE, pd.Series([20250101], dtype=object)),
    ],
)
def test_invalid_values_report_errors_without_crashing(kind, values):
    column = Column("value", kind, nullable=True)
    assert column.validate(pd.Series(values))


@pytest.mark.parametrize(
    "values,valid",
    [
        ([date(2025, 1, 1)], False),
        (["2025-01-01"], False),
        (pd.Series([], dtype=object), False),
        (pd.Series([None], dtype=object), False),
        (pd.to_datetime(["2025-01-01"]), True),
        (pd.to_datetime(["2025-01-01"], utc=True), True),
        (pd.Series([], dtype="datetime64[ns]"), True),
        (pd.Series([pd.NaT], dtype="datetime64[ns]"), True),
    ],
)
@pytest.mark.parametrize("kind", [ColumnType.DATE, ColumnType.DATETIME])
def test_data_exige_dtype_datetime64(kind, values, valid):
    errors = Column("data", kind, nullable=True).validate(pd.Series(values))
    assert (errors == []) is valid
    if not valid:
        assert "datetime64" in errors[0]
