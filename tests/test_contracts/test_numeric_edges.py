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
    "values", [[date(2025, 1, 1)], ["2025-01-01"], pd.to_datetime(["2025-01-01"])]
)
def test_date_representations_remain_valid(values):
    assert Column("data", ColumnType.DATE).validate(pd.Series(values)) == []
