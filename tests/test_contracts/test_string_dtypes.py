from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts


@pytest.mark.parametrize("values", [["soja", 1], [b"soja"], [True, None]])
def test_mixed_object_values_are_not_text(values):
    column = contracts.Column("name", contracts.ColumnType.STRING, nullable=True)
    assert column.validate(pd.Series(values, dtype="object"))


def test_empty_object_text_column_is_valid():
    column = contracts.Column("name", contracts.ColumnType.STRING, nullable=True)
    assert column.validate(pd.Series([None, pd.NA], dtype="object")) == []
