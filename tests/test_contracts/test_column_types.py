from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts


@pytest.mark.parametrize(
    "kind,values,dtype,valid",
    [
        ("int", [1, None], "Int64", True),
        ("int", [1.0, None], "float64", True),
        ("int", [1.5, None], "float64", False),
        ("int", ["1", None], "object", False),
        ("int", [True, False], "bool", False),
        ("str", ["a", None], "object", True),
        ("str", ["a", None], "string", True),
        ("str", [None, None], "string", True),
        ("str", [1, None], "object", False),
        ("str", [1, 2], "int64", False),
        ("str", ["a", 1], "object", False),
        ("bool", [True, None], "boolean", True),
        ("bool", [True, None], "object", True),
        ("bool", [1, 0], "object", False),
        ("bool", ["true", "false"], "object", False),
        ("bool", [1, 0], "int64", False),
    ],
)
def test_column_types_validate_values(kind, values, dtype, valid):
    column = contracts.Column("value", contracts.ColumnType(kind), nullable=True)
    assert (column.validate(pd.Series(values, dtype=dtype)) == []) is valid


def test_empty_frame_nullable_dtypes():
    contract = contracts.Contract(
        "test", "1.0", [contracts.Column(kind.value, kind) for kind in contracts.ColumnType]
    )
    assert contract.empty_frame().dtypes.astype(str).to_dict() == {
        "int": "Int64",
        "float": "Float64",
        "Decimal": "Float64",
        "str": "object",
        "date": "datetime64[ns]",
        "datetime": "datetime64[ns]",
        "bool": "boolean",
    }
