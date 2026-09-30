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


def test_empty_frame_tipos_da_saida():
    contract = contracts.Contract(
        "test", "1.0", [contracts.Column(kind.value, kind) for kind in contracts.ColumnType]
    )
    assert contract.empty_frame().dtypes.astype(str).to_dict() == {
        "int": "Int64",
        "float": "float64",
        "Decimal": "float64",
        "str": str(pd.Series(["texto"]).dtype),
        "date": "datetime64[ns]",
        "datetime": "datetime64[ns]",
        "bool": "boolean",
    }


@pytest.mark.parametrize("name,period", [("ano", "2026"), ("safra", "2024/25"), ("mes", "01/2026")])
def test_rotulo_de_periodo_permanece_texto(name, period):
    column = contracts.Column(name, contracts.ColumnType.STRING)
    assert column.validate(pd.Series([period])) == []


@pytest.mark.parametrize("name,values", [("ano", [2026]), ("codigo", [35]), ("contagem", [2])])
def test_inteiro_por_natureza_aceita_int64_nullable(name, values):
    column = contracts.Column(name, contracts.ColumnType.INTEGER, nullable=True)
    assert column.validate(pd.Series([*values, None], dtype="Int64")) == []
