from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.contracts.bcb_sgs import BCB_SGS_V2
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente


@pytest.fixture
def frame():
    return pd.DataFrame(
        {
            "data": pd.Series(["2024-06-01", "2024-08-01", "2024-09-01"], dtype="datetime64[ns]"),
            "valor": pd.Series([0.21, -0.02, float("nan")], dtype="float64"),
            "codigo": pd.Series([433, 433, 433], dtype="int64"),
            "nome_serie": ["ipca", "ipca", None],
        }
    )


def test_empty_sgs_contract_preserves_ns_float64_int64_and_nullable_name():
    frame = BCB_SGS_V2.empty_frame()
    assert frame.empty
    assert [str(frame[col].dtype) for col in ["data", "valor", "codigo"]] == [
        "datetime64[ns]",
        "float64",
        "int64",
    ]
    assert BCB_SGS_V2.validate(frame) == (True, [])


@pytest.mark.parametrize(
    "column,value",
    [
        ("codigo", 0),
        ("codigo", -1),
        ("valor", float("inf")),
        ("valor", float("-inf")),
        ("nome_serie", ""),
        ("nome_serie", " "),
        ("nome_serie", 433),
        ("nome_serie", float("inf")),
        ("data", pd.NaT),
        ("data", pd.Timestamp("2024-06-01 01:00:00")),
    ],
)
def test_invalid_sgs_value_or_reference_is_rejected(column, value, frame):
    if column == "nome_serie":
        frame[column] = frame[column].astype(object)
    frame.loc[0, column] = value
    valid, errors = BCB_SGS_V2.validate(frame)
    assert not valid and errors


@pytest.mark.parametrize(
    "column,dtype",
    [
        ("data", "datetime64[us]"),
        ("data", "datetime64[ns, UTC]"),
        ("codigo", "Int64"),
        ("codigo", "float64"),
        ("valor", "Float64"),
        ("valor", "object"),
    ],
)
def test_contract_enforces_stable_dtypes(column, dtype, frame):
    if dtype == "datetime64[ns, UTC]":
        frame[column] = frame[column].dt.tz_localize("UTC")
    else:
        frame[column] = frame[column].astype(dtype)
    valid, errors = BCB_SGS_V2.validate(frame)
    assert not valid and errors


def test_contract_requires_order_before_last_selection(frame):
    valid, errors = BCB_SGS_V2.validate(frame.iloc[::-1].reset_index(drop=True))
    assert not valid and errors


def test_duplicate_columns_are_errors_instead_of_attribute_error(frame):
    duplicated = pd.concat([frame, frame[["data"]]], axis=1)
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(duplicated, BCB_SGS_V2)
    assert erro.value.violation == "Duplicate column labels: ['data']"
