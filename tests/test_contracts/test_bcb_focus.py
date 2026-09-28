from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.bcb import focus_parser
from agrobr.contracts.bcb_focus import BCB_FOCUS_V2
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente


@pytest.fixture
def frame(focus_captures):
    page = focus_parser.parse_page(focus_captures["bodies"]["annual_api_ge"], "anual")
    return focus_parser.build_frame(page.records)


def test_empty_contract_preserves_all_numeric_and_date_dtypes():
    empty = BCB_FOCUS_V2.empty_frame()
    assert empty.empty and len(empty.columns) == 12
    assert str(empty["data"].dtype) == "datetime64[ns]"
    assert str(empty["base_calculo"].dtype) == str(empty["numero_respondentes"].dtype) == "Int64"
    assert str(empty["media"].dtype) == "float64"
    assert BCB_FOCUS_V2.validate(empty) == (True, [])


@pytest.mark.parametrize(
    "field,value",
    [
        ("indicador", " "),
        ("indicador", 1),
        ("periodicidade", "trimestral"),
        ("data_referencia", "08/2028"),
        ("data_referencia", "0000"),
        ("data_referencia", 2026),
        ("indicador_detalhe", 1),
        ("media", float("inf")),
        ("media", float("-inf")),
        ("numero_respondentes", -1),
        ("base_calculo", 2**31),
        ("data", pd.NaT),
        ("data", pd.Timestamp("2026-08-28 01:00:00")),
    ],
)
def test_invalid_focus_contract_value_rejected(field, value, frame):
    if field in {"indicador", "periodicidade", "data_referencia", "indicador_detalhe"}:
        frame[field] = frame[field].astype(object)
    frame.loc[0, field] = value
    valid, errors = BCB_FOCUS_V2.validate(frame)
    assert not valid and errors


@pytest.mark.parametrize(
    "field,dtype",
    [
        ("data", "datetime64[us]"),
        ("media", "Float64"),
        ("media", "object"),
        ("numero_respondentes", "int64"),
        ("base_calculo", "float64"),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
def test_wrong_dtype_rejected_even_empty(field, dtype, empty, frame):
    if empty:
        frame = frame.iloc[:0].copy()
    frame[field] = frame[field].astype(dtype)
    valid, errors = BCB_FOCUS_V2.validate(frame)
    assert not valid and errors


def test_empty_detail_text_remains_distinct_from_null_identity(frame):
    frame = frame.iloc[:1].copy()
    frame["indicador_detalhe"] = pd.Series([None], dtype=object)
    other = frame.copy()
    other.loc[0, "indicador_detalhe"] = ""
    assert BCB_FOCUS_V2.validate(pd.concat([frame, other], ignore_index=True)) == (True, [])


def test_monthly_nonnull_detail_violates_entity_contract(focus_captures):
    page = focus_parser.parse_page(focus_captures["bodies"]["monthly_api_ge"], "mensal")
    frame = focus_parser.build_frame(page.records)
    assert BCB_FOCUS_V2.validate(frame) == (True, [])
    frame.loc[0, "indicador_detalhe"] = ""
    valid, errors = BCB_FOCUS_V2.validate(frame)
    assert not valid and errors


def test_increasing_survey_date_rejected_without_reordering(frame):
    frame.loc[1, "data"] = pd.Timestamp("2026-08-29")
    valid, errors = BCB_FOCUS_V2.validate(frame)
    assert not valid and errors


def test_duplicate_columns_are_error_not_attribute_error(frame):
    duplicated = pd.concat([frame, frame[["data"]]], axis=1)
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(duplicated, BCB_FOCUS_V2)
    assert erro.value.violation == "Duplicate column labels: ['data']"
