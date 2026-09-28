from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.comtrade import parser
from agrobr.contracts.comtrade import COMERCIO_BILATERAL_V2, TRADE_MIRROR_V2
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente
from tests.test_comtrade.replay import record


@pytest.fixture
def bilateral(captures):
    return parser.parse_trade_data([record(captures)])


@pytest.fixture
def mirror(captures):
    left = parser.parse_trade_data([record(captures)])
    right = parser.parse_trade_data([record(captures, "soy_cn_br_2023_mirror")])
    return parser.parse_mirror(left, right, "BRA", "CHN")


@pytest.mark.parametrize(
    ("contract", "colunas"), [(COMERCIO_BILATERAL_V2, 27), (TRADE_MIRROR_V2, 24)]
)
def test_empty_contract_has_stable_columns(contract, colunas):
    frame = contract.empty_frame()
    assert frame.empty and len(frame.columns) == colunas
    assert contract.validate(frame) == (True, [])
    assert str(frame["mes"].dtype) == str(frame["reporter_code"].dtype) == "Int64"


def test_contract_accepts_published_iso_absence_without_losing_numeric_identity(bilateral):
    bilateral["reporter_iso"] = None
    bilateral["partner_iso"] = None
    assert COMERCIO_BILATERAL_V2.validate(bilateral) == (True, [])


@pytest.mark.parametrize(
    "column,value",
    [
        ("periodo", "202313"),
        ("ano", 2022),
        ("mes", 1),
        ("reporter_code", 0),
        ("partner_code", -1),
        ("hs_code", "123"),
        ("fluxo_code", "RX"),
        ("nivel_hs", 6),
        ("classificacao", ""),
        ("classificacao", "HS"),
        ("peso_liquido_kg", -1.0),
        ("valor_fob_usd", float("inf")),
        ("quantidade", float("-inf")),
    ],
)
def test_bilateral_contract_rejects_invalid_dimensions_or_metrics(column, value, bilateral):
    bilateral.loc[0, column] = value
    valid, errors = COMERCIO_BILATERAL_V2.validate(bilateral)
    assert not valid and errors


@pytest.mark.parametrize(
    "column,dtype",
    [
        ("reporter_code", "int64"),
        ("ano", "float64"),
        ("classificacao_original", "bool"),
        ("peso_liquido_kg", "Float64"),
    ],
)
def test_contract_enforces_nullable_integer_boolean_and_float64_dtypes(column, dtype, bilateral):
    bilateral[column] = bilateral[column].astype(dtype)
    valid, errors = COMERCIO_BILATERAL_V2.validate(bilateral)
    assert not valid and errors


def test_mirror_contract_rejects_different_hs_revisions(mirror):
    mirror["classificacao_partner"] = "H5"
    valid, errors = TRADE_MIRROR_V2.validate(mirror)
    assert not valid and errors


def test_hs_code_rejects_letters_with_coherent_level(bilateral):
    bilateral.loc[0, "hs_code"] = "ABCD"
    bilateral.loc[0, "nivel_hs"] = 4
    valid, errors = COMERCIO_BILATERAL_V2.validate(bilateral)
    assert not valid
    assert any("hs_code" in error for error in errors)


@pytest.mark.parametrize(
    "contract,fixture,column",
    [
        (COMERCIO_BILATERAL_V2, "bilateral", "valor_fob_usd"),
        (TRADE_MIRROR_V2, "mirror", "valor_fob_usd_reporter"),
    ],
)
def test_duplicate_columns_do_not_cause_attribute_error(contract, fixture, column, request):
    frame = request.getfixturevalue(fixture)
    frame = pd.concat([frame, frame[[column]]], axis=1)
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(frame, contract)
    assert erro.value.violation == f"Duplicate column labels: [{column!r}]"
