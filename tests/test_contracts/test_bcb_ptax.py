from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.bcb import ptax_parser
from agrobr.contracts.bcb_ptax import BCB_PTAX_MOEDAS_V1, BCB_PTAX_V2
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente


@pytest.fixture
def quotes(ptax_captures):
    page = ptax_parser.parse_quotes_page(ptax_captures["bodies"]["usd_day"], "USD")
    return ptax_parser.build_quotes_frame(page.records)


@pytest.fixture
def currencies(ptax_captures):
    page = ptax_parser.parse_currencies_page(ptax_captures["bodies"]["currencies"])
    return ptax_parser.build_currencies_frame(page.records)


@pytest.mark.parametrize("contract,width", [(BCB_PTAX_V2, 8), (BCB_PTAX_MOEDAS_V1, 3)])
def test_empty_contract_is_typed_and_valid(contract, width):
    frame = contract.empty_frame()
    assert frame.empty and len(frame.columns) == width
    assert contract.validate(frame) == (True, [])
    if width == 8:
        assert str(frame["data_hora"].dtype) == str(frame["data"].dtype) == "datetime64[ns]"
        assert str(frame["cotacao_compra"].dtype) == "float64"


@pytest.mark.parametrize(
    "field,dtype",
    [
        ("data", "datetime64[us]"),
        ("data_hora", "datetime64[ms]"),
        ("cotacao_compra", "Float64"),
        ("paridade_venda", "object"),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
def test_wrong_quote_dtype_rejected_even_empty(field, dtype, empty, quotes):
    if empty:
        quotes = quotes.iloc[:0].copy()
    quotes[field] = quotes[field].astype(dtype)
    valid, errors = BCB_PTAX_V2.validate(quotes)
    assert not valid and errors


def test_civil_date_must_equal_published_timestamp_day(quotes):
    quotes.loc[0, "data"] += pd.Timedelta(days=1)
    assert not BCB_PTAX_V2.validate(quotes)[0]


def test_quote_order_cannot_be_repaired_by_contract(quotes):
    assert not BCB_PTAX_V2.validate(quotes.iloc[::-1])[0]


@pytest.mark.parametrize(
    "field,value",
    [
        ("moeda", "usd"),
        ("moeda", ""),
        ("moeda", 1),
        ("nome", " "),
        ("nome", None),
        ("tipo_moeda", ""),
        ("tipo_moeda", 1),
    ],
)
def test_invalid_catalog_identity_or_required_text_rejected(field, value, currencies):
    currencies[field] = currencies[field].astype(object)
    currencies.loc[0, field] = value
    valid, errors = BCB_PTAX_MOEDAS_V1.validate(currencies)
    assert not valid and errors


def test_catalog_unknown_type_is_valid_without_inventing_a_or_b(currencies):
    currencies.loc[0, "tipo_moeda"] = "C"
    assert BCB_PTAX_MOEDAS_V1.validate(currencies) == (True, [])


def test_catalog_duplicates_and_descending_order_rejected(currencies):
    assert not BCB_PTAX_MOEDAS_V1.validate(pd.concat([currencies.iloc[:1]] * 2))[0]
    assert not BCB_PTAX_MOEDAS_V1.validate(currencies.iloc[::-1])[0]


def test_duplicate_columns_report_error_in_both_contracts(quotes, currencies):
    for contract, frame, column in [
        (BCB_PTAX_V2, quotes, "cotacao_compra"),
        (BCB_PTAX_MOEDAS_V1, currencies, "moeda"),
    ]:
        with levanta_exatamente(ContractViolationError) as erro:
            contracts.validate_dataset(pd.concat([frame, frame[[column]]], axis=1), contract)
        assert erro.value.violation == f"Duplicate column labels: [{column!r}]"
