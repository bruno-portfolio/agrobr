from __future__ import annotations

from io import BytesIO
from typing import Any

import pandas as pd
import pytest
import xlrd

from agrobr.conab._serie_historica import parser
from agrobr.exceptions import ParseError
from tests import helpers

MANIFEST = helpers.load_serie_historica_manifest()
CASES = MANIFEST["cases"]


def _case(product: str) -> dict[str, Any]:
    return next(case for case in CASES if case["product"] == product)


def _raw(product: str) -> bytes:
    return (helpers.SERIE_HISTORICA_GOLDEN / _case(product)["file"]).read_bytes()


@pytest.mark.parametrize("case", CASES, ids=lambda item: item["id"])
def test_parser_matches_independent_oracle(case: dict[str, Any]):
    raw = (helpers.SERIE_HISTORICA_GOLDEN / case["file"]).read_bytes()
    frame = parser.records_to_dataframe(parser.parse_serie_historica(BytesIO(raw), case["product"]))
    helpers.assert_serie_historica_case(frame, case)


@pytest.mark.parametrize(
    "scenario,parameters",
    [
        ("test_duplicate_metric_fails_before_read", {}),
        ("test_normalized_names_cannot_collide", {}),
        ("test_required_sheet_missing", {}),
    ],
    ids=[
        "duplicate_metric_fails_before_read-0",
        "normalized_names_cannot_collide-0",
        "required_sheet_missing-0",
    ],
)
def test_abas_historicas_ambiguas_ou_ausentes(scenario: str, parameters: dict[str, Any]):
    with helpers.collect_failures() as check, check((scenario, parameters)):
        if scenario == "test_duplicate_metric_fails_before_read":
            names = ["Área", "Produção", "Produtividade", "Área suplementar"]
            with pytest.raises(ParseError, match="Abas duplicadas.*Área.*Área suplementar"):
                parser.resolve_sheets("soja", names)
        elif scenario == "test_normalized_names_cannot_collide":
            with pytest.raises(ParseError, match="nome ambiguo"):
                parser.resolve_sheets("cafe", ["Área em produção", " area  em producao "])
        elif scenario == "test_required_sheet_missing":
            with pytest.raises(ParseError, match="produto=cafe.*ausentes.*area em formacao"):
                parser.resolve_sheets("cafe", ["Área em produção", "Produção", "Produtividade"])


@pytest.mark.parametrize("filters", [{}, {"inicio": 2100}, {"uf": "XX"}])
def test_selected_sheet_without_state_rows_fails_before_filters(
    monkeypatch: pytest.MonkeyPatch, filters: dict[str, Any]
):
    original = parser.pd.read_excel

    def read(*args: Any, **kwargs: Any) -> pd.DataFrame:
        frame = original(*args, **kwargs)
        if kwargs.get("sheet_name") == "Produção":
            return frame.iloc[:6]
        return frame

    monkeypatch.setattr(parser.pd, "read_excel", read)
    with pytest.raises(ParseError, match="produto=soja.*Produção.*Nenhuma linha de UF"):
        parser.parse_serie_historica(BytesIO(_raw("soja")), "soja", **filters)


def test_unknown_sheet_warns_without_mapping():
    with helpers.capturar_logs() as logs:
        decisions = parser.resolve_sheets("soja", ["Área", "Produção", "Produtividade", "Notas"])
    assert decisions["Notas"].estado == "desconhecida"
    assert decisions["Notas"].campo is None
    assert any(
        entry["event"] == "conab_serie_historica_unknown_sheet"
        and entry["produto"] == "soja"
        and entry["sheet"] == "Notas"
        for entry in logs
    )


def test_selected_sheet_unreadable_fails_whole_file(monkeypatch: pytest.MonkeyPatch):
    original = parser.pd.read_excel

    def read(*args: Any, **kwargs: Any) -> pd.DataFrame:
        if kwargs.get("sheet_name") == "Produção":
            raise ValueError("fixture unreadable")
        return original(*args, **kwargs)

    monkeypatch.setattr(parser.pd, "read_excel", read)
    with pytest.raises(ParseError, match="produto=cafe.*Produção.*fixture unreadable"):
        parser.parse_serie_historica(BytesIO(_raw("cafe")), "cafe")


@pytest.mark.parametrize("case", CASES, ids=lambda item: item["id"])
def test_period_columns_include_last_published_forecast(case: dict[str, Any]):
    book = xlrd.open_workbook(str(helpers.SERIE_HISTORICA_GOLDEN / case["file"]))
    for sheet in book.sheets():
        expected = case["period_columns"][sheet.name]
        decisions = parser.resolve_period_columns(sheet.row_values(5))
        assert set(decisions) == {column["column"] for column in expected}
        for column in expected:
            assert decisions[column["column"]].model_dump() == {
                "estado": column["estado"],
                "safra": column["normalized"],
                "motivo": column["motivo"],
            }
            assert sheet.cell_value(5, column["column"]) == column["raw"]
        last = max(index for index, value in enumerate(sheet.row_values(5)) if str(value).strip())
        assert expected[-1]["column"] == last
        assert decisions[last].estado == "ignorada"
        assert decisions[last].motivo == "previsao"
