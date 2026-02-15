from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from agrobr.b3.models import TICKERS_AGRO
from agrobr.b3.parser import PARSER_VERSION, parse_ajustes_html
from agrobr.exceptions import ParseError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "b3" / "ajustes_sample"


def _golden_html() -> str:
    return GOLDEN_DIR.joinpath("response.html").read_text(encoding="utf-8")


def _golden_weekend_html() -> str:
    return GOLDEN_DIR.joinpath("response_weekend.html").read_text(encoding="utf-8")


def _expected() -> dict:
    return json.loads(GOLDEN_DIR.joinpath("expected.json").read_text(encoding="utf-8"))


class TestParseAjustesHtml:
    def test_golden_data_returns_dataframe(self):
        df = parse_ajustes_html(_golden_html())
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_golden_data_columns(self):
        df = parse_ajustes_html(_golden_html())
        expected = _expected()
        for col in expected["columns"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_golden_data_min_rows(self):
        df = parse_ajustes_html(_golden_html())
        expected = _expected()
        assert len(df) >= expected["agro_row_count_min"]

    def test_golden_data_tickers_are_agro_only(self):
        df = parse_ajustes_html(_golden_html())
        tickers_found = set(df["ticker"].unique())
        assert tickers_found.issubset(TICKERS_AGRO)

    def test_golden_data_all_expected_tickers_present(self):
        df = parse_ajustes_html(_golden_html())
        expected = _expected()
        tickers_found = set(df["ticker"].unique())
        for ticker in expected["agro_tickers"]:
            assert ticker in tickers_found, f"Missing ticker: {ticker}"

    def test_golden_data_bgi_sample(self):
        df = parse_ajustes_html(_golden_html())
        expected = _expected()["sample_bgi"]
        bgi = df[
            (df["ticker"] == expected["ticker"])
            & (df["vencimento_codigo"] == expected["vencimento_codigo"])
        ]
        assert len(bgi) == 1
        row = bgi.iloc[0]
        assert row["ajuste_anterior"] == pytest.approx(expected["ajuste_anterior"])
        assert row["ajuste_atual"] == pytest.approx(expected["ajuste_atual"])
        assert row["variacao"] == pytest.approx(expected["variacao"])

    def test_golden_data_sjc_sample(self):
        df = parse_ajustes_html(_golden_html())
        expected = _expected()["sample_sjc"]
        sjc = df[
            (df["ticker"] == expected["ticker"])
            & (df["vencimento_codigo"] == expected["vencimento_codigo"])
        ]
        assert len(sjc) == 1
        row = sjc.iloc[0]
        assert row["ajuste_anterior"] == pytest.approx(expected["ajuste_anterior"], rel=1e-3)
        assert row["ajuste_atual"] == pytest.approx(expected["ajuste_atual"], rel=1e-3)

    def test_golden_data_date_is_correct(self):
        df = parse_ajustes_html(_golden_html())
        data_ref = pd.Timestamp(date(2025, 2, 13))
        assert (df["data"] == data_ref).all()

    def test_vencimento_mes_range(self):
        df = parse_ajustes_html(_golden_html())
        assert df["vencimento_mes"].min() >= 1
        assert df["vencimento_mes"].max() <= 12

    def test_vencimento_ano_range(self):
        df = parse_ajustes_html(_golden_html())
        assert df["vencimento_ano"].min() >= 2020
        assert df["vencimento_ano"].max() <= 2035

    def test_ajuste_atual_positive(self):
        df = parse_ajustes_html(_golden_html())
        non_null = df["ajuste_atual"].dropna()
        assert (non_null > 0).all()

    def test_ajuste_anterior_positive(self):
        df = parse_ajustes_html(_golden_html())
        non_null = df["ajuste_anterior"].dropna()
        assert (non_null > 0).all()

    def test_variacao_can_be_negative(self):
        df = parse_ajustes_html(_golden_html())
        assert (df["variacao"] < 0).any()

    def test_unidade_present_for_all_rows(self):
        df = parse_ajustes_html(_golden_html())
        assert df["unidade"].notna().all()
        assert (df["unidade"] != "").all()

    def test_no_duplicate_pk(self):
        df = parse_ajustes_html(_golden_html())
        pk = df[["data", "ticker", "vencimento_codigo"]]
        assert not pk.duplicated().any()


class TestParseWeekend:
    def test_weekend_returns_empty_dataframe(self):
        df = parse_ajustes_html(_golden_weekend_html())
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_weekend_has_correct_columns(self):
        df = parse_ajustes_html(_golden_weekend_html())
        from agrobr.b3.models import COLUNAS_SAIDA

        for col in COLUNAS_SAIDA:
            assert col in df.columns


class TestParseEdgeCases:
    def test_empty_html_returns_empty(self):
        df = parse_ajustes_html("<html><body></body></html>")
        assert len(df) == 0

    def test_missing_table_raises(self):
        html = '<html><body><table id="outra"></table></body></html>'
        with pytest.raises(ParseError, match="tblDadosAjustes"):
            html_with_date = html.replace(
                "</body>",
                "<td>ATUALIZADO EM: 13/02/2025</td></body>",
            )
            parse_ajustes_html(html_with_date)

    def test_parser_version_is_integer(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1

    def test_table_with_only_header(self):
        html = """<html><body>
        <td>ATUALIZADO EM: 13/02/2025</td>
        <table id="tblDadosAjustes">
        <tr><th>Mercadoria</th><th>Vct</th><th>Aj Ant</th><th>Aj Atu</th><th>Var</th><th>Val</th></tr>
        </table></body></html>"""
        with pytest.raises(ParseError, match="apenas"):
            parse_ajustes_html(html)

    def test_non_agro_tickers_filtered(self):
        html = """<html><body>
        <td>ATUALIZADO EM: 13/02/2025</td>
        <table id="tblDadosAjustes">
        <tr><th>M</th><th>V</th><th>A</th><th>B</th><th>C</th><th>D</th></tr>
        <tr class="tabelaConteudo1"><td>DI1   - DI Futuro</td><td>G25</td><td>100,00</td><td>101,00</td><td>1,00</td><td>5,00</td></tr>
        <tr class="tabelaConteudo2"><td>BGI   - Boi gordo</td><td>G25</td><td>310,00</td><td>311,00</td><td>1,00</td><td>330,00</td></tr>
        </table></body></html>"""
        df = parse_ajustes_html(html)
        assert len(df) == 1
        assert df.iloc[0]["ticker"] == "BGI"
