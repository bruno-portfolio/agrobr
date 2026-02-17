from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from agrobr.b3.models import COLUNAS_OI_SAIDA, TICKERS_AGRO, TICKERS_AGRO_OI
from agrobr.b3.parser import (
    PARSER_VERSION,
    PARSER_VERSION_OI,
    parse_ajustes_html,
    parse_posicoes_abertas,
)
from agrobr.exceptions import ParseError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "b3" / "ajustes_sample"
GOLDEN_OI_DIR = Path(__file__).parent.parent / "golden_data" / "b3" / "posicoes_sample"


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


def _golden_oi_csv() -> bytes:
    return GOLDEN_OI_DIR.joinpath("response.csv").read_bytes()


def _expected_oi() -> dict:
    return json.loads(GOLDEN_OI_DIR.joinpath("expected.json").read_text(encoding="utf-8"))


class TestParsePosicoesAbertas:
    def test_golden_data_returns_dataframe(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_golden_data_columns(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        expected = _expected_oi()
        for col in expected["columns"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_golden_data_row_count(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        expected = _expected_oi()
        assert len(df) == expected["total_rows"]

    def test_golden_data_tickers_are_agro(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        tickers_found = set(df["ticker"].unique())
        assert tickers_found.issubset(TICKERS_AGRO_OI)

    def test_golden_data_all_expected_tickers(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        expected = _expected_oi()
        tickers_found = set(df["ticker"].unique())
        for ticker in expected["agro_tickers"]:
            assert ticker in tickers_found, f"Missing ticker: {ticker}"

    def test_golden_data_bgi_sample(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        expected = _expected_oi()["sample_bgi"]
        bgi = df[df["ticker_completo"] == expected["ticker_completo"]]
        assert len(bgi) == 1
        row = bgi.iloc[0]
        assert row["posicoes_abertas"] == expected["posicoes_abertas"]
        assert row["variacao_posicoes"] == expected["variacao_posicoes"]

    def test_golden_data_ccm_sample(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        expected = _expected_oi()["sample_ccm"]
        ccm = df[df["ticker_completo"] == expected["ticker_completo"]]
        assert len(ccm) == 1
        row = ccm.iloc[0]
        assert row["posicoes_abertas"] == expected["posicoes_abertas"]
        assert row["variacao_posicoes"] == expected["variacao_posicoes"]


class TestParsePosicoesAbertasTipos:
    def test_futures_count(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        expected = _expected_oi()
        assert len(df[df["tipo"] == "futuro"]) == expected["futures_count"]

    def test_options_count(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        expected = _expected_oi()
        assert len(df[df["tipo"] == "opcao"]) == expected["options_count"]

    def test_tipo_only_futuro_or_opcao(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        assert set(df["tipo"].unique()).issubset({"futuro", "opcao"})


class TestParsePosicoesAbertasVencimento:
    def test_vencimento_mes_range(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        valid = df["vencimento_mes"].dropna()
        assert valid.min() >= 1
        assert valid.max() <= 12

    def test_vencimento_ano_range(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        valid = df["vencimento_ano"].dropna()
        assert valid.min() >= 2020
        assert valid.max() <= 2035

    def test_futures_have_valid_vencimento(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        futuros = df[df["tipo"] == "futuro"]
        assert futuros["vencimento_mes"].notna().all()
        assert futuros["vencimento_ano"].notna().all()


class TestParsePosicoesAbertasVazio:
    def test_empty_bytes_returns_empty(self):
        df = parse_posicoes_abertas(b"")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        for col in COLUNAS_OI_SAIDA:
            assert col in df.columns

    def test_header_only_csv_returns_empty(self):
        csv = b"RptDt;TckrSymb;ISIN;Asst;XprtnCd;SgmtNm;OpnIntrst;VartnOpnIntrst;DstrbtnId;CvrdQty;TtlBlckdPos;UcvrdQty;TtlPos;BrrwrQty;LndrQty;CurQty;FwdPric\n"
        df = parse_posicoes_abertas(csv)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_no_agro_rows_returns_empty(self):
        csv = (
            b"RptDt;TckrSymb;ISIN;Asst;XprtnCd;SgmtNm;OpnIntrst;VartnOpnIntrst;DstrbtnId;CvrdQty;TtlBlckdPos;UcvrdQty;TtlPos;BrrwrQty;LndrQty;CurQty;FwdPric\n"
            b"2025-12-19;PETR4;BRPETRACNPR6;PETR;G26;EQUITY;1000;50;;;;;;;;\n"
        )
        df = parse_posicoes_abertas(csv)
        assert len(df) == 0

    def test_missing_sgmtnm_raises(self):
        csv = b"RptDt;TckrSymb;Asst;OpnIntrst\n2025-12-19;BGIF26;BGI;1000\n"
        with pytest.raises(ParseError, match="SgmtNm"):
            parse_posicoes_abertas(csv)


class TestParserVersionOI:
    def test_is_integer(self):
        assert isinstance(PARSER_VERSION_OI, int)

    def test_at_least_one(self):
        assert PARSER_VERSION_OI >= 1

    def test_independent_of_ajustes(self):
        assert PARSER_VERSION_OI >= 1
        assert PARSER_VERSION >= 1


class TestParsePosicoesAbertasDescricao:
    def test_bgi_descricao(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        bgi = df[df["ticker"] == "BGI"]
        assert (bgi["descricao"] == "boi").all()

    def test_ccm_descricao(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        ccm = df[df["ticker"] == "CCM"]
        assert (ccm["descricao"] == "milho").all()

    def test_unidade_present(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        assert df["unidade"].notna().all()
        assert (df["unidade"] != "").all()


class TestParsePosicoesAbertasPK:
    def test_no_duplicate_pk(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        pk = df[["data", "ticker_completo"]]
        assert not pk.duplicated().any()

    def test_posicoes_abertas_non_negative(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        assert (df["posicoes_abertas"] >= 0).all()
