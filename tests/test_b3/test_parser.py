from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from agrobr.b3.models import COLUNAS_OI_SAIDA, COLUNAS_SAIDA, TICKERS_AGRO_OI
from agrobr.b3.parser import (
    PARSER_VERSION_OI,
    PARSER_VERSION_ZIP,
    parse_ajustes_zip,
    parse_posicoes_abertas,
)
from agrobr.exceptions import ParseError

GOLDEN_OI_DIR = Path(__file__).parent.parent / "golden_data" / "b3" / "posicoes_sample"


def _make_bvmf_xml(*pric_rpts: str) -> bytes:
    body = "".join(pric_rpts)
    return (
        f'<?xml version="1.0" encoding="utf-8"?>'
        f'<Envelope xmlns="urn:bvmf.052.01.xsd">'
        f'<Body xmlns="urn:bvmf.217.01.xsd">{body}</Body>'
        f"</Envelope>"
    ).encode()


def _make_pric_rpt(
    ticker_symb: str,
    trade_dt: str = "2026-02-27",
    prev_adj: str = "353.00",
    adj: str = "352.00",
    var: str = "-1.00",
    adj_val: str = "-330.00",
) -> str:
    ns = "urn:bvmf.217.01.xsd"
    return (
        f'<PricRpt xmlns="{ns}">'
        f"<TradDt><Dt>{trade_dt}</Dt></TradDt>"
        f"<SctyId><TckrSymb>{ticker_symb}</TckrSymb></SctyId>"
        f"<FinInstrmAttrbts>"
        f"<PrvsAdjstdQt>{prev_adj}</PrvsAdjstdQt>"
        f"<AdjstdQt>{adj}</AdjstdQt>"
        f"<VartnPts>{var}</VartnPts>"
        f"<AdjstdValCtrct>{adj_val}</AdjstdValCtrct>"
        f"</FinInstrmAttrbts>"
        f"</PricRpt>"
    )


def _make_nested_zip(xml_content: bytes) -> bytes:
    inner_buf = io.BytesIO()
    with zipfile.ZipFile(inner_buf, "w", zipfile.ZIP_DEFLATED) as inner_zf:
        inner_zf.writestr("PR26022703.xml", xml_content)
    inner_buf.seek(0)

    outer_buf = io.BytesIO()
    with zipfile.ZipFile(outer_buf, "w", zipfile.ZIP_DEFLATED) as outer_zf:
        outer_zf.writestr("BVBG086.zip", inner_buf.getvalue())
    outer_buf.seek(0)
    return outer_buf.getvalue()


class TestParseAjustesZip:
    def test_valid(self):
        xml = _make_bvmf_xml(
            _make_pric_rpt("BGIH27", prev_adj="353", adj="352", var="-1", adj_val="-330"),
            _make_pric_rpt("CCMK26", prev_adj="70.5", adj="71.0", var="0.5", adj_val="135"),
        )
        zip_bytes = _make_nested_zip(xml)
        df = parse_ajustes_zip(zip_bytes)
        assert len(df) == 2
        assert set(df["ticker"]) == {"BGI", "CCM"}
        assert df.iloc[0]["descricao"] == "boi"
        assert df.iloc[1]["descricao"] == "milho"

    def test_empty_xml(self):
        xml = _make_bvmf_xml()
        zip_bytes = _make_nested_zip(xml)
        df = parse_ajustes_zip(zip_bytes)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_filters_non_agro(self):
        xml = _make_bvmf_xml(
            _make_pric_rpt("BGIH27"),
            _make_pric_rpt("DI1F26"),
        )
        zip_bytes = _make_nested_zip(xml)
        df = parse_ajustes_zip(zip_bytes)
        assert len(df) == 1
        assert df.iloc[0]["ticker"] == "BGI"

    def test_columns_match(self):
        xml = _make_bvmf_xml(_make_pric_rpt("BGIH27"))
        zip_bytes = _make_nested_zip(xml)
        df = parse_ajustes_zip(zip_bytes)
        assert df.columns.tolist() == COLUNAS_SAIDA

    def test_dtypes(self):
        xml = _make_bvmf_xml(_make_pric_rpt("BGIH27"))
        zip_bytes = _make_nested_zip(xml)
        df = parse_ajustes_zip(zip_bytes)
        assert pd.api.types.is_datetime64_any_dtype(df["data"])
        for col in ["ajuste_anterior", "ajuste_atual", "variacao", "ajuste_por_contrato"]:
            assert df[col].dtype == "float64"

    def test_corrupted_raises_parse_error(self):
        with pytest.raises(ParseError, match="ZIP externo corrompido"):
            parse_ajustes_zip(b"not a zip file at all")

    def test_parser_version_zip(self):
        assert isinstance(PARSER_VERSION_ZIP, int)
        assert PARSER_VERSION_ZIP >= 1

    def test_vencimento_parsed(self):
        xml = _make_bvmf_xml(_make_pric_rpt("BGIH27"))
        zip_bytes = _make_nested_zip(xml)
        df = parse_ajustes_zip(zip_bytes)
        assert df.iloc[0]["vencimento_codigo"] == "H27"
        assert df.iloc[0]["vencimento_mes"] == 3
        assert df.iloc[0]["vencimento_ano"] == 2027


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

    def test_independent_of_zip(self):
        assert PARSER_VERSION_OI >= 1
        assert PARSER_VERSION_ZIP >= 1


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
