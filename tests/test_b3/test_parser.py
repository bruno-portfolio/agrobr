from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd
import pytest
from lxml import etree

from agrobr.b3.models import COLUNAS_OI_SAIDA
from agrobr.b3.parser import (
    _attr_float,
    _classificar_tipo,
    _extract_record,
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
    def test_corrupted_raises_parse_error(self):
        with pytest.raises(ParseError, match="ZIP externo corrompido"):
            parse_ajustes_zip(b"not a zip file at all")


def _golden_oi_csv() -> bytes:
    return GOLDEN_OI_DIR.joinpath("response.csv").read_bytes()


class TestParsePosicoesAbertasVencimento:
    def test_vencimento_ano_range(self):
        df = parse_posicoes_abertas(_golden_oi_csv())
        valid = df["vencimento_ano"].dropna()
        assert valid.min() >= 2020
        assert valid.max() <= 2035


class TestParsePosicoesAbertasVazio:
    def test_missing_sgmtnm_raises(self):
        csv = b"RptDt;TckrSymb;Asst;OpnIntrst\n2025-12-19;BGIF26;BGI;1000\n"
        with pytest.raises(ParseError, match="SgmtNm"):
            parse_posicoes_abertas(csv)


# ============================================================================
# Coverage: edge cases for parse_ajustes_zip inner errors
# ============================================================================


class TestParseAjustesZipEdgeCases:
    def test_no_inner_zip_raises(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "no zip inside")
        with pytest.raises(ParseError, match="ZIP interno nao encontrado"):
            parse_ajustes_zip(buf.getvalue())

    def test_no_xml_in_inner_zip_raises(self):
        inner_buf = io.BytesIO()
        with zipfile.ZipFile(inner_buf, "w") as zf:
            zf.writestr("readme.txt", "no xml here")

        outer_buf = io.BytesIO()
        with zipfile.ZipFile(outer_buf, "w") as zf:
            zf.writestr("inner.zip", inner_buf.getvalue())

        with pytest.raises(ParseError, match="Nenhum XML encontrado"):
            parse_ajustes_zip(outer_buf.getvalue())

    def test_corrupted_inner_zip_raises(self):
        outer_buf = io.BytesIO()
        with zipfile.ZipFile(outer_buf, "w") as zf:
            zf.writestr("inner.zip", b"not a valid zip file")

        with pytest.raises(ParseError, match="ZIP interno corrompido"):
            parse_ajustes_zip(outer_buf.getvalue())


class TestExtractRecordEdgeCases:
    def _make_elem(self, ticker_symb: str, *, with_attrs: bool = True) -> etree._Element:
        from lxml import etree

        ns = "urn:bvmf.217.01.xsd"
        xml_str = (
            f'<PricRpt xmlns="{ns}">'
            f"<TradDt><Dt>2026-02-27</Dt></TradDt>"
            f"<SctyId><TckrSymb>{ticker_symb}</TckrSymb></SctyId>"
        )
        if with_attrs:
            xml_str += (
                "<FinInstrmAttrbts>"
                "<PrvsAdjstdQt>100.0</PrvsAdjstdQt>"
                "<AdjstdQt>101.0</AdjstdQt>"
                "<VartnPts>1.0</VartnPts>"
                "<AdjstdValCtrct>330.0</AdjstdValCtrct>"
                "</FinInstrmAttrbts>"
            )
        xml_str += "</PricRpt>"
        return etree.fromstring(xml_str)

    def test_non_agro_ticker_returns_none(self):
        elem = self._make_elem("DI1F26")
        assert _extract_record(elem) is None

    def test_bad_vencimento_returns_none(self):
        elem = self._make_elem("BGIZ99")
        from unittest.mock import patch

        with patch.dict("agrobr.b3.models.MONTH_CODES", {}, clear=True):
            result = _extract_record(elem)
        assert result is None

    def test_missing_attrs_returns_none(self):
        elem = self._make_elem("BGIH27", with_attrs=False)
        assert _extract_record(elem) is None


class TestAttrFloat:
    def test_non_numeric_text(self):
        from lxml import etree

        parent = etree.fromstring("<root><val>abc</val></root>")
        assert _attr_float(parent, "val") is None


class TestClassificarTipo:
    def test_short_ticker_default_futuro(self):
        assert _classificar_tipo("ABCDEF") == "futuro"


class TestParsePosicoesAbertasMoreEdges:
    def test_pandas_empty_data_error_returns_empty(self):
        from unittest.mock import patch

        with patch(
            "agrobr.b3.parser.pd.read_csv", side_effect=pd.errors.EmptyDataError("No columns")
        ):
            df = parse_posicoes_abertas(b"some content")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_agro_segment_no_matching_tickers(self):
        csv = (
            b"RptDt;TckrSymb;ISIN;Asst;XprtnCd;SgmtNm;OpnIntrst;VartnOpnIntrst;"
            b"DstrbtnId;CvrdQty;TtlBlckdPos;UcvrdQty;TtlPos;BrrwrQty;LndrQty;CurQty;FwdPric\n"
            b"2025-12-19;ZZZZF26;BRZZZZACNPR6;ZZZZ;F26;AGRIBUSINESS;1000;50;;;;;;;;\n"
        )
        df = parse_posicoes_abertas(csv)
        assert len(df) == 0
        for col in COLUNAS_OI_SAIDA:
            assert col in df.columns


class TestExtractRecordNonAgroTicker:
    def test_valid_future_pattern_but_not_agro_returns_none(self):
        from lxml import etree

        ns = "urn:bvmf.217.01.xsd"
        xml_str = (
            f'<PricRpt xmlns="{ns}">'
            f"<TradDt><Dt>2026-02-27</Dt></TradDt>"
            f"<SctyId><TckrSymb>ABCF27</TckrSymb></SctyId>"
            f"<FinInstrmAttrbts>"
            f"<PrvsAdjstdQt>100.0</PrvsAdjstdQt>"
            f"<AdjstdQt>101.0</AdjstdQt>"
            f"<VartnPts>1.0</VartnPts>"
            f"<AdjstdValCtrct>330.0</AdjstdValCtrct>"
            f"</FinInstrmAttrbts>"
            f"</PricRpt>"
        )
        elem = etree.fromstring(xml_str)
        assert _extract_record(elem) is None


class TestParseBvmfXmlMemoryCleanup:
    def test_only_non_agro_records_returns_empty(self):
        xml = _make_bvmf_xml(
            _make_pric_rpt("ABCF27"),
            _make_pric_rpt("XYZH26"),
        )
        zip_bytes = _make_nested_zip(xml)
        df = parse_ajustes_zip(zip_bytes)
        assert len(df) == 0


class TestParseBvmfXmlRootElement:
    def test_pric_rpt_as_root_parent_is_none(self):
        from agrobr.b3.parser import _parse_bvmf_xml

        ns = "urn:bvmf.217.01.xsd"
        xml = (
            f'<?xml version="1.0"?>'
            f'<PricRpt xmlns="{ns}">'
            f"<TradDt><Dt>2026-02-27</Dt></TradDt>"
            f"<SctyId><TckrSymb>BGIH27</TckrSymb></SctyId>"
            f"<FinInstrmAttrbts>"
            f"<PrvsAdjstdQt>100.0</PrvsAdjstdQt>"
            f"<AdjstdQt>101.0</AdjstdQt>"
            f"<VartnPts>1.0</VartnPts>"
            f"<AdjstdValCtrct>330.0</AdjstdValCtrct>"
            f"</FinInstrmAttrbts>"
            f"</PricRpt>"
        ).encode()
        records = _parse_bvmf_xml(io.BytesIO(xml))
        assert len(records) == 1
        assert records[0]["ticker"] == "BGI"
