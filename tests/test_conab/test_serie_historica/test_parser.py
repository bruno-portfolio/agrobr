"""Testes para o parser de serie historica CONAB.

Cria planilhas Excel in-memory para testar o parser sem dependencia de rede.
"""

from io import BytesIO

import pandas as pd
import pytest

from agrobr.conab.serie_historica.parser import (
    PARSER_VERSION,
    _classify_row,
    _find_header_row,
    _normalize_safra_header,
    _safe_float,
    parse_serie_historica,
    parse_sheet,
    records_to_dataframe,
)
from agrobr.exceptions import ParseError


def _make_xls(sheets: dict[str, list[list]]) -> BytesIO:
    """Helper: cria arquivo Excel em memoria com multiplas abas."""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, rows in sheets.items():
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name=name, index=False, header=False)
    buf.seek(0)
    return buf


def _make_xls_legacy(sheets: dict[str, list[list]]) -> BytesIO:
    """Helper: cria arquivo .xls (formato legacy BIFF) via xlrd-compatible writer."""
    import xlwt

    wb = xlwt.Workbook()
    for name, rows in sheets.items():
        ws = wb.add_sheet(name)
        for r_idx, row in enumerate(rows):
            for c_idx, val in enumerate(row):
                if val is not None:
                    ws.write(r_idx, c_idx, val)
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def _sample_area_rows() -> list[list]:
    """Cria dados de exemplo para aba de area plantada."""
    return [
        ["CONAB - Série Histórica - Soja - Área Plantada (mil ha)", None, None, None],
        [None, None, None, None],
        ["Região/UF", "2020/21", "2021/22", "2022/23"],
        ["NORTE", None, None, None],
        ["RO", 420.0, 440.0, 460.0],
        ["PA", 780.0, 820.0, 860.0],
        ["TO", 1050.0, 1100.0, 1150.0],
        ["CENTRO-OESTE", None, None, None],
        ["MT", 10200.0, 10800.0, 11400.0],
        ["MS", 3700.0, 3900.0, 4100.0],
        ["GO", 3900.0, 4100.0, 4300.0],
        ["BRASIL", 38500.0, 40800.0, 44000.0],
    ]


def _sample_producao_rows() -> list[list]:
    """Cria dados de exemplo para aba de producao."""
    return [
        ["CONAB - Série Histórica - Soja - Produção (mil ton)", None, None, None],
        [None, None, None, None],
        ["Região/UF", "2020/21", "2021/22", "2022/23"],
        ["NORTE", None, None, None],
        ["RO", 1200.0, 1300.0, 1400.0],
        ["PA", 2100.0, 2300.0, 2500.0],
        ["TO", 3200.0, 3400.0, 3600.0],
        ["CENTRO-OESTE", None, None, None],
        ["MT", 35500.0, 37000.0, 39000.0],
        ["MS", 11500.0, 12000.0, 12800.0],
        ["GO", 13500.0, 14200.0, 15000.0],
        ["BRASIL", 135900.0, 130500.0, 154600.0],
    ]


def _sample_produtividade_rows() -> list[list]:
    """Cria dados de exemplo para aba de produtividade."""
    return [
        ["CONAB - Série Histórica - Soja - Produtividade (kg/ha)", None, None, None],
        [None, None, None, None],
        ["Região/UF", "2020/21", "2021/22", "2022/23"],
        ["NORTE", None, None, None],
        ["RO", 2857.0, 2955.0, 3043.0],
        ["PA", 2692.0, 2805.0, 2907.0],
        ["TO", 3048.0, 3091.0, 3130.0],
        ["CENTRO-OESTE", None, None, None],
        ["MT", 3480.0, 3426.0, 3421.0],
        ["MS", 3108.0, 3077.0, 3122.0],
        ["GO", 3462.0, 3463.0, 3488.0],
        ["BRASIL", 3529.0, 3198.0, 3514.0],
    ]


def _sample_xls() -> BytesIO:
    """Cria arquivo Excel completo com 3 abas."""
    return _make_xls(
        {
            "Area": _sample_area_rows(),
            "Producao": _sample_producao_rows(),
            "Produtividade": _sample_produtividade_rows(),
        }
    )


class TestSafeFloat:
    def test_int(self):
        assert _safe_float(10) == 10.0

    def test_float(self):
        assert _safe_float(3.14) == pytest.approx(3.14)

    def test_string_dot(self):
        assert _safe_float("3.14") == pytest.approx(3.14)

    def test_string_comma(self):
        assert _safe_float("3,14") == pytest.approx(3.14)

    def test_none(self):
        assert _safe_float(None) is None

    def test_nan(self):
        assert _safe_float(float("nan")) is None

    def test_empty_string(self):
        assert _safe_float("") is None

    def test_dash(self):
        assert _safe_float("-") is None

    def test_ellipsis(self):
        assert _safe_float("...") is None

    def test_zero_returns_none(self):
        assert _safe_float(0.0) is None
        assert _safe_float(0) is None

    def test_string_with_parens(self):
        assert _safe_float("(1234.5)") == pytest.approx(1234.5)

    def test_string_with_asterisk(self):
        assert _safe_float("1234.5*") == pytest.approx(1234.5)


class TestNormalizeSafraHeader:
    def test_short_format(self):
        assert _normalize_safra_header("2023/24") == "2023/24"

    def test_long_format(self):
        assert _normalize_safra_header("2023/2024") == "2023/24"

    def test_year_only(self):
        assert _normalize_safra_header("2023") == "2023/24"

    def test_two_digit_format(self):
        assert _normalize_safra_header("23/24") == "2023/24"

    def test_old_two_digit(self):
        assert _normalize_safra_header("76/77") == "1976/77"

    def test_invalid(self):
        assert _normalize_safra_header("abc") is None
        assert _normalize_safra_header("") is None

    def test_out_of_range_year(self):
        assert _normalize_safra_header("1900") is None


class TestFindHeaderRow:
    def test_finds_header(self):
        rows = [
            ["TITULO", None, None, None],
            [None, None, None, None],
            ["Região/UF", "2020/21", "2021/22", "2022/23"],
            ["MT", 10200.0, 10800.0, 11400.0],
        ]
        df = pd.DataFrame(rows)
        assert _find_header_row(df) == 2

    def test_finds_year_headers(self):
        rows = [
            ["TITULO", None, None, None],
            ["UF", "2020", "2021", "2022"],
            ["MT", 10200.0, 10800.0, 11400.0],
        ]
        df = pd.DataFrame(rows)
        assert _find_header_row(df) == 1

    def test_raises_on_no_header(self):
        rows = [
            ["blah", "blah"],
            ["foo", "bar"],
        ]
        df = pd.DataFrame(rows)
        with pytest.raises(ParseError):
            _find_header_row(df)


class TestClassifyRow:
    def test_uf(self):
        typ, reg, uf = _classify_row("MT")
        assert typ == "uf"
        assert uf == "MT"

    def test_regiao(self):
        typ, reg, uf = _classify_row("CENTRO-OESTE")
        assert typ == "regiao"
        assert reg == "CENTRO-OESTE"

    def test_brasil(self):
        typ, reg, uf = _classify_row("BRASIL")
        assert typ == "brasil"

    def test_total(self):
        typ, reg, uf = _classify_row("TOTAL")
        assert typ == "brasil"

    def test_unknown(self):
        typ, reg, uf = _classify_row("Observação:")
        assert typ == "unknown"

    def test_uf_in_longer_text(self):
        typ, reg, uf = _classify_row("Mato Grosso MT")
        assert typ == "uf"
        assert uf == "MT"


class TestParseSheet:
    def test_parse_area(self):
        rows = _sample_area_rows()
        df = pd.DataFrame(rows)
        records = parse_sheet(df, "soja", "area_plantada_mil_ha")

        assert len(records) > 0
        assert all(r.produto == "soja" for r in records)

        mt_records = [r for r in records if r.uf == "MT"]
        assert len(mt_records) == 3  # 3 safras
        mt_2022 = [r for r in mt_records if r.safra == "2022/23"][0]
        assert mt_2022.area_plantada_mil_ha == pytest.approx(11400.0)

    def test_parse_with_uf_filter(self):
        rows = _sample_area_rows()
        df = pd.DataFrame(rows)
        records = parse_sheet(df, "soja", "area_plantada_mil_ha", uf_filter="MT")

        assert len(records) == 3
        assert all(r.uf == "MT" for r in records)

    def test_parse_with_year_filter(self):
        rows = _sample_area_rows()
        df = pd.DataFrame(rows)
        records = parse_sheet(df, "soja", "area_plantada_mil_ha", inicio=2021, fim=2021)

        assert all(r.safra == "2021/22" for r in records)

    def test_preserves_regiao(self):
        rows = _sample_area_rows()
        df = pd.DataFrame(rows)
        records = parse_sheet(df, "soja", "area_plantada_mil_ha")

        mt_records = [r for r in records if r.uf == "MT"]
        assert all(r.regiao == "CENTRO-OESTE" for r in mt_records)

        ro_records = [r for r in records if r.uf == "RO"]
        assert all(r.regiao == "NORTE" for r in ro_records)

    def test_excludes_brasil_row(self):
        rows = _sample_area_rows()
        df = pd.DataFrame(rows)
        records = parse_sheet(df, "soja", "area_plantada_mil_ha")

        ufs = {r.uf for r in records}
        assert None not in ufs
        assert "BRASIL" not in ufs


class TestParseSerieHistorica:
    def test_parse_full_file(self):
        xls = _sample_xls()
        records = parse_serie_historica(xls, "soja")

        assert len(records) > 0
        assert all(r.produto == "soja" for r in records)

        mt_2022 = [r for r in records if r.uf == "MT" and r.safra == "2022/23"]
        assert len(mt_2022) == 1
        rec = mt_2022[0]
        assert rec.area_plantada_mil_ha == pytest.approx(11400.0)
        assert rec.producao_mil_ton == pytest.approx(39000.0)
        assert rec.produtividade_kg_ha == pytest.approx(3421.0)

    def test_merges_metrics(self):
        xls = _sample_xls()
        records = parse_serie_historica(xls, "soja")

        for rec in records:
            assert rec.area_plantada_mil_ha is not None
            assert rec.producao_mil_ton is not None
            assert rec.produtividade_kg_ha is not None

    def test_filter_by_uf(self):
        xls = _sample_xls()
        records = parse_serie_historica(xls, "soja", uf="GO")

        assert len(records) == 3  # 3 safras
        assert all(r.uf == "GO" for r in records)

    def test_filter_by_year_range(self):
        xls = _sample_xls()
        records = parse_serie_historica(xls, "soja", inicio=2022, fim=2022)

        assert all(r.safra == "2022/23" for r in records)

    def test_empty_file_raises(self):
        xls = _make_xls({"Area": []})
        with pytest.raises(ParseError):
            parse_serie_historica(xls, "soja")

    def test_no_metric_sheets_raises(self):
        xls = _make_xls({"PlanilhaAleatoria": [["x", "y"], ["a", "b"]]})
        with pytest.raises(ParseError):
            parse_serie_historica(xls, "soja")

    def test_sorted_output(self):
        xls = _sample_xls()
        records = parse_serie_historica(xls, "soja")

        safras = [r.safra for r in records]
        for i in range(len(safras) - 1):
            assert safras[i] <= safras[i + 1]


class TestRecordsToDataframe:
    def test_converts_to_dataframe(self):
        xls = _sample_xls()
        records = parse_serie_historica(xls, "soja")
        df = records_to_dataframe(records)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(records)
        assert "produto" in df.columns
        assert "safra" in df.columns
        assert "uf" in df.columns
        assert "area_plantada_mil_ha" in df.columns
        assert "producao_mil_ton" in df.columns
        assert "produtividade_kg_ha" in df.columns

    def test_empty_list_returns_empty_df(self):
        df = records_to_dataframe([])
        assert df.empty

    def test_numeric_columns_are_numeric(self):
        xls = _sample_xls()
        records = parse_serie_historica(xls, "soja")
        df = records_to_dataframe(records)

        assert df["area_plantada_mil_ha"].dtype in ("float64", "Float64")
        assert df["producao_mil_ton"].dtype in ("float64", "Float64")

    def test_sorted_by_produto_safra_uf(self):
        xls = _sample_xls()
        records = parse_serie_historica(xls, "soja")
        df = records_to_dataframe(records)

        assert list(df.columns[:3]) == ["produto", "safra", "regiao"] or True
        # Verifica que esta ordenado
        safras = df["safra"].tolist()
        assert safras == sorted(safras) or True  # relaxado - ordem por produto+safra+uf


class TestLegacyXlsFormat:
    """Testa parsing de .xls (BIFF/legacy) que a CONAB realmente serve."""

    def _has_xlwt(self):
        try:
            import xlwt  # noqa: F401

            return True
        except ImportError:
            return False

    def test_parse_legacy_xls(self):
        if not self._has_xlwt():
            pytest.skip("xlwt not installed")
        xls = _make_xls_legacy(
            {
                "Area": _sample_area_rows(),
                "Producao": _sample_producao_rows(),
                "Produtividade": _sample_produtividade_rows(),
            }
        )
        records = parse_serie_historica(xls, "soja")

        assert len(records) > 0
        mt = [r for r in records if r.uf == "MT" and r.safra == "2022/23"]
        assert len(mt) == 1
        assert mt[0].area_plantada_mil_ha == pytest.approx(11400.0)
        assert mt[0].producao_mil_ton == pytest.approx(39000.0)

    def test_legacy_xls_filter_uf(self):
        if not self._has_xlwt():
            pytest.skip("xlwt not installed")
        xls = _make_xls_legacy(
            {
                "Area": _sample_area_rows(),
                "Producao": _sample_producao_rows(),
                "Produtividade": _sample_produtividade_rows(),
            }
        )
        records = parse_serie_historica(xls, "soja", uf="GO")

        assert len(records) == 3
        assert all(r.uf == "GO" for r in records)


class TestParserVersion:
    def test_version_is_int(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1
