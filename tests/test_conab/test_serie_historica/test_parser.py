from io import BytesIO
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from agrobr.conab.serie_historica import client as serie_client
from agrobr.conab.serie_historica import parser as serie_parser
from agrobr.conab.serie_historica.parser import (
    _classify_row,
    _find_header_row,
    _normalize_safra_header,
    parse_serie_historica,
    parse_sheet,
    records_to_dataframe,
)
from agrobr.exceptions import InvalidParameterError, ParseError
from tests import helpers


def _make_xls(sheets: dict[str, list[list]]) -> BytesIO:
    """Helper: cria arquivo Excel em memoria com multiplas abas."""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, rows in sheets.items():
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name=name, index=False, header=False)
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


class TestNormalizeSafraHeader:
    @pytest.mark.parametrize(
        "scenario,parameters",
        [("test_old_two_digit", {}), ("test_out_of_range_year", {})],
        ids=["old_two_digit-0", "out_of_range_year-0"],
    )
    def test_periodos_historicos_e_limites(self, scenario: str, parameters: dict[str, Any]):
        with helpers.collect_failures() as check, check((scenario, parameters)):
            if scenario == "test_old_two_digit":
                assert _normalize_safra_header("76/77") == "1976/77"
            elif scenario == "test_out_of_range_year":
                assert _normalize_safra_header("1900") is None


class TestParseSheet:
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

    def test_parse_year_only_headers(self):
        rows = [
            ["CONAB - Série Histórica - Café - Área (mil ha)", None, None],
            ["Região/UF", "2023", "2024"],
            ["MT", 100.0, 110.0],
        ]
        df = pd.DataFrame(rows)
        records = parse_sheet(df, "cafe", "area_plantada_mil_ha")

        assert {r.safra for r in records} == {"2023", "2024"}
        mt_2024 = [r for r in records if r.safra == "2024"][0]
        assert mt_2024.area_plantada_mil_ha == pytest.approx(110.0)


class TestParseSerieHistorica:
    def test_sorted_output(self):
        xls = _sample_xls()
        records = parse_serie_historica(xls, "soja")

        safras = [r.safra for r in records]
        for i in range(len(safras) - 1):
            assert safras[i] <= safras[i + 1]


class TestRecordsToDataframe:
    def test_numeric_columns_are_numeric(self):
        xls = _sample_xls()
        records = parse_serie_historica(xls, "soja")
        df = records_to_dataframe(records)

        assert df["area_plantada_mil_ha"].dtype in ("float64", "Float64")
        assert df["producao_mil_ton"].dtype in ("float64", "Float64")


class TestClassifyRow:
    def test_uf_in_longer_text(self):
        typ, reg, uf = _classify_row("Mato Grosso MT")
        assert typ == "uf"
        assert uf == "MT"


class TestFindHeaderRow:
    def test_raises_on_no_header(self):
        rows = [
            ["blah", "blah"],
            ["foo", "bar"],
        ]
        df = pd.DataFrame(rows)
        with pytest.raises(ParseError, match="linha de cabecalho com safras"):
            _find_header_row(df)


@pytest.mark.parametrize(
    "cenario",
    ["sem_coluna_de_safra", "arquivo_sem_abas", "produto_nao_string", "brasil_ambiguo"],
)
def test_guardas_de_layout_e_de_parametro(cenario: str, monkeypatch: pytest.MonkeyPatch):
    if cenario == "sem_coluna_de_safra":
        frame = pd.DataFrame(
            [["Região/UF", "2025/26 Previsão", "2026/27 Previsão"], ["MT", 1.0, 2.0]]
        )
        with pytest.raises(ParseError, match="Nenhuma coluna de safra encontrada"):
            parse_sheet(frame, "soja", "producao_mil_ton")
    elif cenario == "arquivo_sem_abas":
        monkeypatch.setattr(
            serie_parser, "open_excel_safe", lambda *_, **__: SimpleNamespace(sheet_names=[])
        )
        with pytest.raises(ParseError, match="Arquivo Excel sem abas"):
            parse_serie_historica(BytesIO(b"planilha"), "soja")
    elif cenario == "produto_nao_string":
        with pytest.raises(InvalidParameterError, match="produto deve ser uma string"):
            serie_client.get_xls_url(123)
    else:
        bruto = (helpers.SERIE_HISTORICA_GOLDEN / "soja.xls").read_bytes()
        ler = serie_parser._read_selected_sheet

        def duplicar_brasil(*args: Any) -> pd.DataFrame:
            frame = ler(*args)
            brasil = frame[frame[0].astype(str).str.strip() == "BRASIL"]
            return pd.concat([frame, brasil], ignore_index=True)

        monkeypatch.setattr(serie_parser, "_read_selected_sheet", duplicar_brasil)
        with pytest.raises(ParseError, match="Linha BRASIL ou período"):
            serie_parser.linha_brasil(bruto, "soja", ("2022/23", "2023"))
