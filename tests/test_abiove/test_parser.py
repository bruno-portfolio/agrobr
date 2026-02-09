"""Testes para o parser ABIOVE."""

import io

import openpyxl
import pandas as pd
import pytest

from agrobr.abiove.parser import (
    PARSER_VERSION,
    _detect_month,
    _detect_produto_from_header,
    _parse_meses_rows,
    _safe_float,
    agregar_mensal,
    parse_exportacao_excel,
)
from agrobr.exceptions import ParseError


def _make_excel_bytes(sheets: dict[str, list[list]]) -> bytes:
    """Cria arquivo Excel em memória a partir de dados de sheets.

    Args:
        sheets: Dict de sheet_name -> lista de linhas (cada linha é lista de valores).

    Returns:
        Bytes do arquivo .xlsx.
    """
    wb = openpyxl.Workbook()
    first = True
    for name, rows in sheets.items():
        if first:
            ws = wb.active
            ws.title = name
            first = False
        else:
            ws = wb.create_sheet(name)
        for row in rows:
            ws.append(row)

    buffer = io.BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


class TestSafeFloat:
    def test_integer(self):
        assert _safe_float(42) == 42.0

    def test_float(self):
        assert _safe_float(3.14) == 3.14

    def test_string_br_thousands(self):
        assert _safe_float("150.000") == 150000.0

    def test_string_br_decimal(self):
        assert _safe_float("1.234,56") == 1234.56

    def test_none(self):
        assert _safe_float(None) is None

    def test_dash(self):
        assert _safe_float("-") is None
        assert _safe_float("–") is None

    def test_empty(self):
        assert _safe_float("") is None

    def test_nd(self):
        assert _safe_float("n.d.") is None

    def test_nan(self):
        assert _safe_float(float("nan")) is None

    def test_multiple_dots_thousands(self):
        assert _safe_float("1.234.567") == 1234567.0

    def test_ellipsis(self):
        assert _safe_float("...") is None


class TestDetectMonth:
    def test_numeric(self):
        assert _detect_month("1") == 1
        assert _detect_month("12") == 12

    def test_name_full(self):
        assert _detect_month("Janeiro") == 1
        assert _detect_month("Fevereiro") == 2
        assert _detect_month("Dezembro") == 12

    def test_name_abbrev(self):
        assert _detect_month("Jan") == 1
        assert _detect_month("Fev") == 2
        assert _detect_month("Dez") == 12

    def test_invalid(self):
        assert _detect_month("Total") is None
        assert _detect_month("Acumulado") is None

    def test_out_of_range(self):
        assert _detect_month("13") is None
        assert _detect_month("0") is None

    def test_range_excluded(self):
        assert _detect_month("Janeiro a Dezembro") is None
        assert _detect_month("Jan/Dez") is None

    def test_none_input(self):
        assert _detect_month(None) is None

    def test_total_excluded(self):
        assert _detect_month("Total do Ano") is None

    def test_anual_excluded(self):
        assert _detect_month("Anual") is None


class TestDetectProdutoFromHeader:
    def test_grao(self):
        assert _detect_produto_from_header("Soja em Grão") == "grao"
        assert _detect_produto_from_header("Grain") == "grao"
        assert _detect_produto_from_header("Soybean") == "grao"

    def test_farelo(self):
        assert _detect_produto_from_header("Farelo de Soja") == "farelo"
        assert _detect_produto_from_header("Soybean Meal") == "farelo"

    def test_oleo(self):
        assert _detect_produto_from_header("Óleo de Soja") == "oleo"
        assert _detect_produto_from_header("Soybean Oil") == "oleo"
        assert _detect_produto_from_header("Oleo vegetal") == "oleo"

    def test_milho(self):
        assert _detect_produto_from_header("Milho") == "milho"
        assert _detect_produto_from_header("Corn") == "milho"

    def test_total(self):
        assert _detect_produto_from_header("Total Geral") == "total"

    def test_unknown(self):
        assert _detect_produto_from_header("Qualquer Coisa") is None

    def test_farelo_not_confused_with_grao(self):
        """Farelo contém 'soja' mas não deve ser detectado como grão."""
        assert _detect_produto_from_header("Farelo de Soja") == "farelo"

    def test_oleo_not_confused_with_grao(self):
        """Óleo de soja contém 'soja' mas não deve ser detectado como grão."""
        assert _detect_produto_from_header("Óleo de Soja") == "oleo"


class TestParseMesesRows:
    def test_basic_structure(self):
        """Testa parsing de formato com meses nas linhas."""
        df = pd.DataFrame(
            [
                ["Exportação", "Soja em Grão", None, "Farelo", None],
                ["Mês", "Volume (t)", "US$ mil", "Volume (t)", "US$ mil"],
                ["Janeiro", 5000000, 2500000, 2000000, 800000],
                ["Fevereiro", 6000000, 3000000, 2200000, 880000],
                ["Março", 5500000, 2750000, 2100000, 840000],
            ]
        )

        records = _parse_meses_rows(df, ano=2024, sheet_name="Export")

        assert len(records) >= 3  # Pelo menos 3 meses
        grao_jan = [r for r in records if r["produto"] == "grao" and r["mes"] == 1]
        assert len(grao_jan) == 1
        assert grao_jan[0]["volume_ton"] == 5000000

    def test_too_few_months_returns_empty(self):
        """Menos de 3 meses detectados retorna vazio."""
        df = pd.DataFrame(
            [
                ["Mês", "Volume"],
                ["Janeiro", 1000],
                ["Total", 1000],
            ]
        )

        records = _parse_meses_rows(df, ano=2024, sheet_name="Export")
        assert records == []

    def test_sheet_name_fallback(self):
        """Quando cabeçalhos não detectam produto, usa nome da sheet."""
        df = pd.DataFrame(
            [
                ["Mês", "Volume (t)", "US$ mil"],
                ["Janeiro", 5000000, 2500000],
                ["Fevereiro", 6000000, 3000000],
                ["Março", 5500000, 2750000],
            ]
        )

        records = _parse_meses_rows(df, ano=2024, sheet_name="Soja em Grão")

        assert len(records) == 3
        assert all(r["produto"] == "grao" for r in records)


class TestParseExportacaoExcel:
    def test_valid_excel_with_months(self):
        """Testa parsing de Excel válido com dados mensais."""
        excel_data = _make_excel_bytes(
            {
                "Soja em Grão": [
                    ["Exportação de Soja em Grão"],
                    ["Mês", "Volume (t)", "US$ mil"],
                    ["Janeiro", 5000000, 2500000],
                    ["Fevereiro", 6000000, 3000000],
                    ["Março", 5500000, 2750000],
                ]
            }
        )

        df = parse_exportacao_excel(excel_data, ano=2024)

        assert len(df) == 3
        assert "volume_ton" in df.columns
        assert "produto" in df.columns
        assert all(df["ano"] == 2024)

    def test_invalid_file_raises_parse_error(self):
        with pytest.raises(ParseError):
            parse_exportacao_excel(b"not an excel file", ano=2024)

    def test_empty_excel_raises_parse_error(self):
        """Excel sem dados reconhecíveis deve falhar."""
        excel_data = _make_excel_bytes(
            {
                "Vazio": [
                    ["Título sem dados"],
                    ["Rodapé"],
                ]
            }
        )

        with pytest.raises(ParseError, match="Nenhum dado"):
            parse_exportacao_excel(excel_data, ano=2024)

    def test_multiple_sheets(self):
        """Testa Excel com múltiplas sheets de produtos."""
        excel_data = _make_excel_bytes(
            {
                "Soja em Grão": [
                    ["Exportação"],
                    ["Mês", "Volume (t)", "US$ mil"],
                    ["Janeiro", 5000000, 2500000],
                    ["Fevereiro", 6000000, 3000000],
                    ["Março", 5500000, 2750000],
                ],
                "Farelo": [
                    ["Exportação de Farelo"],
                    ["Mês", "Volume (t)", "US$ mil"],
                    ["Janeiro", 2000000, 800000],
                    ["Fevereiro", 2200000, 880000],
                    ["Março", 2100000, 840000],
                ],
            }
        )

        df = parse_exportacao_excel(excel_data, ano=2024)

        # Deve ter dados de ambas sheets
        assert len(df) == 6
        assert set(df["produto"].unique()) == {"grao", "farelo"}

    def test_tabular_format(self):
        """Testa parsing de formato tabular com colunas nomeadas."""
        excel_data = _make_excel_bytes(
            {
                "Dados": [
                    ["produto", "mês", "volume_ton", "receita_usd"],
                    ["grao", "1", "5000000", "2500000"],
                    ["grao", "2", "6000000", "3000000"],
                    ["farelo", "1", "2000000", "800000"],
                ]
            }
        )

        df = parse_exportacao_excel(excel_data, ano=2024)

        assert len(df) == 3
        assert "grao" in df["produto"].values


class TestAgregarMensal:
    def test_basic(self):
        data = [
            {
                "ano": 2024,
                "mes": 1,
                "produto": "grao",
                "volume_ton": 5000000,
                "receita_usd_mil": 2500000,
            },
            {
                "ano": 2024,
                "mes": 1,
                "produto": "farelo",
                "volume_ton": 2000000,
                "receita_usd_mil": 800000,
            },
            {
                "ano": 2024,
                "mes": 2,
                "produto": "grao",
                "volume_ton": 6000000,
                "receita_usd_mil": 3000000,
            },
        ]
        df = pd.DataFrame(data)
        result = agregar_mensal(df)

        assert len(result) == 2
        jan = result[result["mes"] == 1].iloc[0]
        assert jan["volume_ton"] == 7000000
        assert jan["receita_usd_mil"] == 3300000

    def test_empty(self):
        result = agregar_mensal(pd.DataFrame())
        assert result.empty

    def test_sets_produto_total(self):
        data = [
            {"ano": 2024, "mes": 1, "produto": "grao", "volume_ton": 5000000},
            {"ano": 2024, "mes": 1, "produto": "farelo", "volume_ton": 2000000},
        ]
        df = pd.DataFrame(data)
        result = agregar_mensal(df)

        assert all(result["produto"] == "total")


class TestParserVersion:
    def test_version(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1
