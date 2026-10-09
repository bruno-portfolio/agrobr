"""Testes para o parser ABIOVE."""

import io

import openpyxl
import pandas as pd
import pytest

from agrobr.abiove.parser import (
    _detect_produto_from_header,
    _parse_meses_rows,
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


class TestDetectProdutoFromHeader:
    def test_total(self):
        assert _detect_produto_from_header("Total Geral") == "total"


class TestParseMesesRows:
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

    def test_secao_sem_produto_recusada(self):
        df = pd.DataFrame(
            [
                ["Mês", "Volume (t)", "US$ mil"],
                ["Janeiro", 5000000, 2500000],
                ["Fevereiro", 6000000, 3000000],
                ["Março", 5500000, 2750000],
            ]
        )

        with pytest.raises(ParseError, match="Seção sem produto identificado na aba Soja em Grão"):
            _parse_meses_rows(df, ano=2024, sheet_name="Soja em Grão")


class TestParseExportacaoExcel:
    def test_multiple_sheets(self):
        excel_data = _make_excel_bytes(
            {
                "Soja em Grão": [
                    ["Exportação de Soja em Grão"],
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

        assert len(df) == 6
        assert set(df["produto"].unique()) == {"grao", "farelo"}

    def test_aba_com_erro_levanta_em_vez_de_sumir(self):
        excel_data = _make_excel_bytes(
            {
                "Soja em Grão": [
                    ["Exportação de Soja em Grão"],
                    ["Mês", "Volume (t)", "US$ mil"],
                    ["Janeiro", 5000000, 2500000],
                    ["Fevereiro", 6000000, 3000000],
                    ["Março", 5500000, 2750000],
                ],
                "Sem Produto": [
                    ["Mês", "Volume (t)", "US$ mil"],
                    ["Janeiro", 2000000, 800000],
                    ["Fevereiro", 2200000, 880000],
                    ["Março", 2100000, 840000],
                ],
            }
        )

        with pytest.raises(ParseError, match="Seção sem produto identificado na aba Sem Produto"):
            parse_exportacao_excel(excel_data, ano=2024)


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
