"""Testes para o parser ANDA."""

import sys

import pandas as pd
import pytest

from agrobr import exceptions
from agrobr.anda import parser
from agrobr.anda.parser import (
    _detect_month,
    _expand_newline_cells,
    _parse_indicadores,
    agregar_mensal,
    parse_entregas_table,
)
from tests.helpers import levanta_exatamente


class TestDetectMonth:
    def test_numeric(self):
        assert _detect_month("1") == 1
        assert _detect_month("12") == 12

    def test_out_of_range(self):
        assert _detect_month("13") is None
        assert _detect_month("0") is None


class TestParseEntregasTable:
    def test_empty_table(self):
        records = parse_entregas_table([], 2024)
        assert records == []


class TestAgregarMensal:
    def test_basic(self):
        data = [
            {
                "ano": 2024,
                "mes": 1,
                "uf": "MT",
                "produto_fertilizante": "total",
                "volume_ton": 150000,
            },
            {
                "ano": 2024,
                "mes": 1,
                "uf": "SP",
                "produto_fertilizante": "total",
                "volume_ton": 100000,
            },
            {
                "ano": 2024,
                "mes": 2,
                "uf": "MT",
                "produto_fertilizante": "total",
                "volume_ton": 120000,
            },
        ]
        df = pd.DataFrame(data)
        result = agregar_mensal(df)

        assert len(result) == 2
        jan = result[result["mes"] == 1].iloc[0]
        assert jan["volume_ton"] == 250000

    def test_empty(self):
        result = agregar_mensal(pd.DataFrame())
        assert result.empty


def _indicadores_single_section():
    """Tabela 'Principais Indicadores' com apenas uma secao (entregas)."""
    return [
        ["", "Fertilizantes Entregues ao Mercado (em toneladas de produto)", "", ""],
        ["", "", "2021", "2022"],
        ["", "Janeiro", "3.397.952", "3.200.000"],
        ["", "Fevereiro", "2.800.000", "2.600.000"],
        ["", "Março", "3.100.000", "2.900.000"],
        ["", "Abril", "3.400.000", "3.100.000"],
        ["", "Maio", "3.300.000", "3.000.000"],
        ["", "Junho", "3.500.000", "3.200.000"],
        ["", "Julho", "4.100.000", "3.800.000"],
        ["", "Agosto", "4.200.000", "3.900.000"],
        ["", "Setembro", "4.500.000", "4.100.000"],
        ["", "Outubro", "5.300.000", "4.800.000"],
        ["", "Novembro", "4.800.000", "4.400.000"],
        ["", "Dezembro", "3.500.000", "3.100.000"],
        ["", "Janeiro a Dezembro", "45.897.952", "42.100.000"],
    ]


def _indicadores_multi_section():
    """Tabela multi-secao simulando PDF real (entregas + producao + importacao)."""
    return [
        ["", "Fertilizantes Entregues ao Mercado (em toneladas de produto)", "", ""],
        ["", "", "2021", "2022"],
        ["", "Janeiro", "3.397.952", "3.200.000"],
        ["", "Fevereiro", "2.800.000", "2.600.000"],
        ["", "Março", "3.100.000", "2.900.000"],
        ["", "Abril", "3.400.000", "3.100.000"],
        ["", "Maio", "3.300.000", "3.000.000"],
        ["", "Junho", "3.500.000", "3.200.000"],
        ["", "Julho", "4.100.000", "3.800.000"],
        ["", "Agosto", "4.200.000", "3.900.000"],
        ["", "Setembro", "4.500.000", "4.100.000"],
        ["", "Outubro", "5.300.000", "4.800.000"],
        ["", "Novembro", "4.800.000", "4.400.000"],
        ["", "Dezembro", "3.500.000", "3.100.000"],
        ["", "Janeiro a Dezembro", "45.897.952", "42.100.000"],
        ["Producao Nacional de Fertilizantes Intermediarios (em toneladas)", "", "", ""],
        ["", "", "2021", "2022"],
        ["", "Janeiro", "700.000", "650.000"],
        ["", "Fevereiro", "600.000", "550.000"],
        ["", "Março", "650.000", "600.000"],
        ["", "Abril", "680.000", "620.000"],
        ["", "Maio", "660.000", "610.000"],
        ["", "Junho", "700.000", "640.000"],
        ["", "Julho", "720.000", "660.000"],
        ["", "Agosto", "730.000", "670.000"],
        ["", "Setembro", "710.000", "650.000"],
        ["", "Outubro", "740.000", "680.000"],
        ["", "Novembro", "750.000", "690.000"],
        ["", "Dezembro", "700.000", "640.000"],
    ]


class TestParseIndicadores:
    def test_multi_section_only_first(self):
        """Deve parar na primeira secao e ignorar producao/importacao."""
        records = _parse_indicadores(_indicadores_multi_section(), 2022)
        assert len(records) == 12

    def test_wrong_year_raises(self):
        with pytest.raises(exceptions.ParseError, match="Ano 2025 ausente"):
            _parse_indicadores(_indicadores_single_section(), 2025)

    def test_empty_table(self):
        records = _parse_indicadores([], 2022)
        assert records == []


class TestExpandNewlineCells:
    def test_below_threshold_not_expanded(self):
        table = [["UF", "V"], ["MT\nSP\nPR", "1\n2\n3"]]
        assert _expand_newline_cells(table) == [["UF", "V"], ["MT\nSP\nPR", "1\n2\n3"]]


class TestExpandIntegration:
    def test_multiline_cells_expanded_and_parsed(self):
        table = [
            ["Fertilizantes Entregues ao Mercado (em toneladas de produto)", ""],
            ["", "2024"],
            ["Janeiro\nFevereiro\nMarço\nAbril\nMaio", "1\n2\n3\n4\n50"],
        ]
        records = parse_entregas_table(table, 2024)

        assert [(r["mes"], r["volume_ton"]) for r in records] == [
            (1, 1.0),
            (2, 2.0),
            (3, 3.0),
            (4, 4.0),
            (5, 50.0),
        ]
        assert {r["uf"] for r in records} == {"BR"}


def test_tabela_com_duas_secoes_de_entregas_recusada():
    titulo = "Fertilizantes entregues ao mercado (em toneladas de produto)"
    tabela = [[titulo, ""], ["Janeiro", "1"], [titulo, ""], ["Fevereiro", "2"]]
    with levanta_exatamente(exceptions.ParseError, "Múltiplas seções"):
        parse_entregas_table(tabela, 2024)


def test_pdf_sem_tabela_recusado(monkeypatch):
    monkeypatch.setattr(parser, "extract_tables_from_pdf", lambda _pdf: [])
    with levanta_exatamente(exceptions.ParseError, "Nenhuma tabela"):
        parser.parse_entregas_pdf(b"%PDF", 2024)


def test_sem_pdfplumber_explica_a_instalacao(monkeypatch):
    monkeypatch.setitem(sys.modules, "pdfplumber", None)
    with levanta_exatamente(ImportError, "pip install agrobr"):
        parser.extract_tables_from_pdf(b"%PDF")
