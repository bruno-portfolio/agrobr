"""Testes para agrobr.alt.anp_diesel.parser."""

from __future__ import annotations

import io

import pandas as pd
import pytest

from agrobr.alt.anp_diesel import parser
from agrobr.exceptions import ParseError


def _make_precos_xlsx(
    rows: list[dict] | None = None,
    columns: list[str] | None = None,
) -> bytes:
    """Gera XLSX sintetico de precos."""
    if columns is None:
        columns = [
            "ESTADO - SIGLA",
            "MUNICÍPIO",
            "PRODUTO",
            "DATA INICIAL",
            "DATA FINAL",
            "PREÇO MÉDIO REVENDA",
            "PREÇO MÉDIO DISTRIBUIÇÃO",
            "NÚMERO DE POSTOS PESQUISADOS",
        ]

    if rows is None:
        rows = [
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "01/01/2024",
                "DATA FINAL": "07/01/2024",
                "PREÇO MÉDIO REVENDA": "6.45",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.80",
                "NÚMERO DE POSTOS PESQUISADOS": "150",
            },
            {
                "ESTADO - SIGLA": "MT",
                "MUNICÍPIO": "CUIABA",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "01/01/2024",
                "DATA FINAL": "07/01/2024",
                "PREÇO MÉDIO REVENDA": "6.20",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.60",
                "NÚMERO DE POSTOS PESQUISADOS": "80",
            },
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "DIESEL",
                "DATA INICIAL": "01/01/2024",
                "DATA FINAL": "07/01/2024",
                "PREÇO MÉDIO REVENDA": "5.95",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.30",
                "NÚMERO DE POSTOS PESQUISADOS": "120",
            },
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "GASOLINA COMUM",
                "DATA INICIAL": "01/01/2024",
                "DATA FINAL": "07/01/2024",
                "PREÇO MÉDIO REVENDA": "5.50",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "4.80",
                "NÚMERO DE POSTOS PESQUISADOS": "200",
            },
            {
                "ESTADO - SIGLA": "MT",
                "MUNICÍPIO": "CUIABA",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "08/01/2024",
                "DATA FINAL": "14/01/2024",
                "PREÇO MÉDIO REVENDA": "6.30",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.65",
                "NÚMERO DE POSTOS PESQUISADOS": "82",
            },
        ]

    df = pd.DataFrame(rows, columns=columns)
    df["UNIDADE DE MEDIDA"] = "R$/l"
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def _make_vendas_csv(
    rows: list[dict] | None = None,
) -> bytes:
    """Gera CSV sintetico de vendas diesel (formato dados abertos ANP)."""
    if rows is None:
        rows = [
            {
                "ANO": "2024",
                "MES": "JAN",
                "GRANDE REGIAO": "REGIAO CENTRO-OESTE",
                "UNIDADE DA FEDERACAO": "MATO GROSSO",
                "PRODUTO": "OLEO DIESEL",
                "VENDAS": "500000",
            },
            {
                "ANO": "2024",
                "MES": "JAN",
                "GRANDE REGIAO": "REGIAO SUDESTE",
                "UNIDADE DA FEDERACAO": "SAO PAULO",
                "PRODUTO": "OLEO DIESEL",
                "VENDAS": "800000",
            },
            {
                "ANO": "2024",
                "MES": "FEV",
                "GRANDE REGIAO": "REGIAO CENTRO-OESTE",
                "UNIDADE DA FEDERACAO": "MATO GROSSO",
                "PRODUTO": "OLEO DIESEL",
                "VENDAS": "520000",
            },
            {
                "ANO": "2024",
                "MES": "JAN",
                "GRANDE REGIAO": "REGIAO CENTRO-OESTE",
                "UNIDADE DA FEDERACAO": "MATO GROSSO",
                "PRODUTO": "GASOLINA C",
                "VENDAS": "300000",
            },
            {
                "ANO": "2024",
                "MES": "FEV",
                "GRANDE REGIAO": "REGIAO SUDESTE",
                "UNIDADE DA FEDERACAO": "SAO PAULO",
                "PRODUTO": "OLEO DIESEL S-10",
                "VENDAS": "850000",
            },
        ]

    df = pd.DataFrame(rows)
    return df.to_csv(index=False, sep=";").encode("utf-8")


class TestParsePrecos:
    def test_xlsx_vazio_raise(self):
        df_vazio = pd.DataFrame()
        buf = io.BytesIO()
        df_vazio.to_excel(buf, index=False, engine="openpyxl")
        with pytest.raises(ParseError, match="vazio"):
            parser.parse_precos(buf.getvalue())

    def test_sem_diesel_raise(self):
        rows = [
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "GASOLINA COMUM",
                "DATA INICIAL": "01/01/2024",
                "DATA FINAL": "07/01/2024",
                "PREÇO MÉDIO REVENDA": "5.50",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "4.80",
                "NÚMERO DE POSTOS PESQUISADOS": "200",
            },
        ]
        content = _make_precos_xlsx(rows=rows)
        with pytest.raises(ParseError, match="diesel"):
            parser.parse_precos(content)

    def test_bytes_invalidos_raise(self):
        with pytest.raises(ParseError, match="Erro ao ler"):
            parser.parse_precos(b"isso nao eh xlsx")


class TestParseVendas:
    def test_sem_colunas_obrigatorias_raise(self):
        content = b"OUTRA;COLUNA\nval1;val2\n"
        with pytest.raises(ParseError, match="obrigatorias"):
            parser.parse_vendas(content)

    def test_bytes_invalidos_raise(self):
        with pytest.raises(ParseError, match="(Erro ao ler|vazio)"):
            parser.parse_vendas(b"\x00\x01\x02\x03")


class TestOleoDieselNormalization:
    """Testa que OLEO DIESEL e variantes sao normalizados para DIESEL."""

    def test_vendas_oleo_diesel_normalizado(self):
        content = _make_vendas_csv()
        df = parser.parse_vendas(content)
        for p in df["produto"]:
            assert p in ("DIESEL", "DIESEL S-10"), f"Produto nao normalizado: {p}"


class TestEstadoNomeCompleto:
    """Testa que nome completo de estado e convertido para sigla."""

    def test_precos_filtro_uf_com_nome_completo(self):
        rows = [
            {
                "ESTADO": "MATO GROSSO",
                "MUNICÍPIO": "CUIABA",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "01/01/2024",
                "DATA FINAL": "07/01/2024",
                "PREÇO MÉDIO REVENDA": "6.20",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.60",
                "NÚMERO DE POSTOS PESQUISADOS": "80",
            },
            {
                "ESTADO": "SÃO PAULO",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "01/01/2024",
                "DATA FINAL": "07/01/2024",
                "PREÇO MÉDIO REVENDA": "6.45",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.80",
                "NÚMERO DE POSTOS PESQUISADOS": "150",
            },
        ]
        columns = [
            "ESTADO",
            "MUNICÍPIO",
            "PRODUTO",
            "DATA INICIAL",
            "DATA FINAL",
            "PREÇO MÉDIO REVENDA",
            "PREÇO MÉDIO DISTRIBUIÇÃO",
            "NÚMERO DE POSTOS PESQUISADOS",
        ]
        content = _make_precos_xlsx(rows=rows, columns=columns)
        df = parser.parse_precos(content, uf="MT")
        assert len(df) == 1
        assert df["uf"].iloc[0] == "MT"


class TestHelpers:
    def test_find_column_case_insensitive(self):
        df = pd.DataFrame({"Produto": [1], "Estado - Sigla": [2]})
        df = parser._normalize_columns(df)
        assert parser._find_column(df, ["PRODUTO"]) == "PRODUTO"
        assert parser._find_column(df, ["ESTADO - SIGLA"]) == "ESTADO - SIGLA"
