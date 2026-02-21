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
    def test_parse_basico(self):
        content = _make_precos_xlsx()
        df = parser.parse_precos(content)
        assert not df.empty
        assert "data" in df.columns
        assert "uf" in df.columns
        assert "preco_venda" in df.columns
        assert "margem" in df.columns

    def test_filtra_diesel_somente(self):
        content = _make_precos_xlsx()
        df = parser.parse_precos(content)
        produtos = df["produto"].unique()
        assert all("DIESEL" in p for p in produtos)
        assert "GASOLINA COMUM" not in produtos

    def test_filtro_produto_s10(self):
        content = _make_precos_xlsx()
        df = parser.parse_precos(content, produto="DIESEL S10")
        assert all(df["produto"] == "DIESEL S10")

    def test_filtro_produto_diesel(self):
        content = _make_precos_xlsx()
        df = parser.parse_precos(content, produto="DIESEL")
        assert all(df["produto"] == "DIESEL")

    def test_filtro_uf(self):
        content = _make_precos_xlsx()
        df = parser.parse_precos(content, uf="MT")
        assert all(df["uf"] == "MT")

    def test_filtro_municipio(self):
        content = _make_precos_xlsx()
        df = parser.parse_precos(content, municipio="CUIABA")
        assert all("CUIABA" in m.upper() for m in df["municipio"])

    def test_margem_calculada(self):
        content = _make_precos_xlsx()
        df = parser.parse_precos(content)
        for _, row in df.iterrows():
            if pd.notna(row["preco_venda"]) and pd.notna(row["preco_compra"]):
                expected = row["preco_venda"] - row["preco_compra"]
                assert abs(row["margem"] - expected) < 0.01

    def test_n_postos_int(self):
        content = _make_precos_xlsx()
        df = parser.parse_precos(content)
        assert df["n_postos"].dtype.name in ("Int64", "int64")

    def test_ordenado_por_data(self):
        content = _make_precos_xlsx()
        df = parser.parse_precos(content)
        assert df["data"].is_monotonic_increasing

    def test_xlsx_vazio_raise(self):
        df_vazio = pd.DataFrame()
        buf = io.BytesIO()
        df_vazio.to_excel(buf, index=False, engine="openpyxl")
        with pytest.raises(ParseError, match="vazio"):
            parser.parse_precos(buf.getvalue())

    def test_sem_coluna_produto_raise(self):
        df = pd.DataFrame({"OUTRA": ["valor"]})
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        with pytest.raises(ParseError, match="PRODUTO"):
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

    def test_preco_com_virgula(self):
        rows = [
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "01/01/2024",
                "DATA FINAL": "07/01/2024",
                "PREÇO MÉDIO REVENDA": "6,45",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5,80",
                "NÚMERO DE POSTOS PESQUISADOS": "150",
            },
        ]
        content = _make_precos_xlsx(rows=rows)
        df = parser.parse_precos(content)
        assert df["preco_venda"].iloc[0] == pytest.approx(6.45)

    def test_data_invalida_ignorada(self):
        rows = [
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "invalido",
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
        ]
        content = _make_precos_xlsx(rows=rows)
        df = parser.parse_precos(content)
        assert len(df) == 1
        assert df["uf"].iloc[0] == "MT"


class TestParseVendas:
    def test_parse_csv_basico(self):
        content = _make_vendas_csv()
        df = parser.parse_vendas(content)
        assert not df.empty
        assert "data" in df.columns
        assert "volume_m3" in df.columns
        assert "uf" in df.columns

    def test_filtra_diesel_somente(self):
        content = _make_vendas_csv()
        df = parser.parse_vendas(content)
        assert all("DIESEL" in p.upper() for p in df["produto"])
        assert not any("GASOLINA" in p.upper() for p in df["produto"])

    def test_filtro_uf(self):
        content = _make_vendas_csv()
        df = parser.parse_vendas(content, uf="MT")
        assert all(df["uf"] == "MT")

    def test_ordenado_por_data(self):
        content = _make_vendas_csv()
        df = parser.parse_vendas(content)
        datas = df.groupby("uf")["data"].apply(lambda x: x.is_monotonic_increasing)
        assert datas.all()

    def test_csv_vazio_raise(self):
        content = b"ANO;MES;PRODUTO;VENDAS\n"
        with pytest.raises(ParseError, match="vazio"):
            parser.parse_vendas(content)

    def test_sem_colunas_obrigatorias_raise(self):
        content = b"OUTRA;COLUNA\nval1;val2\n"
        with pytest.raises(ParseError, match="obrigatorias"):
            parser.parse_vendas(content)

    def test_sem_diesel_raise(self):
        rows = [
            {
                "ANO": "2024",
                "MES": "JAN",
                "GRANDE REGIAO": "SUDESTE",
                "UNIDADE DA FEDERACAO": "SAO PAULO",
                "PRODUTO": "GASOLINA C",
                "VENDAS": "800000",
            },
        ]
        content = _make_vendas_csv(rows)
        with pytest.raises(ParseError, match="diesel"):
            parser.parse_vendas(content)

    def test_bytes_invalidos_raise(self):
        with pytest.raises(ParseError, match="(Erro ao ler|vazio)"):
            parser.parse_vendas(b"\x00\x01\x02\x03")

    def test_volume_com_virgula(self):
        rows = [
            {
                "ANO": "2024",
                "MES": "JAN",
                "GRANDE REGIAO": "REGIAO CENTRO-OESTE",
                "UNIDADE DA FEDERACAO": "MATO GROSSO",
                "PRODUTO": "OLEO DIESEL",
                "VENDAS": "500.000,50",
            },
        ]
        content = _make_vendas_csv(rows)
        df = parser.parse_vendas(content)
        assert len(df) == 1
        assert abs(df["volume_m3"].iloc[0] - 500000.50) < 0.01

    def test_mes_texto_resolve(self):
        rows = [
            {
                "ANO": "2024",
                "MES": "MAR",
                "GRANDE REGIAO": "SUDESTE",
                "UNIDADE DA FEDERACAO": "SAO PAULO",
                "PRODUTO": "OLEO DIESEL S-10",
                "VENDAS": "100000",
            },
        ]
        content = _make_vendas_csv(rows)
        df = parser.parse_vendas(content)
        assert df["data"].iloc[0].month == 3


class TestOleoDieselNormalization:
    """Testa que OLEO DIESEL e variantes sao normalizados para DIESEL."""

    def test_precos_oleo_diesel_normalizado(self):
        rows = [
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "ÓLEO DIESEL",
                "DATA INICIAL": "01/01/2024",
                "DATA FINAL": "07/01/2024",
                "PREÇO MÉDIO REVENDA": "5.95",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.30",
                "NÚMERO DE POSTOS PESQUISADOS": "120",
            },
            {
                "ESTADO - SIGLA": "MT",
                "MUNICÍPIO": "CUIABA",
                "PRODUTO": "ÓLEO DIESEL S10",
                "DATA INICIAL": "01/01/2024",
                "DATA FINAL": "07/01/2024",
                "PREÇO MÉDIO REVENDA": "6.20",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.60",
                "NÚMERO DE POSTOS PESQUISADOS": "80",
            },
        ]
        content = _make_precos_xlsx(rows=rows)
        df = parser.parse_precos(content)
        assert set(df["produto"].unique()) == {"DIESEL", "DIESEL S10"}

    def test_precos_filtro_diesel_pega_oleo_diesel(self):
        rows = [
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "ÓLEO DIESEL S10",
                "DATA INICIAL": "01/01/2024",
                "DATA FINAL": "07/01/2024",
                "PREÇO MÉDIO REVENDA": "6.45",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.80",
                "NÚMERO DE POSTOS PESQUISADOS": "150",
            },
        ]
        content = _make_precos_xlsx(rows=rows)
        df = parser.parse_precos(content, produto="DIESEL S10")
        assert len(df) == 1
        assert df["produto"].iloc[0] == "DIESEL S10"

    def test_vendas_oleo_diesel_normalizado(self):
        content = _make_vendas_csv()
        df = parser.parse_vendas(content)
        for p in df["produto"]:
            assert p in ("DIESEL", "DIESEL S-10"), f"Produto nao normalizado: {p}"


class TestEstadoNomeCompleto:
    """Testa que nome completo de estado e convertido para sigla."""

    def test_precos_estado_nome_completo(self):
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
        df = parser.parse_precos(content)
        assert set(df["uf"].unique()) == {"MT", "SP"}

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

    def test_vendas_estado_nome_completo(self):
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
        ]
        content = _make_vendas_csv(rows)
        df = parser.parse_vendas(content)
        assert set(df["uf"].unique()) == {"MT", "SP"}


class TestAgregarMensal:
    def test_agregacao_basica(self):
        content = _make_precos_xlsx()
        df = parser.parse_precos(content)
        result = parser.agregar_mensal(df)
        assert not result.empty
        assert "data" in result.columns
        assert "preco_venda" in result.columns
        assert len(result) <= len(df)

    def test_dataframe_vazio(self):
        df = pd.DataFrame()
        result = parser.agregar_mensal(df)
        assert result.empty

    def test_margem_recalculada(self):
        content = _make_precos_xlsx()
        df = parser.parse_precos(content)
        result = parser.agregar_mensal(df)
        for _, row in result.iterrows():
            if pd.notna(row.get("preco_venda")) and pd.notna(row.get("preco_compra")):
                expected = row["preco_venda"] - row["preco_compra"]
                assert abs(row["margem"] - expected) < 0.01


class TestHelpers:
    def test_resolve_mes_texto(self):
        assert parser._resolve_mes("JAN") == 1
        assert parser._resolve_mes("DEZ") == 12

    def test_resolve_mes_numerico(self):
        assert parser._resolve_mes("3") == 3

    def test_resolve_mes_invalido(self):
        assert parser._resolve_mes("XYZ") is None
        assert parser._resolve_mes("") is None

    def test_parse_numeric_br(self):
        assert parser._parse_numeric_br("3517,6") == 3517.6
        assert parser._parse_numeric_br("1.234,5") == 1234.5
        assert parser._parse_numeric_br("-") is None
        assert parser._parse_numeric_br("") is None

    def test_normalize_columns(self):
        df = pd.DataFrame({"  produto  ": [1], "Estado - Sigla": [2]})
        result = parser._normalize_columns(df)
        assert "PRODUTO" in result.columns
        assert "ESTADO - SIGLA" in result.columns

    def test_find_column_case_insensitive(self):
        df = pd.DataFrame({"Produto": [1], "Estado - Sigla": [2]})
        df = parser._normalize_columns(df)
        assert parser._find_column(df, ["PRODUTO"]) == "PRODUTO"
        assert parser._find_column(df, ["ESTADO - SIGLA"]) == "ESTADO - SIGLA"

    def test_find_column_nao_encontrada(self):
        df = pd.DataFrame({"A": [1]})
        assert parser._find_column(df, ["B", "C"]) is None
