"""Testes para agrobr.antaq.parser."""

from __future__ import annotations

import pandas as pd
import pytest

from agrobr.antaq.parser import (
    _read_txt,
    join_movimentacao,
    parse_atracacao,
    parse_carga,
    parse_mercadoria,
)

ATRACACAO_TXT = (
    "IDAtracacao;Porto Atracação;Complexo Portuário;Tipo da Autoridade Portuária;"
    "Data Atracação;Data Desatracação;Ano;Mes;"
    "Tipo de Navegação da Atracação;Terminal;Município;UF;SGUF;Região Geográfica\n"
    "1;Santos;Complexo Santos;Autoridade Portuária;01/01/2024;02/01/2024;"
    "2024;1;Longo Curso;Terminal 1;Santos;SP;SP;Sudeste\n"
    "2;Paranaguá;Complexo Paranaguá;Autoridade Portuária;15/01/2024;16/01/2024;"
    "2024;1;Cabotagem;Terminal 2;Paranaguá;PR;PR;Sul\n"
    "3;Salvador;Complexo Salvador;Autoridade Portuária;20/02/2024;21/02/2024;"
    "2024;2;Interior;Terminal 3;Salvador;BA;BA;Nordeste\n"
)

CARGA_TXT = (
    "IDCarga;IDAtracacao;Origem;Destino;CDMercadoria;Tipo Operação da Carga;"
    "Tipo Navegação;Natureza da Carga;Sentido;TEU;QTCarga;VLPesoCargaBruta\n"
    "1;1;Brasil;China;1201;Exportação;Longo Curso;Granel Sólido;Embarcados;0;1;"
    '"34.452,28"\n'
    "2;1;Brasil;EUA;1507;Exportação;Longo Curso;Granel Líquido e Gasoso;Embarcados;0;1;"
    '"12.100,50"\n'
    "3;2;Bahia;São Paulo;2304;Cabotagem;Cabotagem;Granel Sólido;Desembarcados;0;1;"
    '"5.000,00"\n'
    "4;3;São Paulo;Bahia;1005;Interior;Interior;Carga Geral;Embarcados;2;1;"
    '"800,75"\n'
)

MERCADORIA_TXT = (
    "CDMercadoria;Grupo de Mercadoria;Mercadoria;Nomenclatura Simplificada Mercadoria\n"
    "1201;Grãos;Soja em grãos;SOJA EM GRAOS\n"
    "1507;Óleos;Óleo de soja;OLEO DE SOJA\n"
    "2304;Farelos;Farelo de soja;FARELO DE SOJA\n"
    "1005;Grãos;Milho;MILHO\n"
)


class TestReadTxt:
    def test_basic_read(self):
        content = "colA;colB;colC\n1;2;3\n4;5;6\n"

        df = _read_txt(content)

        assert len(df) == 2
        assert list(df.columns) == ["colA", "colB", "colC"]

    def test_usecols_filter(self):
        content = "colA;colB;colC\n1;2;3\n4;5;6\n"

        df = _read_txt(content, usecols=["colA", "colC"])

        assert list(df.columns) == ["colA", "colC"]
        assert len(df) == 2

    def test_strips_column_names(self):
        content = " colA ; colB \n1;2\n"

        df = _read_txt(content)

        assert "colA" in df.columns
        assert "colB" in df.columns

    def test_all_str_dtype(self):
        content = "num;dec\n123;45.6\n"

        df = _read_txt(content)

        assert df["num"].dtype == object
        assert df["dec"].dtype == object

    def test_empty_content(self):
        content = "colA;colB\n"

        df = _read_txt(content)

        assert len(df) == 0
        assert list(df.columns) == ["colA", "colB"]


class TestParseAtracacao:
    def test_parse_basic(self):
        df = parse_atracacao(ATRACACAO_TXT)

        assert len(df) == 3
        assert "IDAtracacao" in df.columns
        assert "Porto Atracação" in df.columns
        assert "SGUF" in df.columns

    def test_selects_known_columns(self):
        df = parse_atracacao(ATRACACAO_TXT)

        for col in ["IDAtracacao", "Porto Atracação", "Ano", "Mes", "SGUF"]:
            assert col in df.columns

    def test_handles_missing_columns(self):
        minimal = "IDAtracacao;Porto Atracação;Ano;Mes\n1;Santos;2024;1\n"

        df = parse_atracacao(minimal)

        assert len(df) == 1
        assert "IDAtracacao" in df.columns


class TestParseCarga:
    def test_parse_basic(self):
        df = parse_carga(CARGA_TXT)

        assert len(df) == 4
        assert "IDCarga" in df.columns
        assert "IDAtracacao" in df.columns

    def test_peso_bruto_conversion(self):
        df = parse_carga(CARGA_TXT)

        assert pd.api.types.is_numeric_dtype(df["VLPesoCargaBruta"])
        assert df.iloc[0]["VLPesoCargaBruta"] == pytest.approx(34452.28, rel=1e-4)
        assert df.iloc[1]["VLPesoCargaBruta"] == pytest.approx(12100.50, rel=1e-4)
        assert df.iloc[3]["VLPesoCargaBruta"] == pytest.approx(800.75, rel=1e-4)

    def test_qtcarga_conversion(self):
        df = parse_carga(CARGA_TXT)

        assert pd.api.types.is_numeric_dtype(df["QTCarga"])

    def test_teu_conversion(self):
        df = parse_carga(CARGA_TXT)

        assert df["TEU"].dtype in (int, "int64", "int32")
        assert df.iloc[3]["TEU"] == 2

    def test_handles_missing_peso_column(self):
        txt = "IDCarga;IDAtracacao;Sentido\n1;1;Embarcados\n"

        df = parse_carga(txt)

        assert len(df) == 1
        assert "VLPesoCargaBruta" not in df.columns


class TestParseMercadoria:
    def test_parse_basic(self):
        df = parse_mercadoria(MERCADORIA_TXT)

        assert len(df) == 4
        assert "CDMercadoria" in df.columns
        assert "Nomenclatura Simplificada Mercadoria" in df.columns

    def test_values(self):
        df = parse_mercadoria(MERCADORIA_TXT)

        soja = df[df["CDMercadoria"] == "1201"]
        assert len(soja) == 1
        assert soja.iloc[0]["Nomenclatura Simplificada Mercadoria"] == "SOJA EM GRAOS"


class TestJoinMovimentacao:
    def _parse_all(self):
        df_a = parse_atracacao(ATRACACAO_TXT)
        df_c = parse_carga(CARGA_TXT)
        df_m = parse_mercadoria(MERCADORIA_TXT)
        return df_a, df_c, df_m

    def test_join_produces_result(self):
        df_a, df_c, df_m = self._parse_all()

        df = join_movimentacao(df_a, df_c, df_m)

        assert len(df) == 4
        assert isinstance(df, pd.DataFrame)

    def test_columns_renamed(self):
        df_a, df_c, df_m = self._parse_all()

        df = join_movimentacao(df_a, df_c, df_m)

        assert "porto" in df.columns
        assert "uf" in df.columns
        assert "peso_bruto_ton" in df.columns
        assert "mercadoria" in df.columns

    def test_original_columns_absent(self):
        df_a, df_c, df_m = self._parse_all()

        df = join_movimentacao(df_a, df_c, df_m)

        assert "IDAtracacao" not in df.columns
        assert "Porto Atracação" not in df.columns
        assert "VLPesoCargaBruta" not in df.columns

    def test_ano_mes_numeric(self):
        df_a, df_c, df_m = self._parse_all()

        df = join_movimentacao(df_a, df_c, df_m)

        assert pd.api.types.is_integer_dtype(df["ano"])
        assert pd.api.types.is_integer_dtype(df["mes"])

    def test_sorted_output(self):
        df_a, df_c, df_m = self._parse_all()

        df = join_movimentacao(df_a, df_c, df_m)

        anos = df["ano"].tolist()
        assert anos == sorted(anos)

    def test_mercadoria_lookup_works(self):
        df_a, df_c, df_m = self._parse_all()

        df = join_movimentacao(df_a, df_c, df_m)

        soja_rows = df[df["cd_mercadoria"] == "1201"]
        assert len(soja_rows) == 1
        assert soja_rows.iloc[0]["mercadoria"] == "SOJA EM GRAOS"

    def test_porto_from_atracacao(self):
        df_a, df_c, df_m = self._parse_all()

        df = join_movimentacao(df_a, df_c, df_m)

        assert "Santos" in df["porto"].values

    def test_final_column_order(self):
        df_a, df_c, df_m = self._parse_all()

        df = join_movimentacao(df_a, df_c, df_m)

        expected_prefix = ["ano", "mes", "data_atracacao", "tipo_navegacao"]
        assert list(df.columns[:4]) == expected_prefix

    def test_empty_inputs(self):
        df_a = parse_atracacao(
            "IDAtracacao;Porto Atracação;Complexo Portuário;Terminal;"
            "Município;SGUF;Região Geográfica;Ano;Mes;Data Atracação\n"
        )
        df_c = parse_carga(
            "IDCarga;IDAtracacao;Sentido;VLPesoCargaBruta;Tipo Navegação;"
            "Natureza da Carga;CDMercadoria;Tipo Operação da Carga;"
            "Origem;Destino;QTCarga;TEU\n"
        )
        df_m = parse_mercadoria(
            "CDMercadoria;Grupo de Mercadoria;Nomenclatura Simplificada Mercadoria\n"
        )

        df = join_movimentacao(df_a, df_c, df_m)

        assert len(df) == 0
