"""Testes para o parser BCB/SICOR."""

import pytest

from agrobr.bcb.parser import (
    PARSER_VERSION,
    agregar_por_uf,
    parse_credito_rural,
)
from agrobr.exceptions import ParseError


def _sicor_record(
    safra="2023/2024",
    uf="MT",
    cd_uf="51",
    municipio="SORRISO",
    cd_municipio="5107248",
    produto="SOJA",
    valor=1000000.0,
    area=500.0,
    contratos=10,
):
    return {
        "Safra": safra,
        "AnoEmissao": 2023,
        "MesEmissao": 9,
        "cdUF": cd_uf,
        "UF": uf,
        "cdMunicipio": cd_municipio,
        "Municipio": municipio,
        "Produto": produto,
        "Valor": valor,
        "AreaFinanciada": area,
        "QtdContratos": contratos,
    }


class TestParseCreditorRural:
    def test_parse_basic(self):
        dados = [_sicor_record()]
        df = parse_credito_rural(dados)

        assert len(df) == 1
        assert "safra" in df.columns
        assert "uf" in df.columns
        assert "municipio" in df.columns
        assert "valor" in df.columns
        assert "area_financiada" in df.columns

    def test_parse_renames_columns(self):
        dados = [_sicor_record()]
        df = parse_credito_rural(dados)

        assert "Safra" not in df.columns
        assert "safra" in df.columns
        assert "Valor" not in df.columns
        assert "valor" in df.columns

    def test_parse_normalizes_produto(self):
        dados = [_sicor_record(produto="SOJA")]
        df = parse_credito_rural(dados)

        assert df.iloc[0]["produto"] == "soja"

    def test_parse_normalizes_uf(self):
        dados = [_sicor_record(uf="mt")]
        df = parse_credito_rural(dados)

        assert df.iloc[0]["uf"] == "MT"

    def test_parse_numeric_conversion(self):
        dados = [_sicor_record(valor=1500000.50, area=750.25)]
        df = parse_credito_rural(dados)

        assert df.iloc[0]["valor"] == pytest.approx(1500000.50)
        assert df.iloc[0]["area_financiada"] == pytest.approx(750.25)

    def test_parse_empty_raises(self):
        with pytest.raises(ParseError) as exc_info:
            parse_credito_rural([])
        assert "vazia" in str(exc_info.value).lower()
        assert exc_info.value.parser_version == PARSER_VERSION

    def test_parse_multiple_records(self):
        dados = [
            _sicor_record(municipio="SORRISO", valor=1000000),
            _sicor_record(municipio="SINOP", valor=800000),
            _sicor_record(municipio="LUCAS DO RIO VERDE", valor=1200000),
        ]
        df = parse_credito_rural(dados)

        assert len(df) == 3

    def test_parse_sorted(self):
        dados = [
            _sicor_record(uf="SP", municipio="CAMPINAS"),
            _sicor_record(uf="MT", municipio="SORRISO"),
            _sicor_record(uf="MT", municipio="CUIABA"),
        ]
        df = parse_credito_rural(dados)

        assert df.iloc[0]["uf"] == "MT"


class TestAgregarPorUf:
    def test_basic_aggregation(self):
        dados = [
            _sicor_record(municipio="SORRISO", valor=1000000, area=500, contratos=10),
            _sicor_record(municipio="SINOP", valor=800000, area=400, contratos=8),
        ]
        df = parse_credito_rural(dados)
        df_uf = agregar_por_uf(df)

        assert len(df_uf) == 1
        assert df_uf.iloc[0]["valor"] == pytest.approx(1800000)
        assert df_uf.iloc[0]["area_financiada"] == pytest.approx(900)
        assert df_uf.iloc[0]["qtd_contratos"] == 18

    def test_multi_uf(self):
        dados = [
            _sicor_record(uf="MT", cd_uf="51", valor=1000000),
            _sicor_record(uf="PR", cd_uf="41", valor=500000),
        ]
        df = parse_credito_rural(dados)
        df_uf = agregar_por_uf(df)

        assert len(df_uf) == 2

    def test_empty_df(self):
        import pandas as pd

        df = pd.DataFrame()
        result = agregar_por_uf(df)
        assert result.empty


class TestParserVersion:
    def test_version_is_int(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1
