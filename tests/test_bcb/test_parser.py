import pandas as pd
import pytest

from agrobr.bcb.parser import (
    PARSER_VERSION,
    agregar_por_programa,
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
    cd_programa=None,
    cd_sub_programa=None,
    cd_fonte_recurso=None,
    cd_tipo_seguro=None,
    cd_modalidade=None,
    atividade=None,
):
    rec: dict = {
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
    if cd_programa is not None:
        rec["cdPrograma"] = cd_programa
    if cd_sub_programa is not None:
        rec["cdSubPrograma"] = cd_sub_programa
    if cd_fonte_recurso is not None:
        rec["cdFonteRecurso"] = cd_fonte_recurso
    if cd_tipo_seguro is not None:
        rec["cdTipoSeguro"] = cd_tipo_seguro
    if cd_modalidade is not None:
        rec["cdModalidade"] = cd_modalidade
    if atividade is not None:
        rec["Atividade"] = atividade
    return rec


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


class TestParseNewApiColumns:
    def test_renames_cd_programa(self):
        dados = [_sicor_record(cd_programa="0050")]
        df = parse_credito_rural(dados)

        assert "cdPrograma" not in df.columns
        assert "cd_programa" in df.columns
        assert df.iloc[0]["cd_programa"] == "0050"

    def test_renames_cd_fonte_recurso(self):
        dados = [_sicor_record(cd_fonte_recurso="0430")]
        df = parse_credito_rural(dados)

        assert "cdFonteRecurso" not in df.columns
        assert "cd_fonte_recurso" in df.columns
        assert df.iloc[0]["cd_fonte_recurso"] == "0430"

    def test_renames_cd_tipo_seguro(self):
        dados = [_sicor_record(cd_tipo_seguro="3")]
        df = parse_credito_rural(dados)

        assert "cdTipoSeguro" not in df.columns
        assert "cd_tipo_seguro" in df.columns

    def test_renames_cd_modalidade(self):
        dados = [_sicor_record(cd_modalidade="01")]
        df = parse_credito_rural(dados)

        assert "cdModalidade" not in df.columns
        assert "cd_modalidade" in df.columns

    def test_renames_atividade_to_cd_atividade(self):
        dados = [_sicor_record(atividade="1")]
        df = parse_credito_rural(dados)

        assert "Atividade" not in df.columns
        assert "cd_atividade" in df.columns
        assert df.iloc[0]["cd_atividade"] == "1"

    def test_renames_cd_sub_programa(self):
        dados = [_sicor_record(cd_sub_programa="0001")]
        df = parse_credito_rural(dados)

        assert "cdSubPrograma" not in df.columns
        assert "cd_sub_programa" in df.columns

    def test_all_dimension_columns_together(self):
        dados = [
            _sicor_record(
                cd_programa="0050",
                cd_sub_programa="0001",
                cd_fonte_recurso="0303",
                cd_tipo_seguro="9",
                cd_modalidade="01",
                atividade="1",
            )
        ]
        df = parse_credito_rural(dados)

        assert df.iloc[0]["cd_programa"] == "0050"
        assert df.iloc[0]["cd_sub_programa"] == "0001"
        assert df.iloc[0]["cd_fonte_recurso"] == "0303"
        assert df.iloc[0]["cd_tipo_seguro"] == "9"
        assert df.iloc[0]["cd_modalidade"] == "01"
        assert df.iloc[0]["cd_atividade"] == "1"


class TestEnriquecimento:
    def test_programa_resolved(self):
        dados = [_sicor_record(cd_programa="0050")]
        df = parse_credito_rural(dados)

        assert "programa" in df.columns
        assert df.iloc[0]["programa"] == "Pronamp"

    def test_fonte_recurso_resolved(self):
        dados = [_sicor_record(cd_fonte_recurso="0430")]
        df = parse_credito_rural(dados)

        assert "fonte_recurso" in df.columns
        assert df.iloc[0]["fonte_recurso"] == "LCA"

    def test_tipo_seguro_resolved(self):
        dados = [_sicor_record(cd_tipo_seguro="1")]
        df = parse_credito_rural(dados)

        assert "tipo_seguro" in df.columns
        assert df.iloc[0]["tipo_seguro"] == "Proagro"

    def test_modalidade_resolved(self):
        dados = [_sicor_record(cd_modalidade="01")]
        df = parse_credito_rural(dados)

        assert "modalidade" in df.columns
        assert df.iloc[0]["modalidade"] == "Individual"

    def test_atividade_resolved(self):
        dados = [_sicor_record(atividade="1")]
        df = parse_credito_rural(dados)

        assert "atividade" in df.columns
        assert df.iloc[0]["atividade"] == "Agricola"

    def test_all_dimensions_resolved(self):
        dados = [
            _sicor_record(
                cd_programa="0001",
                cd_fonte_recurso="0502",
                cd_tipo_seguro="3",
                cd_modalidade="03",
                atividade="2",
            )
        ]
        df = parse_credito_rural(dados)

        assert df.iloc[0]["programa"] == "Pronaf"
        assert df.iloc[0]["fonte_recurso"] == "FNE"
        assert df.iloc[0]["tipo_seguro"] == "Seguro privado"
        assert df.iloc[0]["modalidade"] == "Coletiva"
        assert df.iloc[0]["atividade"] == "Pecuaria"


class TestEnriquecimentoDesconhecido:
    def test_programa_desconhecido(self):
        dados = [_sicor_record(cd_programa="9999")]
        df = parse_credito_rural(dados)

        assert df.iloc[0]["programa"] == "Desconhecido (9999)"

    def test_fonte_recurso_desconhecida(self):
        dados = [_sicor_record(cd_fonte_recurso="0000")]
        df = parse_credito_rural(dados)

        assert df.iloc[0]["fonte_recurso"] == "Desconhecido (0000)"

    def test_tipo_seguro_desconhecido(self):
        dados = [_sicor_record(cd_tipo_seguro="7")]
        df = parse_credito_rural(dados)

        assert df.iloc[0]["tipo_seguro"] == "Desconhecido (7)"

    def test_modalidade_desconhecida(self):
        dados = [_sicor_record(cd_modalidade="99")]
        df = parse_credito_rural(dados)

        assert df.iloc[0]["modalidade"] == "Desconhecido (99)"

    def test_atividade_desconhecida(self):
        dados = [_sicor_record(atividade="5")]
        df = parse_credito_rural(dados)

        assert df.iloc[0]["atividade"] == "Desconhecido (5)"


class TestEnriquecimentoSemColunas:
    def test_sem_dimensoes_nao_quebra(self):
        dados = [_sicor_record()]
        df = parse_credito_rural(dados)

        assert "cd_programa" not in df.columns
        assert "programa" not in df.columns
        assert len(df) == 1


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
        df = pd.DataFrame()
        result = agregar_por_uf(df)
        assert result.empty


class TestAgregarPorPrograma:
    def test_basic_aggregation(self):
        dados = [
            _sicor_record(
                municipio="SORRISO",
                valor=1000000,
                area=500,
                contratos=10,
                cd_programa="0050",
            ),
            _sicor_record(
                municipio="SINOP",
                valor=800000,
                area=400,
                contratos=8,
                cd_programa="0050",
            ),
        ]
        df = parse_credito_rural(dados)
        df_prog = agregar_por_programa(df)

        assert len(df_prog) == 1
        assert df_prog.iloc[0]["valor"] == pytest.approx(1800000)
        assert df_prog.iloc[0]["area_financiada"] == pytest.approx(900)
        assert df_prog.iloc[0]["qtd_contratos"] == 18

    def test_multi_programa(self):
        dados = [
            _sicor_record(cd_programa="0050", valor=1000000),
            _sicor_record(cd_programa="0001", valor=500000),
        ]
        df = parse_credito_rural(dados)
        df_prog = agregar_por_programa(df)

        assert len(df_prog) == 2

    def test_empty_df(self):
        df = pd.DataFrame()
        result = agregar_por_programa(df)
        assert result.empty

    def test_keeps_programa_name(self):
        dados = [
            _sicor_record(cd_programa="0050", valor=1000000),
            _sicor_record(cd_programa="0050", valor=500000),
        ]
        df = parse_credito_rural(dados)
        df_prog = agregar_por_programa(df)

        assert "programa" in df_prog.columns
        assert df_prog.iloc[0]["programa"] == "Pronamp"


class TestParserVersion:
    def test_version_is_int(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 2
