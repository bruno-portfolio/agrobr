"""Testes para agrobr.alt.mapa_psr.models."""

from __future__ import annotations

import pytest

from agrobr.alt.mapa_psr.models import (
    ANO_INICIO_PSR,
    CLASSIFICACOES_VALIDAS,
    COLUNAS_APOLICES,
    COLUNAS_CSV,
    COLUNAS_DROP,
    COLUNAS_FLOAT,
    COLUNAS_PII,
    COLUNAS_SINISTROS,
    CSV_RESOURCES,
    PERIODOS_VALIDOS,
    UFS_VALIDAS,
    ApoliceSeguroRural,
    _resolve_periodos,
    get_csv_url,
)


class TestConstants:
    def test_csv_resources_tem_tres_periodos(self):
        assert len(CSV_RESOURCES) == 3
        assert "2006-2015" in CSV_RESOURCES
        assert "2016-2024" in CSV_RESOURCES
        assert "2025" in CSV_RESOURCES

    def test_csv_resources_tem_resource_id_e_filename(self):
        for periodo, info in CSV_RESOURCES.items():
            assert "resource_id" in info, f"{periodo} sem resource_id"
            assert "filename" in info, f"{periodo} sem filename"
            assert info["filename"].endswith(".csv"), f"{periodo} filename nao eh CSV"

    def test_periodos_validos_corresponde_csv_resources(self):
        assert frozenset(CSV_RESOURCES.keys()) == PERIODOS_VALIDOS

    def test_ufs_validas_tem_27(self):
        assert len(UFS_VALIDAS) == 27

    def test_ufs_validas_contem_principais(self):
        for uf in ("SP", "MT", "PR", "GO", "RS", "MG", "MS", "BA"):
            assert uf in UFS_VALIDAS

    def test_colunas_pii_contem_campos_sensiveis(self):
        assert "NM_SEGURADO" in COLUNAS_PII
        assert "NR_DOCUMENTO_SEGURADO" in COLUNAS_PII

    def test_colunas_drop_inclui_pii_e_geo(self):
        assert COLUNAS_PII.issubset(COLUNAS_DROP)
        assert "LATITUDE" in COLUNAS_DROP
        assert "LONGITUDE" in COLUNAS_DROP

    def test_colunas_csv_mapeamento_completo(self):
        assert len(COLUNAS_CSV) >= 18
        assert COLUNAS_CSV["ANO_APOLICE"] == "ano_apolice"
        assert COLUNAS_CSV["SG_UF_PROPRIEDADE"] == "uf"
        assert COLUNAS_CSV["NM_CULTURA_GLOBAL"] == "cultura"
        assert COLUNAS_CSV["VALOR_INDENIZACAO"] == "valor_indenizacao"
        assert COLUNAS_CSV["EVENTO_PREPONDERANTE"] == "evento"

    def test_colunas_float_contem_monetarias(self):
        for col in (
            "valor_premio",
            "valor_subvencao",
            "valor_indenizacao",
            "valor_limite_garantia",
        ):
            assert col in COLUNAS_FLOAT

    def test_colunas_sinistros_tem_evento(self):
        assert "evento" in COLUNAS_SINISTROS
        assert "valor_indenizacao" in COLUNAS_SINISTROS

    def test_colunas_apolices_tem_taxa(self):
        assert "taxa" in COLUNAS_APOLICES
        assert "nr_apolice" in COLUNAS_APOLICES

    def test_classificacoes_validas(self):
        assert "AGRICOLA" in CLASSIFICACOES_VALIDAS
        assert "PECUARIO" in CLASSIFICACOES_VALIDAS

    def test_ano_inicio_psr(self):
        assert ANO_INICIO_PSR == 2006


class TestResolvePeriodos:
    def test_sem_filtro_retorna_todos(self):
        result = _resolve_periodos()
        assert len(result) == 3

    def test_ano_2010_retorna_primeiro_periodo(self):
        result = _resolve_periodos(ano_inicio=2010, ano_fim=2010)
        assert result == ["2006-2015"]

    def test_ano_2020_retorna_segundo_periodo(self):
        result = _resolve_periodos(ano_inicio=2020, ano_fim=2020)
        assert result == ["2016-2024"]

    def test_ano_2025_retorna_terceiro_periodo(self):
        result = _resolve_periodos(ano_inicio=2025, ano_fim=2025)
        assert result == ["2025"]

    def test_range_amplo_retorna_multiplos(self):
        result = _resolve_periodos(ano_inicio=2010, ano_fim=2025)
        assert len(result) == 3

    def test_range_2015_2016_retorna_dois_periodos(self):
        result = _resolve_periodos(ano_inicio=2015, ano_fim=2016)
        assert "2006-2015" in result
        assert "2016-2024" in result

    def test_apenas_ano_inicio(self):
        result = _resolve_periodos(ano_inicio=2020)
        assert "2016-2024" in result
        assert "2025" in result
        assert "2006-2015" not in result

    def test_apenas_ano_fim(self):
        result = _resolve_periodos(ano_fim=2015)
        assert "2006-2015" in result
        assert "2016-2024" not in result or "2025" not in result

    def test_ano_futuro_retorna_vazio(self):
        result = _resolve_periodos(ano_inicio=2030, ano_fim=2030)
        assert result == []


class TestGetCsvUrl:
    def test_url_valida(self):
        url = get_csv_url("2025")
        assert "dados.agricultura.gov.br" in url
        assert "ac7e4351" in url
        assert ".csv" in url

    def test_periodo_invalido_raise(self):
        with pytest.raises(ValueError, match="invalido"):
            get_csv_url("2030")


class TestApoliceSeguroRural:
    def test_basico(self):
        apolice = ApoliceSeguroRural(
            nr_apolice="12345",
            ano_apolice=2023,
            uf="MT",
            cultura="SOJA",
        )
        assert apolice.ano_apolice == 2023
        assert apolice.uf == "MT"

    def test_convert_numeric_vazio(self):
        apolice = ApoliceSeguroRural(
            ano_apolice=2023,
            valor_premio="",
            valor_indenizacao="-",
        )
        assert apolice.valor_premio is None
        assert apolice.valor_indenizacao is None

    def test_convert_numeric_valido(self):
        apolice = ApoliceSeguroRural(
            ano_apolice=2023,
            valor_premio="1234.56",
        )
        assert apolice.valor_premio == 1234.56

    def test_convert_ano_float(self):
        apolice = ApoliceSeguroRural(ano_apolice="2023.0")
        assert apolice.ano_apolice == 2023

    def test_convert_ano_vazio_raise(self):
        with pytest.raises(ValueError):
            ApoliceSeguroRural(ano_apolice="")
