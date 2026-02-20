"""Testes para agrobr.alt.antt_pedagio.models."""

from __future__ import annotations

from datetime import date

from agrobr.alt.antt_pedagio.models import (
    ANO_FIM_V1,
    ANO_INICIO,
    ANO_INICIO_V2,
    CATEGORIA_MAP,
    CKAN_BASE,
    COLUNAS_FLUXO,
    COLUNAS_V1,
    COLUNAS_V2,
    EIXOS_TIPO_MAP,
    UFS_VALIDAS,
    _resolve_anos,
    build_ckan_package_url,
    build_ckan_resource_url,
    schema_version,
)


class TestConstants:
    def test_ano_inicio(self):
        assert ANO_INICIO == 2010

    def test_ano_v1_v2_boundary(self):
        assert ANO_FIM_V1 == 2023
        assert ANO_INICIO_V2 == 2024

    def test_ckan_base(self):
        assert "dados.antt.gov.br" in CKAN_BASE

    def test_ufs_validas_count(self):
        assert len(UFS_VALIDAS) == 27


class TestCategoriaMap:
    def test_9_categorias(self):
        assert len(CATEGORIA_MAP) == 9

    def test_categoria_1_passeio(self):
        n, tipo = CATEGORIA_MAP["Categoria 1"]
        assert n == 2
        assert tipo == "Passeio"

    def test_categoria_4_comercial(self):
        n, tipo = CATEGORIA_MAP["Categoria 4"]
        assert n == 3
        assert tipo == "Comercial"

    def test_categoria_8_6eixos(self):
        n, tipo = CATEGORIA_MAP["Categoria 8"]
        assert n == 6
        assert tipo == "Comercial"

    def test_categoria_9_moto(self):
        n, tipo = CATEGORIA_MAP["Categoria 9"]
        assert n == 2
        assert tipo == "Moto"

    def test_all_n_eixos_positive(self):
        for cat, (n, _) in CATEGORIA_MAP.items():
            assert n >= 2, f"{cat} has n_eixos < 2"

    def test_all_tipos_valid(self):
        valid = {"Passeio", "Comercial", "Moto"}
        for cat, (_, tipo) in CATEGORIA_MAP.items():
            assert tipo in valid, f"{cat} has invalid tipo: {tipo}"


class TestEixosTipoMap:
    def test_2eixos_passeio(self):
        assert EIXOS_TIPO_MAP[2] == "Passeio"

    def test_3plus_comercial(self):
        for n in range(3, 19):
            if n in EIXOS_TIPO_MAP:
                assert EIXOS_TIPO_MAP[n] == "Comercial"

    def test_covers_range(self):
        for n in range(2, 19):
            assert n in EIXOS_TIPO_MAP, f"Missing eixo: {n}"


class TestSchemaVersion:
    def test_v1_for_2010(self):
        assert schema_version(2010) == 1

    def test_v1_for_2023(self):
        assert schema_version(2023) == 1

    def test_v2_for_2024(self):
        assert schema_version(2024) == 2

    def test_v2_for_2025(self):
        assert schema_version(2025) == 2


class TestResolveAnos:
    def test_single_ano(self):
        assert _resolve_anos(ano=2023) == [2023]

    def test_range(self):
        result = _resolve_anos(ano_inicio=2020, ano_fim=2023)
        assert result == [2020, 2021, 2022, 2023]

    def test_range_inicio_only(self):
        result = _resolve_anos(ano_inicio=2024)
        assert 2024 in result
        assert result[0] == 2024

    def test_range_fim_only(self):
        result = _resolve_anos(ano_fim=2012)
        assert result[0] == ANO_INICIO
        assert result[-1] == 2012

    def test_default_2_anos(self):
        result = _resolve_anos()
        assert len(result) == 2
        current = date.today().year
        assert result == [current - 1, current]


class TestUrlBuilders:
    def test_ckan_package_url(self):
        url = build_ckan_package_url("test-slug")
        assert "package_show" in url
        assert "test-slug" in url

    def test_ckan_resource_url(self):
        url = build_ckan_resource_url("abc-123")
        assert "abc-123" in url
        assert CKAN_BASE in url


class TestColunas:
    def test_colunas_fluxo_has_data(self):
        assert "data" in COLUNAS_FLUXO

    def test_colunas_fluxo_has_volume(self):
        assert "volume" in COLUNAS_FLUXO

    def test_colunas_fluxo_has_enrichment(self):
        assert "rodovia" in COLUNAS_FLUXO
        assert "uf" in COLUNAS_FLUXO
        assert "municipio" in COLUNAS_FLUXO

    def test_colunas_v1_has_categoria(self):
        assert "categoria" in COLUNAS_V1

    def test_colunas_v2_has_categoria_eixo(self):
        assert "categoria_eixo" in COLUNAS_V2
