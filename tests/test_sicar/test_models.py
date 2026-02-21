"""Testes para agrobr.alt.sicar.models."""

from __future__ import annotations

import pytest

from agrobr.alt.sicar.models import (
    COLUNAS_IMOVEIS,
    MAX_FEATURES_WARNING,
    PAGE_SIZE,
    PROPERTY_NAMES,
    RENAME_MAP,
    STATUS_LABELS,
    STATUS_VALIDOS,
    TIPO_LABELS,
    TIPO_VALIDOS,
    UFS_VALIDAS,
    WFS_BASE,
    WFS_VERSION,
    layer_name,
)


class TestConstants:
    def test_wfs_base_url(self):
        assert "geoserver.car.gov.br" in WFS_BASE
        assert WFS_BASE.startswith("https://")

    def test_wfs_version(self):
        assert WFS_VERSION == "2.0.0"

    def test_page_size(self):
        assert PAGE_SIZE == 10_000

    def test_max_features_warning(self):
        assert MAX_FEATURES_WARNING == 100_000


class TestLayerName:
    def test_lowercase(self):
        assert layer_name("DF") == "sicar_imoveis_df"

    def test_already_lowercase(self):
        assert layer_name("mt") == "sicar_imoveis_mt"

    def test_mixed_case(self):
        assert layer_name("Ba") == "sicar_imoveis_ba"

    @pytest.mark.parametrize("uf", ["AC", "MT", "SP", "RS", "PA", "TO"])
    def test_all_return_valid_format(self, uf: str):
        result = layer_name(uf)
        assert result.startswith("sicar_imoveis_")
        assert result == f"sicar_imoveis_{uf.lower()}"


class TestPropertyNames:
    def test_has_all_expected_fields(self):
        expected = {
            "cod_imovel",
            "status_imovel",
            "dat_criacao",
            "data_atualizacao",
            "area",
            "condicao",
            "uf",
            "municipio",
            "cod_municipio_ibge",
            "m_fiscal",
            "tipo_imovel",
        }
        assert set(PROPERTY_NAMES) == expected

    def test_no_geometry_field(self):
        assert "geo_area_imovel" not in PROPERTY_NAMES
        assert "the_geom" not in PROPERTY_NAMES

    def test_count(self):
        assert len(PROPERTY_NAMES) == 11


class TestRenameMap:
    def test_status_rename(self):
        assert RENAME_MAP["status_imovel"] == "status"

    def test_date_rename(self):
        assert RENAME_MAP["dat_criacao"] == "data_criacao"

    def test_area_rename(self):
        assert RENAME_MAP["area"] == "area_ha"

    def test_m_fiscal_rename(self):
        assert RENAME_MAP["m_fiscal"] == "modulos_fiscais"

    def test_tipo_rename(self):
        assert RENAME_MAP["tipo_imovel"] == "tipo"


class TestColunasImoveis:
    def test_count(self):
        assert len(COLUNAS_IMOVEIS) == 11

    def test_first_is_cod_imovel(self):
        assert COLUNAS_IMOVEIS[0] == "cod_imovel"

    def test_has_all_output_columns(self):
        expected = {
            "cod_imovel",
            "status",
            "data_criacao",
            "data_atualizacao",
            "area_ha",
            "condicao",
            "uf",
            "municipio",
            "cod_municipio_ibge",
            "modulos_fiscais",
            "tipo",
        }
        assert set(COLUNAS_IMOVEIS) == expected


class TestStatusValidos:
    def test_all_status(self):
        assert {"AT", "PE", "SU", "CA"} == STATUS_VALIDOS

    def test_is_frozenset(self):
        assert isinstance(STATUS_VALIDOS, frozenset)

    def test_labels_match_status(self):
        assert set(STATUS_LABELS.keys()) == STATUS_VALIDOS

    def test_labels_values(self):
        assert STATUS_LABELS["AT"] == "Ativo"
        assert STATUS_LABELS["PE"] == "Pendente"
        assert STATUS_LABELS["SU"] == "Suspenso"
        assert STATUS_LABELS["CA"] == "Cancelado"


class TestTipoValidos:
    def test_all_tipos(self):
        assert {"IRU", "AST", "PCT"} == TIPO_VALIDOS

    def test_is_frozenset(self):
        assert isinstance(TIPO_VALIDOS, frozenset)

    def test_labels_match_tipos(self):
        assert set(TIPO_LABELS.keys()) == TIPO_VALIDOS

    def test_labels_values(self):
        assert TIPO_LABELS["IRU"] == "Rural"
        assert TIPO_LABELS["AST"] == "Assentamento"
        assert TIPO_LABELS["PCT"] == "Terra Indigena"


class TestUfsValidas:
    def test_count_27(self):
        assert len(UFS_VALIDAS) == 27

    def test_is_frozenset(self):
        assert isinstance(UFS_VALIDAS, frozenset)

    @pytest.mark.parametrize("uf", ["AC", "DF", "MT", "SP", "RS", "PA", "TO", "BA"])
    def test_major_states(self, uf: str):
        assert uf in UFS_VALIDAS

    def test_no_invalid_uf(self):
        assert "XX" not in UFS_VALIDAS
        assert "BR" not in UFS_VALIDAS
