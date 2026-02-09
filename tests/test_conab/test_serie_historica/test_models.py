"""Testes para os modelos Pydantic de serie historica CONAB."""

import pytest

from agrobr.conab.serie_historica.models import (
    REGIOES_BRASIL,
    SERIE_HISTORICA_PRODUTOS,
    UFS_BRASIL,
    SafraHistorica,
    detect_safra_format,
    normalize_produto,
)


class TestSafraHistorica:
    def test_basic_creation(self):
        rec = SafraHistorica(
            produto="Soja",
            safra="2023/24",
            uf="mt",
            regiao="CENTRO-OESTE",
            area_plantada_mil_ha=1200.5,
            producao_mil_ton=3600.0,
            produtividade_kg_ha=3000.0,
        )

        assert rec.produto == "soja"
        assert rec.uf == "MT"
        assert rec.regiao == "CENTRO-OESTE"
        assert rec.area_plantada_mil_ha == 1200.5

    def test_normalization(self):
        rec = SafraHistorica(
            produto="  SOJA  ",
            safra="2023/24",
            uf="mt",
        )

        assert rec.produto == "soja"
        assert rec.uf == "MT"

    def test_optional_fields(self):
        rec = SafraHistorica(
            produto="soja",
            safra="2023/24",
        )

        assert rec.uf is None
        assert rec.regiao is None
        assert rec.area_plantada_mil_ha is None
        assert rec.producao_mil_ton is None
        assert rec.produtividade_kg_ha is None

    def test_uf_none_stays_none(self):
        rec = SafraHistorica(
            produto="soja",
            safra="2023/24",
            uf=None,
        )
        assert rec.uf is None

    def test_regiao_none_stays_none(self):
        rec = SafraHistorica(
            produto="soja",
            safra="2023/24",
            regiao=None,
        )
        assert rec.regiao is None

    def test_negative_area_raises(self):
        with pytest.raises(ValueError):
            SafraHistorica(
                produto="soja",
                safra="2023/24",
                area_plantada_mil_ha=-100.0,
            )

    def test_model_dump(self):
        rec = SafraHistorica(
            produto="soja",
            safra="2023/24",
            uf="MT",
            area_plantada_mil_ha=1200.5,
            producao_mil_ton=3600.0,
        )

        d = rec.model_dump()
        assert d["produto"] == "soja"
        assert d["safra"] == "2023/24"
        assert d["uf"] == "MT"
        assert d["area_plantada_mil_ha"] == 1200.5
        assert d["produtividade_kg_ha"] is None


class TestNormalizeProduto:
    def test_known_products(self):
        assert normalize_produto("Soja") == "soja"
        assert normalize_produto("MILHO") == "milho"
        assert normalize_produto("Milho 1ª Safra") == "milho_1"
        assert normalize_produto("Arroz Total") == "arroz"
        assert normalize_produto("Café Arábica") == "cafe_arabica"
        assert normalize_produto("Cana-de-Açúcar") == "cana"

    def test_unknown_product(self):
        assert normalize_produto("Quinoa") == "quinoa"
        assert normalize_produto("Grão de Bico") == "grão_de_bico"

    def test_map_completeness(self):
        assert len(SERIE_HISTORICA_PRODUTOS) >= 20


class TestDetectSafraFormat:
    def test_short_format(self):
        assert detect_safra_format("2023/24") == "2023/24"

    def test_long_format(self):
        assert detect_safra_format("2023/2024") == "2023/24"

    def test_year_only(self):
        assert detect_safra_format("2023") == "2023/24"

    def test_with_prefix(self):
        assert detect_safra_format("Safra 2023/24") == "2023/24"

    def test_whitespace(self):
        assert detect_safra_format("  2023/24  ") == "2023/24"


class TestConstants:
    def test_ufs_count(self):
        assert len(UFS_BRASIL) == 27

    def test_regioes_count(self):
        assert len(REGIOES_BRASIL) == 5

    def test_ufs_are_uppercase(self):
        for uf in UFS_BRASIL:
            assert uf == uf.upper()
            assert len(uf) == 2
