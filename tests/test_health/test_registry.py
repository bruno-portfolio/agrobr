"""Tests for agrobr.health.registry module."""

from __future__ import annotations

import ssl

from agrobr.constants import URLS, Fonte
from agrobr.health.registry import (
    HEALTH_REGISTRY,
    SOURCE_DATASET_MAP,
    SourceHealthConfig,
    get_affected_datasets,
)


class TestHealthRegistry:
    def test_all_fontes_in_registry(self):
        for fonte in Fonte:
            assert fonte in HEALTH_REGISTRY, f"{fonte} missing from HEALTH_REGISTRY"

    def test_registry_has_22_entries(self):
        assert len(HEALTH_REGISTRY) == len(Fonte)

    def test_all_urls_are_nonempty_strings(self):
        for fonte, config in HEALTH_REGISTRY.items():
            assert isinstance(config.url, str), f"{fonte} url is not a string"
            assert len(config.url) > 0, f"{fonte} has empty url"

    def test_config_is_frozen_dataclass(self):
        config = HEALTH_REGISTRY[Fonte.CEPEA]
        assert isinstance(config, SourceHealthConfig)

    def test_cepea_has_deep_check(self):
        assert HEALTH_REGISTRY[Fonte.CEPEA].has_deep_check is True

    def test_cepea_is_critical_tier(self):
        assert HEALTH_REGISTRY[Fonte.CEPEA].tier == "critical"

    def test_usda_requires_api_key(self):
        config = HEALTH_REGISTRY[Fonte.USDA]
        assert config.requires_api_key is True
        assert config.api_key_env_var == "AGROBR_USDA_API_KEY"

    def test_inmet_requires_api_key(self):
        config = HEALTH_REGISTRY[Fonte.INMET]
        assert config.requires_api_key is True
        assert config.api_key_env_var == "AGROBR_INMET_TOKEN"

    def test_comtrade_requires_api_key(self):
        config = HEALTH_REGISTRY[Fonte.COMTRADE]
        assert config.requires_api_key is True

    def test_ibge_uses_api_url(self):
        config = HEALTH_REGISTRY[Fonte.IBGE]
        assert "apisidra" in config.url
        assert "/values/t/5457/" in config.url

    def test_gov_sources_use_dataset_endpoints(self):
        expected = {
            Fonte.ANA: URLS[Fonte.ANA]["arcgis"],
            Fonte.ANP_DIESEL: URLS[Fonte.ANP_DIESEL]["vendas_diesel_csv"],
            Fonte.ANTAQ: URLS[Fonte.ANTAQ]["bulk_txt"],
            Fonte.ANTT_PEDAGIO: "package_show?id=volume-trafego-praca-pedagio",
            Fonte.CONAB: URLS[Fonte.CONAB]["boletim_graos"],
            Fonte.DEFENSIVOS: URLS[Fonte.DEFENSIVOS]["formulados"],
            Fonte.FUNAI: URLS[Fonte.FUNAI]["geoserver"],
            Fonte.ICMBIO: URLS[Fonte.ICMBIO]["geoserver"],
            Fonte.INCRA: URLS[Fonte.INCRA]["geoserver"],
            Fonte.LISTA_SUJA: URLS[Fonte.LISTA_SUJA]["download"],
            Fonte.MAPA_PSR: URLS[Fonte.MAPA_PSR]["dataset"],
            Fonte.SFB: URLS[Fonte.SFB]["arcgis"],
            Fonte.SICAR: URLS[Fonte.SICAR]["geoserver"],
            Fonte.RNC: URLS[Fonte.RNC]["cultivarweb"],
            Fonte.ZARC: "package_show?id=tabua-de-risco-zoneamento-agricola-de-risco-climatico",
        }

        for fonte, url_part in expected.items():
            assert url_part in HEALTH_REGISTRY[fonte].url

    def test_file_download_sources_use_head(self):
        sources = [
            Fonte.ANTAQ,
            Fonte.COMEXSTAT,
            Fonte.DEFENSIVOS,
        ]

        for fonte in sources:
            assert HEALTH_REGISTRY[fonte].method == "HEAD"

    def test_comexstat_disables_tls_verification(self):
        assert HEALTH_REGISTRY[Fonte.COMEXSTAT].verify is False

    def test_antaq_is_best_effort(self):
        assert HEALTH_REGISTRY[Fonte.ANTAQ].tier == "best_effort"

    def test_sicar_uses_legacy_tls_context(self):
        assert isinstance(HEALTH_REGISTRY[Fonte.SICAR].verify, ssl.SSLContext)

    def test_cepea_marks_403_as_soft_block(self):
        assert HEALTH_REGISTRY[Fonte.CEPEA].soft_block_codes == (403,)

    def test_default_method_is_get(self):
        for config in HEALTH_REGISTRY.values():
            assert config.method in ("GET", "HEAD")

    def test_default_tier_is_standard(self):
        for fonte, config in HEALTH_REGISTRY.items():
            if fonte != Fonte.CEPEA:
                assert config.tier in ("standard", "best_effort"), f"{fonte} unexpected tier"


class TestSourceDatasetMap:
    def test_cepea_datasets(self):
        assert "preco_diario" in SOURCE_DATASET_MAP["cepea"]

    def test_conab_datasets(self):
        datasets = SOURCE_DATASET_MAP["conab"]
        assert "estimativa_safra" in datasets
        assert "producao_anual" in datasets

    def test_ibge_datasets(self):
        datasets = SOURCE_DATASET_MAP["ibge"]
        assert "estimativa_safra" in datasets
        assert "abate_trimestral" in datasets

    def test_unknown_source_returns_empty(self):
        assert get_affected_datasets(Fonte.DEFENSIVOS) == []

    def test_get_affected_datasets_cepea(self):
        result = get_affected_datasets(Fonte.CEPEA)
        assert result == ["preco_diario"]

    def test_get_affected_datasets_ibge(self):
        result = get_affected_datasets(Fonte.IBGE)
        assert len(result) == 12

    def test_get_affected_datasets_nasa_power(self):
        result = get_affected_datasets(Fonte.NASA_POWER)
        assert result == ["clima"]
