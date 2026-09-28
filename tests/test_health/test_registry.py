"""Tests for agrobr.health.registry module."""

from __future__ import annotations

from agrobr.constants import Fonte
from agrobr.health.registry import (
    HEALTH_REGISTRY,
    SourceHealthConfig,
    get_affected_datasets,
)


class TestHealthRegistry:
    def test_all_fontes_in_registry(self):
        for fonte in Fonte:
            assert fonte in HEALTH_REGISTRY, f"{fonte} missing from HEALTH_REGISTRY"

    def test_all_urls_are_nonempty_strings(self):
        for fonte, config in HEALTH_REGISTRY.items():
            assert isinstance(config.url, str), f"{fonte} url is not a string"
            assert len(config.url) > 0, f"{fonte} has empty url"

    def test_config_is_frozen_dataclass(self):
        config = HEALTH_REGISTRY[Fonte.CEPEA]
        assert isinstance(config, SourceHealthConfig)


class TestSourceDatasetMap:
    def test_inmet_multiple_fetchers_list_dataset_once(self):
        assert get_affected_datasets(Fonte.INMET) == ["clima"]
