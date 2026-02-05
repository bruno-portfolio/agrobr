"""Testes para o registry de datasets."""

import pytest

from agrobr.datasets import registry


class TestRegistry:
    def test_list_datasets_returns_list(self):
        result = registry.list_datasets()
        assert isinstance(result, list)

    def test_list_datasets_includes_preco_diario(self):
        result = registry.list_datasets()
        assert "preco_diario" in result

    def test_get_dataset_found(self):
        dataset = registry.get_dataset("preco_diario")
        assert dataset is not None
        assert dataset.info.name == "preco_diario"

    def test_get_dataset_not_found(self):
        with pytest.raises(KeyError) as exc_info:
            registry.get_dataset("nao_existe")
        assert "nao_existe" in str(exc_info.value)

    def test_list_products_preco_diario(self):
        products = registry.list_products("preco_diario")
        assert isinstance(products, list)
        assert "soja" in products
        assert "milho" in products

    def test_info_preco_diario(self):
        info = registry.info("preco_diario")
        assert isinstance(info, dict)
        assert info["name"] == "preco_diario"
        assert "sources" in info
        assert "products" in info
        assert "contract_version" in info
