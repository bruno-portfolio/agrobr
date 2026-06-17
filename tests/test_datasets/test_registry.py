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

    def test_list_datasets_sorted(self):
        result = registry.list_datasets()
        assert result == sorted(result)

    def test_get_dataset_found(self):
        dataset = registry.get_dataset("preco_diario")
        assert dataset is not None
        assert dataset.info.name == "preco_diario"

    def test_get_dataset_not_found(self):
        with pytest.raises(KeyError) as exc_info:
            registry.get_dataset("nao_existe")
        assert "nao_existe" in str(exc_info.value)

    def test_get_dataset_not_found_lists_available(self):
        with pytest.raises(KeyError, match="Disponíveis"):
            registry.get_dataset("xpto_fake")

    def test_list_products_preco_diario(self):
        products = registry.list_products("preco_diario")
        assert isinstance(products, list)
        assert "soja" in products
        assert "milho" in products
        assert "cafe_robusta" in products

    def test_list_products_not_found(self):
        with pytest.raises(KeyError):
            registry.list_products("nao_existe")

    def test_info_preco_diario(self):
        info = registry.info("preco_diario")
        assert isinstance(info, dict)
        assert info["name"] == "preco_diario"
        assert "sources" in info
        assert "products" in info
        assert "contract_version" in info

    def test_info_not_found(self):
        with pytest.raises(KeyError):
            registry.info("nao_existe")


class TestRegistryDescribe:
    def test_describe_preco_diario(self):
        result = registry.describe("preco_diario")
        assert "Dataset: preco_diario" in result
        assert "Institution:" in result
        assert "URL:" in result
        assert "License:" in result
        assert "Products:" in result
        assert "Sources:" in result
        assert "Frequency:" in result
        assert "Contract:" in result
        assert "Min date:" in result
        assert "Unit:" in result

    def test_describe_not_found(self):
        with pytest.raises(KeyError):
            registry.describe("nao_existe")

    def test_describe_contains_source_names(self):
        result = registry.describe("preco_diario")
        assert "cepea" in result

    def test_describe_all(self):
        result = registry.describe_all()
        assert "Dataset" in result
        assert "Institution" in result
        assert "Frequency" in result
        assert "License" in result
        assert "Products" in result
        lines = result.strip().split("\n")
        assert len(lines) >= 3
        assert "-" * 10 in lines[1]

    def test_describe_all_contains_all_datasets(self):
        result = registry.describe_all()
        for name in registry.list_datasets():
            assert name in result

    def test_describe_all_truncates_products(self):
        result = registry.describe_all()
        assert isinstance(result, str)
        for name in sorted(registry.list_datasets()):
            ds = registry.get_dataset(name)
            if len(ds.info.products) > 4:
                assert f"+{len(ds.info.products) - 4}" in result
                break


class TestRegistryRegister:
    def test_register_returns_dataset(self):
        ds = registry.get_dataset("preco_diario")
        result = registry.register(ds)
        assert result is ds
