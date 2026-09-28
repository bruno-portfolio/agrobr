"""Testes para o registry de datasets."""

import pytest

from agrobr.datasets import registry
from tests.helpers import collect_failures, isolated_dataset_case


class TestRegistry:
    def test_registry_casos_2(self):
        with collect_failures() as check:
            case = "test_list_datasets_returns_list"
            with check(case), isolated_dataset_case(case):
                result = registry.list_datasets()
                assert isinstance(result, list)
            case = "test_list_datasets_includes_preco_diario"
            with check(case), isolated_dataset_case(case):
                result = registry.list_datasets()
                assert {"abate_trimestral", "preco_diario"} <= set(result)
            case = "test_list_datasets_sorted"
            with check(case), isolated_dataset_case(case):
                result = registry.list_datasets()
                assert result == sorted(result)

    def test_info_preco_diario(self):
        info = registry.info("preco_diario")
        assert isinstance(info, dict)
        assert info["name"] == "preco_diario"
        assert "sources" in info
        assert "products" in info
        assert "contract_version" in info

    def test_info_not_found(self):
        with pytest.raises(KeyError, match="'nao_existe' não encontrado"):
            registry.info("nao_existe")


class TestRegistryDescribe:
    def test_describe_lista_os_produtos_do_dataset(self):
        texto = registry.describe("exportacao")
        produtos = ", ".join(registry.info("exportacao")["products"])
        assert f"  Products: {produtos}" in texto.split("\n")

    def test_describe_all(self):
        result = registry.describe_all()
        censo = [linha for linha in result.split("\n") if linha.startswith("censo_agropecuario ")]
        total = len(registry.info("censo_agropecuario")["products"])
        assert len(censo) == 1, result
        assert censo[0].endswith(f"+{total - 4}")
        assert "Dataset" in result
        assert "Institution" in result
        assert "Frequency" in result
        assert "License" in result
        assert "Products" in result
        lines = result.strip().split("\n")
        assert len(lines) >= 3
        assert "-" * 10 in lines[1]
