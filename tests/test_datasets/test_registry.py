"""Testes para o registry de datasets."""

import copy

import pytest

from agrobr import exceptions
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


@pytest.mark.parametrize(
    "lookup", [registry.get_dataset, registry.list_products, registry.info, registry.describe]
)
def test_lookup_inexistente_preserva_keyerror_e_indica_parametro(lookup):
    with pytest.raises(exceptions.InvalidParameterError) as caught:
        lookup("nao_existe")
    assert isinstance(caught.value, KeyError)
    assert "nao_existe" in str(caught.value)
    assert "preco_diario" in str(caught.value)


def test_catalogo_devolve_copias_independentes():
    original = registry.info("preco_diario")
    original_dataset = registry._REGISTRY["preco_diario"]
    original_info = copy.deepcopy(original_dataset.info)
    products = registry.list_products("preco_diario")
    products.clear()
    info = registry.info("preco_diario")
    info["products"].clear()
    info["sources"].clear()
    dataset = registry.get_dataset("preco_diario")
    dataset.info.description = "alterado"
    dataset.info.products.clear()
    dataset.info.sources[0].enabled = False
    dataset.info.sources.clear()
    assert registry.info("preco_diario") == original
    assert original_dataset.info == original_info
    assert registry.get_dataset("preco_diario") is not dataset


def test_dataset_info_to_dict_copia_produtos():
    dataset = registry.get_dataset("preco_diario")
    original = list(dataset.info.products)
    dataset.info.to_dict()["products"].clear()
    assert dataset.info.products == original


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
