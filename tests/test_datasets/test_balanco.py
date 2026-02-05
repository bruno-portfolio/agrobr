"""Testes para o dataset balanco."""

import pytest

from agrobr.datasets.balanco import BalancoDataset, BALANCO_INFO


class TestBalancoInfo:
    def test_info_name(self):
        assert BALANCO_INFO.name == "balanco"

    def test_info_products(self):
        assert "soja" in BALANCO_INFO.products
        assert "milho" in BALANCO_INFO.products
        assert "arroz" in BALANCO_INFO.products

    def test_info_sources(self):
        source_names = [s.name for s in BALANCO_INFO.sources]
        assert "conab" in source_names
        assert len(source_names) == 1

    def test_info_contract_version(self):
        assert BALANCO_INFO.contract_version == "1.0"

    def test_info_update_frequency(self):
        assert BALANCO_INFO.update_frequency == "monthly"


class TestBalancoDataset:
    def test_validate_produto_valid(self):
        dataset = BalancoDataset()
        dataset._validate_produto("soja")

    def test_validate_produto_invalid(self):
        dataset = BalancoDataset()
        with pytest.raises(ValueError) as exc_info:
            dataset._validate_produto("laranja")
        assert "laranja" in str(exc_info.value)

    def test_info_to_dict(self):
        info_dict = BALANCO_INFO.to_dict()
        assert info_dict["name"] == "balanco"
        assert "conab" in info_dict["sources"]
