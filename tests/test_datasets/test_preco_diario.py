"""Testes para o dataset preco_diario."""

import pytest

from agrobr.datasets.preco_diario import PRECO_DIARIO_INFO, PrecoDiarioDataset


class TestPrecoDiarioInfo:
    def test_info_name(self):
        assert PRECO_DIARIO_INFO.name == "preco_diario"

    def test_info_products(self):
        assert "soja" in PRECO_DIARIO_INFO.products
        assert "milho" in PRECO_DIARIO_INFO.products
        assert "boi" in PRECO_DIARIO_INFO.products
        assert "cafe" in PRECO_DIARIO_INFO.products

    def test_info_sources(self):
        source_names = [s.name for s in PRECO_DIARIO_INFO.sources]
        assert "cepea" in source_names
        assert "cache" in source_names

    def test_info_cepea_priority(self):
        cepea_source = next(s for s in PRECO_DIARIO_INFO.sources if s.name == "cepea")
        cache_source = next(s for s in PRECO_DIARIO_INFO.sources if s.name == "cache")
        assert cepea_source.priority < cache_source.priority

    def test_info_contract_version(self):
        assert PRECO_DIARIO_INFO.contract_version == "1.0"


class TestPrecoDiarioDataset:
    def test_validate_produto_valid(self):
        dataset = PrecoDiarioDataset()
        dataset._validate_produto("soja")

    def test_validate_produto_invalid(self):
        dataset = PrecoDiarioDataset()
        with pytest.raises(ValueError) as exc_info:
            dataset._validate_produto("banana")
        assert "banana" in str(exc_info.value)
        assert "preco_diario" in str(exc_info.value)

    def test_info_to_dict(self):
        info_dict = PRECO_DIARIO_INFO.to_dict()
        assert info_dict["name"] == "preco_diario"
        assert "cepea" in info_dict["sources"]
        assert "soja" in info_dict["products"]
