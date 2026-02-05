"""Testes para o dataset estimativa_safra."""

import pytest

from agrobr.datasets.estimativa_safra import ESTIMATIVA_SAFRA_INFO, EstimativaSafraDataset


class TestEstimativaSafraInfo:
    def test_info_name(self):
        assert ESTIMATIVA_SAFRA_INFO.name == "estimativa_safra"

    def test_info_products(self):
        assert "soja" in ESTIMATIVA_SAFRA_INFO.products
        assert "milho" in ESTIMATIVA_SAFRA_INFO.products
        assert "trigo" in ESTIMATIVA_SAFRA_INFO.products

    def test_info_sources(self):
        source_names = [s.name for s in ESTIMATIVA_SAFRA_INFO.sources]
        assert "conab" in source_names
        assert "ibge_lspa" in source_names

    def test_info_conab_priority(self):
        conab_source = next(s for s in ESTIMATIVA_SAFRA_INFO.sources if s.name == "conab")
        lspa_source = next(s for s in ESTIMATIVA_SAFRA_INFO.sources if s.name == "ibge_lspa")
        assert conab_source.priority < lspa_source.priority

    def test_info_contract_version(self):
        assert ESTIMATIVA_SAFRA_INFO.contract_version == "1.0"

    def test_info_update_frequency(self):
        assert ESTIMATIVA_SAFRA_INFO.update_frequency == "monthly"


class TestEstimativaSafraDataset:
    def test_validate_produto_valid(self):
        dataset = EstimativaSafraDataset()
        dataset._validate_produto("soja")

    def test_validate_produto_invalid(self):
        dataset = EstimativaSafraDataset()
        with pytest.raises(ValueError) as exc_info:
            dataset._validate_produto("abacaxi")
        assert "abacaxi" in str(exc_info.value)

    def test_info_to_dict(self):
        info_dict = ESTIMATIVA_SAFRA_INFO.to_dict()
        assert info_dict["name"] == "estimativa_safra"
        assert "conab" in info_dict["sources"]
