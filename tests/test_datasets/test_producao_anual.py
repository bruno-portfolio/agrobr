"""Testes para o dataset producao_anual."""

import pytest

from agrobr.datasets.producao_anual import ProducaoAnualDataset, PRODUCAO_ANUAL_INFO


class TestProducaoAnualInfo:
    def test_info_name(self):
        assert PRODUCAO_ANUAL_INFO.name == "producao_anual"

    def test_info_products(self):
        assert "soja" in PRODUCAO_ANUAL_INFO.products
        assert "milho" in PRODUCAO_ANUAL_INFO.products
        assert "cafe" in PRODUCAO_ANUAL_INFO.products

    def test_info_sources(self):
        source_names = [s.name for s in PRODUCAO_ANUAL_INFO.sources]
        assert "ibge_pam" in source_names
        assert "conab" in source_names

    def test_info_ibge_pam_priority(self):
        ibge_source = next(s for s in PRODUCAO_ANUAL_INFO.sources if s.name == "ibge_pam")
        conab_source = next(s for s in PRODUCAO_ANUAL_INFO.sources if s.name == "conab")
        assert ibge_source.priority < conab_source.priority

    def test_info_contract_version(self):
        assert PRODUCAO_ANUAL_INFO.contract_version == "1.0"

    def test_info_update_frequency(self):
        assert PRODUCAO_ANUAL_INFO.update_frequency == "yearly"


class TestProducaoAnualDataset:
    def test_validate_produto_valid(self):
        dataset = ProducaoAnualDataset()
        dataset._validate_produto("soja")

    def test_validate_produto_invalid(self):
        dataset = ProducaoAnualDataset()
        with pytest.raises(ValueError) as exc_info:
            dataset._validate_produto("banana")
        assert "banana" in str(exc_info.value)

    def test_info_to_dict(self):
        info_dict = PRODUCAO_ANUAL_INFO.to_dict()
        assert info_dict["name"] == "producao_anual"
        assert "ibge_pam" in info_dict["sources"]
