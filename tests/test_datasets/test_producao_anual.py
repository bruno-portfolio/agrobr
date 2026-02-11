"""Testes específicos para o dataset producao_anual."""

from agrobr.datasets.producao_anual import PRODUCAO_ANUAL_INFO


class TestProducaoAnualSpecific:
    def test_info_ibge_pam_priority(self):
        ibge_source = next(s for s in PRODUCAO_ANUAL_INFO.sources if s.name == "ibge_pam")
        conab_source = next(s for s in PRODUCAO_ANUAL_INFO.sources if s.name == "conab")
        assert ibge_source.priority < conab_source.priority
