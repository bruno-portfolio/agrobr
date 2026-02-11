"""Testes específicos para o dataset estimativa_safra."""

from agrobr.datasets.estimativa_safra import ESTIMATIVA_SAFRA_INFO


class TestEstimativaSafraSpecific:
    def test_info_conab_priority(self):
        conab_source = next(s for s in ESTIMATIVA_SAFRA_INFO.sources if s.name == "conab")
        lspa_source = next(s for s in ESTIMATIVA_SAFRA_INFO.sources if s.name == "ibge_lspa")
        assert conab_source.priority < lspa_source.priority
