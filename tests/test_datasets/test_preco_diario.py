"""Testes específicos para o dataset preco_diario."""

from agrobr.datasets.preco_diario import PRECO_DIARIO_INFO


class TestPrecoDiarioSpecific:
    def test_info_cepea_priority(self):
        cepea_source = next(s for s in PRECO_DIARIO_INFO.sources if s.name == "cepea")
        cache_source = next(s for s in PRECO_DIARIO_INFO.sources if s.name == "cache")
        assert cepea_source.priority < cache_source.priority
