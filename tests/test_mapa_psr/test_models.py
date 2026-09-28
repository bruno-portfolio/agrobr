"""Testes para agrobr.alt.mapa_psr.models."""

from __future__ import annotations

from agrobr.alt.mapa_psr.models import (
    _resolve_periodos,
)


class TestResolvePeriodos:
    def test_apenas_ano_inicio(self):
        result = _resolve_periodos(ano_inicio=2020)
        assert "2016-2024" in result
        assert "2025" in result
        assert "2006-2015" not in result

    def test_apenas_ano_fim(self):
        result = _resolve_periodos(ano_fim=2015)
        assert "2006-2015" in result
        assert "2016-2024" not in result or "2025" not in result
