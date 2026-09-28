from __future__ import annotations

from agrobr.alt.antt_pedagio.models import (
    ANO_INICIO,
    _resolve_anos,
)
from agrobr.utils.time import hoje


class TestResolveAnos:
    def test_range_inicio_only(self):
        result = _resolve_anos(ano_inicio=2024)
        assert 2024 in result
        assert result[0] == 2024
        assert result[-1] == hoje().year

    def test_range_fim_only(self):
        result = _resolve_anos(ano_fim=2012)
        assert result[0] == ANO_INICIO
        assert result[-1] == 2012

    def test_default_2_anos(self):
        result = _resolve_anos()
        assert len(result) == 2
        current = hoje().year
        assert result == [current - 1, current]
