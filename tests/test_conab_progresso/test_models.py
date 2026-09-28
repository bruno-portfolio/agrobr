from __future__ import annotations

from agrobr.conab.progresso.models import (
    estado_para_uf,
)


class TestEstadoParaUf:
    def test_double_space(self) -> None:
        assert estado_para_uf("Mato  Grosso") == "MT"
