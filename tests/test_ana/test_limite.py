from __future__ import annotations

from unittest.mock import Mock

import pytest

from agrobr import ana
from agrobr.exceptions import InvalidParameterError
from agrobr.utils import geo


@pytest.mark.parametrize(
    "nome",
    [
        "hidrografia",
        "hidrografia_geo",
        "pivos_irrigacao",
        "pivos_irrigacao_geo",
        "demanda_irrigacao",
        "demanda_irrigacao_geo",
        "disponibilidade_hidrica",
        "disponibilidade_hidrica_geo",
    ],
)
@pytest.mark.parametrize("limite", [-1, 0, True, 1.5, "10"])
async def test_limite_invalido_recusado_antes_da_rede(nome, limite, monkeypatch):
    http = Mock(side_effect=AssertionError("Cliente HTTP não deveria ser aberto"))
    monkeypatch.setattr(geo.httpx, "AsyncClient", http)
    with pytest.raises(InvalidParameterError, match="max_registros"):
        await getattr(ana, nome)(bbox=(-48.1, -16.1, -47.9, -15.9), max_registros=limite)
    http.assert_not_called()


async def test_nome_antigo_do_limite_recusado_antes_da_rede(monkeypatch):
    http = Mock(side_effect=AssertionError("Cliente HTTP não deveria ser aberto"))
    monkeypatch.setattr(geo.httpx, "AsyncClient", http)
    with pytest.raises(TypeError, match="max_features"):
        await ana.hidrografia(bbox=(-48.1, -16.1, -47.9, -15.9), max_features=1)
    http.assert_not_called()
