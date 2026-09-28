from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from agrobr import ibge
from agrobr.exceptions import InvalidParameterError
from agrobr.ibge import client
from tests.helpers import levanta_exatamente


@pytest.mark.parametrize(
    "chamada",
    [
        lambda: ibge.abate("bovino", trimestre="2024T1", uf="XX"),
        lambda: ibge.leite_trimestral(trimestre="2024T1", uf="XX"),
    ],
    ids=["abate", "leite_trimestral"],
)
async def test_uf_desconhecida_e_recusada_antes_da_rede(monkeypatch, chamada):
    sidra = AsyncMock()
    monkeypatch.setattr(client, "fetch_sidra", sidra)
    with levanta_exatamente(InvalidParameterError, match="UF invalida: 'XX'"):
        await chamada()
    sidra.assert_not_awaited()


def test_codigo_ibge_da_uf_e_o_codigo_ja_valido():
    assert client.uf_to_ibge_code(" mt ") == "51"
    assert client.uf_to_ibge_code("51") == "51"
