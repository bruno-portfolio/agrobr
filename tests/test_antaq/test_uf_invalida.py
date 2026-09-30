from __future__ import annotations

from unittest.mock import AsyncMock

from agrobr import antaq
from agrobr.antaq import client
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente


async def test_uf_desconhecida_e_recusada_antes_da_descarga(monkeypatch):
    ano_zip = AsyncMock()
    mercadoria_zip = AsyncMock()
    monkeypatch.setattr(client, "fetch_ano_zip", ano_zip)
    monkeypatch.setattr(client, "fetch_mercadoria_zip", mercadoria_zip)
    with levanta_exatamente(InvalidParameterError, match="UF inválida: 'XX'"):
        await antaq.movimentacao(2024, uf="XX")
    ano_zip.assert_not_awaited()
    mercadoria_zip.assert_not_awaited()
