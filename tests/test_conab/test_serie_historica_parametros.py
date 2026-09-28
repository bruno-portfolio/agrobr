from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from agrobr import conab
from agrobr.conab.serie_historica import client
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente


@pytest.mark.parametrize(
    ("argumentos", "mensagem"),
    [
        ({"inicio": 2020, "fim": 2019}, "inicio \\(2020\\) posterior a fim \\(2019\\)"),
        ({"uf": "XX"}, "UF invalida: 'XX'"),
    ],
    ids=["inicio_depois_do_fim", "uf_inexistente"],
)
async def test_serie_historica_recusa_antes_da_rede(monkeypatch, argumentos, mensagem):
    download = AsyncMock()
    monkeypatch.setattr(client, "download_xls", download)
    with levanta_exatamente(InvalidParameterError, match=mensagem):
        await conab.serie_historica("soja", **argumentos)
    download.assert_not_awaited()
