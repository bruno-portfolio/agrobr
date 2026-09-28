from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
import pytest

from agrobr import constants
from agrobr.acervo_fundiario import client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import levanta_exatamente

pytestmark = pytest.mark.usefixtures("isolated_cache")

URL = "https://certificacao.incra.gov.br/csv_shp/zip/Sigef%20Brasil_SE.zip"


def _servir(
    monkeypatch: pytest.MonkeyPatch, responder: Callable[[httpx.Request], httpx.Response]
) -> list[str]:
    pedidos: list[str] = []
    original = httpx.AsyncClient

    def espiao(request: httpx.Request) -> httpx.Response:
        pedidos.append(request.method)
        return responder(request)

    def fabrica(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(espiao), **kwargs)

    monkeypatch.setattr(client.httpx, "AsyncClient", fabrica)
    return pedidos


def _sem_rede(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("sem rede", request=request)


def _erro_500(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(500)


@pytest.mark.parametrize(
    ("responder", "causa", "mensagem"),
    [
        (_sem_rede, httpx.ConnectError, "ConnectError: sem rede"),
        (_erro_500, httpx.HTTPStatusError, "RetriableStatusError: HTTP 500"),
    ],
    ids=["sem_rede", "http_500"],
)
async def test_download_tipa_a_falha_e_repete(monkeypatch, responder, causa, mensagem):
    pedidos = _servir(monkeypatch, responder)
    with levanta_exatamente(SourceUnavailableError) as erro:
        await client.download_and_cache("sigef", "SE")
    assert mensagem in str(erro.value)
    assert isinstance(erro.value.__cause__, causa)
    assert erro.value.url == URL
    assert pedidos == ["GET"] * constants.HTTPSettings().max_retries


async def test_head_tipa_a_falha_e_repete(monkeypatch):
    pedidos = _servir(monkeypatch, _erro_500)
    async with httpx.AsyncClient() as http:
        with levanta_exatamente(SourceUnavailableError) as erro:
            await client._head(http, URL)
    assert "RetriableStatusError: HTTP 500" in str(erro.value)
    assert isinstance(erro.value.__cause__, httpx.HTTPStatusError)
    assert pedidos == ["HEAD"] * constants.HTTPSettings().max_retries


async def test_status_nao_transitorio_tipa_sem_repetir(monkeypatch):
    pedidos = _servir(monkeypatch, lambda _request: httpx.Response(403))
    with levanta_exatamente(SourceUnavailableError) as erro:
        await client.download_and_cache("sigef", "SE")
    assert "HTTP 403: a fonte recusou o pedido" in str(erro.value)
    assert isinstance(erro.value.__cause__, httpx.HTTPStatusError)
    assert pedidos == ["GET"]
