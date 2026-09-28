from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
import pytest

from agrobr import conab, constants
from agrobr.conab import client
from agrobr.exceptions import SourceUnavailableError
from agrobr.http import browser
from tests.helpers import levanta_exatamente

BOLETIM = constants.URLS[constants.Fonte.CONAB]["boletim_graos"]
PLANILHA = "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/safra-de-graos/boletim.xlsx"


def _servir(
    monkeypatch: pytest.MonkeyPatch, responder: Callable[[httpx.Request], httpx.Response]
) -> list[str]:
    pedidos: list[str] = []
    original = httpx.AsyncClient

    def espiao(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        return responder(request)

    def fabrica(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(espiao), **kwargs)

    monkeypatch.setattr(client.httpx, "AsyncClient", fabrica)
    monkeypatch.setattr(browser, "is_available", lambda: False)
    return pedidos


def _sem_rede(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("sem rede", request=request)


CASOS = [
    (_sem_rede, "SourceUnavailableError", "ConnectError: sem rede"),
    (lambda _r: httpx.Response(500), "SourceUnavailableError", "HTTP 500"),
    (
        lambda _r: httpx.Response(404),
        "SourceUnavailableError",
        "HTTP 404: o recurso não existe na URL",
    ),
]
IDS = ["sem_rede", "http_500", "http_404"]


@pytest.mark.parametrize(("responder", "tipo", "detalhe"), CASOS, ids=IDS)
@pytest.mark.parametrize("pedido", ["boletim", "planilha"])
async def test_mensagem_traz_a_causa_http_e_o_playwright_como_motivo(
    monkeypatch, responder, tipo, detalhe, pedido
):
    _servir(monkeypatch, responder)
    url = BOLETIM if pedido == "boletim" else PLANILHA
    with levanta_exatamente(SourceUnavailableError) as erro:
        if pedido == "boletim":
            await client.fetch_boletim_page()
        else:
            await client.download_xlsx(url)
    mensagem = str(erro.value)
    assert f"HTTP falhou ({tipo} em {url}: " in mensagem
    assert detalhe in mensagem
    assert mensagem.index("HTTP falhou") < mensagem.index(
        "o fallback por navegador também falhou: Playwright not available"
    )
    causa = erro.value.__cause__
    assert isinstance(causa, (httpx.HTTPError, SourceUnavailableError))
    assert not isinstance(causa, SourceUnavailableError) or "Playwright" not in causa.last_error


async def test_safras_sem_rede_diz_a_causa(monkeypatch):
    pedidos = _servir(monkeypatch, _sem_rede)
    with levanta_exatamente(
        SourceUnavailableError, match="HTTP falhou \\(SourceUnavailableError em "
    ):
        await conab.safras("soja", safra="2025/26")
    assert pedidos and set(pedidos) == {BOLETIM}
