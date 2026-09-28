from __future__ import annotations

import warnings
from typing import Any

import httpx
import pytest

from agrobr import datasets, usda
from tests.helpers import sem_excecao

AVISO = (
    "usda: a fonte respondeu HTTP 404 para o ano 2024: sem dado publicado para a combinação "
    "(ou a URL da API mudou); o resultado vem vazio"
)


@pytest.fixture
def psd_404(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """A API PSD responde 404 com corpo vazio, como ao vivo para um ano sem dado (soja BR 1950)."""
    monkeypatch.setenv("AGROBR_USDA_API_KEY", "chave-de-teste")
    pedidos: list[str] = []

    async def responder(_transporte: Any, request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        return httpx.Response(
            404, headers={"content-type": "text/plain"}, content=b"", request=request
        )

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", responder)
    return pedidos


@pytest.mark.parametrize(
    "consulta",
    [
        lambda: usda.psd("soja", country="BR", market_year=2024, return_meta=True),
        lambda: datasets.oferta_demanda_global("soja", market_year=2024, return_meta=True),
    ],
    ids=["fonte", "dataset"],
)
async def test_404_do_psd_vem_vazio_com_aviso(psd_404, consulta):
    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        frame, meta = await consulta()

    assert frame.empty
    assert AVISO in meta.validation_warnings
    assert [str(aviso.message) for aviso in emitidos if "HTTP 404" in str(aviso.message)] == [AVISO]
    assert len(psd_404) == 1
