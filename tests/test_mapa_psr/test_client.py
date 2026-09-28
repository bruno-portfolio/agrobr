from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from agrobr import constants
from agrobr.alt.mapa_psr import client
from agrobr.exceptions import ResourceLimitError, SourceUnavailableError
from tests.helpers import RETRY_SLEEP, TrackedAsyncStream, levanta_exatamente

CSV = b"ANO_APOLICE;SG_UF_PROPRIEDADE;NM_CULTURA_GLOBAL\n" + b"2023;MT;SOJA\n" * 10


@pytest.fixture
def serve(monkeypatch):
    original = httpx.AsyncClient
    monkeypatch.setattr(RETRY_SLEEP, AsyncMock())

    def install(handler):
        monkeypatch.setattr(
            client.httpx,
            "AsyncClient",
            lambda **kwargs: original(transport=httpx.MockTransport(handler), **kwargs),
        )

    return install


@pytest.mark.parametrize(
    "status,error", [(404, SourceUnavailableError), (500, SourceUnavailableError)]
)
async def test_erro_http_propaga_sem_dados(serve, status, error):
    serve(lambda _: httpx.Response(status, content=CSV))
    with levanta_exatamente(error):
        async with client.open_periodo("2025"):
            raise AssertionError("download com erro HTTP entregue")


async def test_download_respeita_orcamento(serve, monkeypatch):
    monkeypatch.setattr(constants, "MAPA_PSR_MAX_DOWNLOAD_BYTES", 5)
    stream = TrackedAsyncStream([b"x" * 65536, b"not downloaded"])
    serve(lambda _: httpx.Response(200, stream=stream))
    with pytest.raises(ResourceLimitError):
        async with client.open_periodo("2025"):
            raise AssertionError("download acima do orçamento entregue")
    assert stream.received == 1
    assert stream.closed


async def test_periodo_invalido_falha_antes_da_rede(serve):
    serve(lambda _: pytest.fail("Não deveria acessar a rede"))
    with levanta_exatamente(ValueError, match="invalido"):
        async with client.open_periodo("2030"):
            raise AssertionError("período inválido entregue")
