from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from agrobr.b3 import api, client
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from tests.helpers import conferir_corpo, sem_excecao

CSV_OI = Path(__file__).parents[1] / "golden_data" / "b3" / "posicoes_sample" / "response.csv"


async def test_csv_das_posicoes_traz_o_recurso_sem_o_token_e_o_ticket(monkeypatch):
    corpo = CSV_OI.read_bytes()

    def responder(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("requestname"):
            return httpx.Response(200, json={"token": "tok-9f8e7d6c5b4a"})
        assert request.url.params["token"] == "tok-9f8e7d6c5b4a"
        return httpx.Response(200, content=corpo)

    original = httpx.AsyncClient
    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(responder), **kwargs),
    )
    with sem_excecao():
        _, meta = await api.posicoes_abertas(data="2025-12-19", return_meta=True)

    conferir_corpo(meta, corpo)
    assert meta.source_url == f"{client.BASE_URL_ARQUIVOS}?token=[REDACTED]"
    assert meta.source_details == {"ticket_url": client.ticket_url("2025-12-19")}
    assert "date=2025-12-19" in meta.source_details["ticket_url"]


async def test_oi_404_download_returns_empty(monkeypatch):
    token = httpx.Response(
        200, json={"token": "test"}, request=httpx.Request("GET", "https://example.com")
    )
    unavailable = httpx.Response(404, request=httpx.Request("GET", "https://example.com"))
    monkeypatch.setattr(client, "retry_on_status", AsyncMock(side_effect=[token, unavailable]))
    frame, meta = await api.posicoes_abertas(data="2026-09-03", return_meta=True)
    assert frame.empty
    assert meta.records_count == 0


@pytest.mark.parametrize("value", ["", "20260903", "2026-02-30", "03/09/2026", None, 2026])
async def test_invalid_date_rejected_before_token(value, monkeypatch):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "_fetch_posicoes_abertas", fetch)
    with pytest.raises(InvalidParameterError):
        await client.fetch_posicoes_abertas(value)
    fetch.assert_not_awaited()


async def test_download_400_is_not_unpublished(monkeypatch):
    request = httpx.Request("GET", "https://example.com")
    token = httpx.Response(200, json={"token": "test"}, request=request)
    failure = httpx.Response(400, text="pedido inválido", request=request)
    monkeypatch.setattr(client, "retry_on_status", AsyncMock(side_effect=[token, failure]))
    with pytest.raises(SourceUnavailableError, match="HTTP 400: pedido inválido"):
        await client.fetch_posicoes_abertas("2026-09-03")


async def test_concurrent_downloads_do_not_invalidate_tokens(monkeypatch):
    events = []
    active_token = ""

    async def respond(request):
        nonlocal active_token
        if request.url.path.endswith("requestname"):
            active_token = request.url.params["date"]
            events.append(("token", active_token))
            await asyncio.sleep(0)
            return httpx.Response(200, json={"token": active_token})
        token = request.url.params["token"]
        events.append(("download", token))
        if token != active_token:
            return httpx.Response(401)
        return httpx.Response(200, content=b"x" * 200)

    original = httpx.AsyncClient
    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(respond), **kwargs),
    )
    with sem_excecao():
        results = await asyncio.gather(
            client.fetch_posicoes_abertas("2026-09-02"), client.fetch_posicoes_abertas("2026-09-03")
        )
    assert all(content for content, _ in results)
    assert events == [
        ("token", "2026-09-02"),
        ("download", "2026-09-02"),
        ("token", "2026-09-03"),
        ("download", "2026-09-03"),
    ]
