from __future__ import annotations

from io import BytesIO
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.conab import client


@pytest.mark.asyncio
@pytest.mark.parametrize("status,body", [(403, b"Forbidden"), (200, b"<html>Unavailable</html>")])
async def test_boletim_http_invalido_usa_browser(status: int, body: bytes):
    response = httpx.Response(status, content=body, request=httpx.Request("GET", "https://gov.br"))
    with (
        patch.object(client, "_fetch_http", new_callable=AsyncMock) as fetch,
        patch.object(client, "_fetch_boletim_page_browser", return_value="levantamento") as browser,
    ):
        if status == 403:
            fetch.side_effect = httpx.HTTPStatusError(
                "Forbidden", request=response.request, response=response
            )
        else:
            fetch.return_value = body
        assert await client.fetch_boletim_page() == "levantamento"
    browser.assert_awaited_once()


@pytest.mark.asyncio
async def test_download_http_html_usa_browser():
    expected = BytesIO(b"xlsx")
    with (
        patch.object(client, "_fetch_http", return_value=b"<html>" + b"x" * 3000),
        patch.object(client, "_download_xlsx_browser", return_value=expected) as browser,
    ):
        assert await client.download_xlsx("https://gov.br/current.xlsx") is expected
    browser.assert_awaited_once_with("https://gov.br/current.xlsx")


@pytest.mark.asyncio
async def test_http_status_transitorio_repete_antes_do_browser():
    responses = iter([503, 200])

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(next(responses), content=b"recovered", request=request)

    http_client = httpx.AsyncClient
    with patch.object(
        client.httpx,
        "AsyncClient",
        side_effect=lambda **kwargs: http_client(transport=httpx.MockTransport(respond), **kwargs),
    ):
        assert await client._fetch_http("https://gov.br/current.xlsx") == b"recovered"
    assert list(responses) == []
