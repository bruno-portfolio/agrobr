"""Testes de resiliência HTTP para agrobr.conab.custo_producao.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.conab.custo_producao import client
from agrobr.exceptions import SourceUnavailableError


def _mock_response(
    status_code: int = 200, text: str = "<html></html>", content: bytes = b"xlsx-data"
) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    resp.content = content
    resp.url = "https://www.gov.br/conab/test"
    resp.headers = {"content-type": "text/html"}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    return resp


class TestConabCustoTimeout:
    @pytest.mark.asyncio
    async def test_timeout_on_fetch_custos_page(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.conab.custo_producao.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError),
        ):
            await client.fetch_custos_page()

    @pytest.mark.asyncio
    async def test_timeout_on_download_xlsx(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.conab.custo_producao.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="conab_custo"),
        ):
            await client.download_xlsx("https://www.gov.br/test.xlsx")


class TestConabCustoHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_on_page_raises(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.conab.custo_producao.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError),
        ):
            await client.fetch_custos_page()

    @pytest.mark.asyncio
    async def test_http_403_on_download_raises(self):
        resp_403 = _mock_response(403)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_403)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.conab.custo_producao.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="conab_custo"),
        ):
            await client.download_xlsx("https://www.gov.br/test.xlsx")

    @pytest.mark.asyncio
    async def test_http_429_no_retry(self):
        resp_429 = _mock_response(429)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_429)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "agrobr.conab.custo_producao.client.httpx.AsyncClient", return_value=mock_client
        ):
            await client.download_xlsx("https://www.gov.br/test.xlsx")


class TestConabCustoEmptyResponse:
    @pytest.mark.asyncio
    async def test_all_tabs_empty_fallback_to_main_page(self):
        resp_ok = _mock_response(200, text="<html>main page</html>")
        call_count = 0

        async def side_effect(_url: str, **_kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count <= len(client._TAB_SLUGS):
                raise httpx.HTTPError("tab failed")
            return resp_ok

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=side_effect)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "agrobr.conab.custo_producao.client.httpx.AsyncClient", return_value=mock_client
        ):
            result = await client.fetch_custos_page()

        assert "main page" in result

    def test_parse_links_empty_html(self):
        links = client.parse_links_from_html("")
        assert links == []

    def test_parse_links_no_xlsx(self):
        links = client.parse_links_from_html('<html><a href="test.pdf">PDF</a></html>')
        assert links == []


class TestConabCustoDownloadXlsx:
    @pytest.mark.asyncio
    async def test_relative_url_prepends_base(self):
        resp = _mock_response(200, content=b"xlsx")
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "agrobr.conab.custo_producao.client.httpx.AsyncClient", return_value=mock_client
        ):
            await client.download_xlsx("/conab/test.xlsx")

        call_url = mock_client.get.call_args[0][0]
        assert call_url.startswith("https://www.gov.br")
