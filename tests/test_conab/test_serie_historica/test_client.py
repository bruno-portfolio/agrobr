"""Testes de resiliência HTTP para agrobr.conab.serie_historica.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.conab.serie_historica import client
from agrobr.exceptions import SourceUnavailableError


def _mock_response(
    status_code: int = 200, content: bytes = b"xls-data", text: str = "<html></html>"
) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.content = content
    resp.text = text
    resp.url = "https://www.gov.br/conab/test"
    resp.headers = {"content-type": "application/vnd.ms-excel"}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    return resp


class TestConabSerieTimeout:
    @pytest.mark.asyncio
    async def test_timeout_on_download_xls(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch(
                "agrobr.conab.serie_historica.client.httpx.AsyncClient", return_value=mock_client
            ),
            pytest.raises(SourceUnavailableError, match="conab_serie_historica"),
        ):
            await client.download_xls("soja")

    @pytest.mark.asyncio
    async def test_timeout_on_fetch_series_page(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch(
                "agrobr.conab.serie_historica.client.httpx.AsyncClient", return_value=mock_client
            ),
            pytest.raises(SourceUnavailableError, match="conab_serie_historica"),
        ):
            await client.fetch_series_page("graos")


class TestConabSerieHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_raises(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch(
                "agrobr.conab.serie_historica.client.httpx.AsyncClient", return_value=mock_client
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await client.download_xls("soja")

    @pytest.mark.asyncio
    async def test_http_404_raises(self):
        resp_404 = _mock_response(404)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_404)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch(
                "agrobr.conab.serie_historica.client.httpx.AsyncClient", return_value=mock_client
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await client.download_xls("soja")

    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="conab_serie_historica.client não implementa retry — sem tratamento 429"
    )
    async def test_http_429_no_retry(self):
        resp_429 = _mock_response(429)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_429)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "agrobr.conab.serie_historica.client.httpx.AsyncClient", return_value=mock_client
        ):
            result, _ = await client.download_xls("soja")
            assert result is not None


class TestConabSerieEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_content_returns_bytesio(self):
        resp = _mock_response(200, content=b"")
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "agrobr.conab.serie_historica.client.httpx.AsyncClient", return_value=mock_client
        ):
            result, metadata = await client.download_xls("soja")

        assert result.read() == b""
        assert metadata["produto"] == "soja"


class TestConabSerieProductRegistry:
    def test_get_xls_url_valid_product(self):
        url = client.get_xls_url("soja")
        assert "sojaseriehist.xls" in url

    def test_get_xls_url_invalid_product(self):
        with pytest.raises(SourceUnavailableError, match="nao encontrado"):
            client.get_xls_url("banana")

    def test_list_produtos_returns_all(self):
        produtos = client.list_produtos()
        assert len(produtos) > 0
        names = [p["produto"] for p in produtos]
        assert "soja" in names
        assert "milho" in names

    def test_parse_xls_links_empty_html(self):
        links = client.parse_xls_links_from_html("")
        assert links == []
