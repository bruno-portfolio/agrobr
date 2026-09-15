"""Testes de resiliência HTTP para agrobr.conab.serie_historica.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.conab.serie_historica import client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import RETRY_SLEEP, make_mock_async_client, make_mock_response

_URL = "https://www.gov.br/conab/test"
_HEADERS = {"content-type": "application/vnd.ms-excel"}


def _resp(status_code: int = 200, *, content: bytes = b"xls-data", text: str = "<html></html>"):
    return make_mock_response(
        status_code,
        content=content,
        text=text,
        url=_URL,
        headers=_HEADERS,
    )


class TestConabSerieTimeout:
    @pytest.mark.asyncio
    async def test_timeout_on_download_xls(self):
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")

        with (
            patch(
                "agrobr.conab.serie_historica.client.httpx.AsyncClient", return_value=mock_client
            ),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError, match="conab_serie"),
        ):
            await client.download_xls("soja")


class TestConabSerieHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_raises(self):
        resp_500 = _resp(500)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        with (
            patch(
                "agrobr.conab.serie_historica.client.httpx.AsyncClient", return_value=mock_client
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await client.download_xls("soja")

    @pytest.mark.asyncio
    async def test_http_404_raises(self):
        resp_404 = _resp(404)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_404)

        with (
            patch(
                "agrobr.conab.serie_historica.client.httpx.AsyncClient", return_value=mock_client
            ),
            pytest.raises(SourceUnavailableError),
        ):
            await client.download_xls("soja")

    @pytest.mark.asyncio
    async def test_http_429_raises_after_retries(self):
        resp_429 = _resp(429)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_429)

        with (
            patch(
                "agrobr.conab.serie_historica.client.httpx.AsyncClient", return_value=mock_client
            ),
            pytest.raises(SourceUnavailableError, match="conab_serie"),
        ):
            await client.download_xls("soja")


class TestConabSerieEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_content_returns_bytesio(self):
        resp = _resp(200, content=b"")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

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

    @pytest.mark.parametrize(
        "produto",
        [
            "feijao_caupi",
            "feijao_caupi_1",
            "feijao_caupi_2",
            "feijao_caupi_3",
            "feijao_cores",
            "feijao_cores_1",
            "feijao_cores_2",
            "feijao_cores_3",
            "feijao_preto",
            "feijao_preto_1",
            "feijao_preto_2",
            "feijao_preto_3",
        ],
    )
    def test_list_produtos_contains_feijao_subtypes(self, produto):
        assert produto in {p["produto"] for p in client.list_produtos()}
