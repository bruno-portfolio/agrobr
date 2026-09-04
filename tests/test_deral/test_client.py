"""Testes de resiliência HTTP para agrobr.deral.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.deral import client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import (
    RETRY_SLEEP,
    make_mock_async_client,
    make_mock_response,
    make_sleep_tracker,
)


class TestDeralTimeout:
    @pytest.mark.asyncio
    async def test_timeout_retried_raises_source_unavailable(self):
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.TimeoutException("connect timeout")

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

        assert mock_client.get.call_count == 3


class TestDeralHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_404_raises_immediately(self):
        resp_404 = make_mock_response(404, content=b"xls-data", url="https://test.pr.gov.br/PC.xls")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_404)

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="404"),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_http_500_retries(self):
        resp_500 = make_mock_response(500, content=b"xls-data", url="https://test.pr.gov.br/PC.xls")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

        assert mock_client.get.call_count > 1

    @pytest.mark.asyncio
    async def test_http_429_retries_then_succeeds(self):
        ok_content = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"x" * 1500
        resp_429 = make_mock_response(429, content=b"xls-data", url="https://test.pr.gov.br/PC.xls")
        resp_ok = make_mock_response(200, content=ok_content, url="https://test.pr.gov.br/PC.xls")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(side_effect=[resp_429, resp_ok])

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

        assert result == ok_content

    @pytest.mark.asyncio
    async def test_http_403_raises_via_raise_for_status(self):
        resp_403 = make_mock_response(403, content=b"xls-data", url="https://test.pr.gov.br/PC.xls")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_403)

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")


class TestDeralEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_content_raises_source_unavailable(self):
        resp = make_mock_response(200, content=b"", url="https://test.pr.gov.br/PC.xls")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="muito pequeno"),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")


class TestDeralRetryBackoff:
    @pytest.mark.asyncio
    async def test_backoff_exponential(self):
        resp_500 = make_mock_response(500, content=b"xls-data", url="https://test.pr.gov.br/PC.xls")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        sleep_calls, track_sleep = make_sleep_tracker()

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, side_effect=track_sleep),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

        assert len(sleep_calls) >= 2
        for i in range(1, len(sleep_calls)):
            assert sleep_calls[i] > sleep_calls[i - 1]


class TestDeralFetchHelpers:
    @pytest.mark.asyncio
    async def test_fetch_pc_xls_calls_fetch_bytes(self):
        with patch("agrobr.deral.client._fetch_bytes", new_callable=AsyncMock) as mock:
            mock.return_value = b"xls"
            result = await client.fetch_pc_xls()
            assert result == b"xls"
            assert "PC.xls" in mock.call_args[0][0]
