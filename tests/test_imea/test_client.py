"""Testes de resiliência HTTP para agrobr.imea.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.imea import client
from tests.helpers import (
    RETRY_SLEEP,
    make_mock_async_client,
    make_mock_response,
    make_sleep_tracker,
)


class TestImeaTimeout:
    @pytest.mark.asyncio
    async def test_timeout_retried_raises_source_unavailable(self):
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_json("https://api1.imea.com.br/test")

        assert mock_client.get.call_count == 3


class TestImeaHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_retries(self):
        resp_500 = make_mock_response(500, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_json("https://api1.imea.com.br/test")

        assert mock_client.get.call_count > 1

    @pytest.mark.asyncio
    async def test_http_429_retries_then_succeeds(self):
        resp_429 = make_mock_response(429, json_data=[])
        resp_ok = make_mock_response(200, json_data=[{"id": 1}])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(side_effect=[resp_429, resp_ok])

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await client._fetch_json("https://api1.imea.com.br/test")

        assert result == [{"id": 1}]

    @pytest.mark.asyncio
    async def test_http_403_raises_via_raise_for_status(self):
        resp_403 = make_mock_response(403, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_403)

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client._fetch_json("https://api1.imea.com.br/test")


class TestImeaEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_list_response(self):
        resp = make_mock_response(200, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._fetch_json("https://api1.imea.com.br/test")

        assert result == []

    @pytest.mark.asyncio
    async def test_non_list_response_raises_source_unavailable(self):
        resp = make_mock_response(200, json_data={"error": "unexpected"})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="JSON inesperado: dict"),
        ):
            await client._fetch_json("https://api1.imea.com.br/test")


class TestImeaRetryBackoff:
    @pytest.mark.asyncio
    async def test_backoff_exponential(self):
        resp_500 = make_mock_response(500, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        sleep_calls, track_sleep = make_sleep_tracker()

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, side_effect=track_sleep),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_json("https://api1.imea.com.br/test")

        assert len(sleep_calls) >= 2
        for i in range(1, len(sleep_calls)):
            assert sleep_calls[i] > sleep_calls[i - 1]


class TestImeaFetchHelpers:
    @pytest.mark.asyncio
    async def test_fetch_cotacoes_builds_correct_url(self):
        with patch("agrobr.imea.client._fetch_json", new_callable=AsyncMock) as mock:
            mock.return_value = [{"id": 1}]
            result = await client.fetch_cotacoes(cadeia_id=4)
            assert result == [{"id": 1}]
            assert "/cadeias/4/cotacoes" in mock.call_args[0][0]
