"""Testes de resiliência HTTP para agrobr.nasa_power.client."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.nasa_power import client
from tests.helpers import RETRY_SLEEP, make_mock_async_client, make_mock_response


class TestNasaPowerTimeout:
    @pytest.mark.asyncio
    async def test_timeout_on_get_json(self):
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")

        with (
            patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._get_json({"parameters": "T2M", "format": "JSON"})

    @pytest.mark.asyncio
    async def test_timeout_on_fetch_daily(self):
        with (
            patch("agrobr.nasa_power.client._get_json", new_callable=AsyncMock) as mock_get,
            pytest.raises(httpx.TimeoutException),
        ):
            mock_get.side_effect = httpx.TimeoutException("timeout")
            await client.fetch_daily(-15.0, -47.0, date(2024, 1, 1), date(2024, 1, 10))


class TestNasaPowerHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_raises(self):
        resp_500 = make_mock_response(500, json_data={})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        with (
            patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="nasa_power"),
        ):
            await client._get_json({"test": "1"})

    @pytest.mark.asyncio
    async def test_http_403_raises(self):
        resp_403 = make_mock_response(403, json_data={})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_403)

        with (
            patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError),
        ):
            await client._get_json({"test": "1"})

    @pytest.mark.asyncio
    async def test_http_429_raises_after_retries(self):
        resp_429 = make_mock_response(429, json_data={})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_429)

        with (
            patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="nasa_power"),
        ):
            await client._get_json({"test": "1"})


class TestNasaPowerEmptyResponse:
    @pytest.mark.asyncio
    async def test_non_dict_response_returns_empty(self):
        resp = make_mock_response(200, json_data="not a dict")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._get_json({"test": "1"})

        assert result == {}

    @pytest.mark.asyncio
    async def test_empty_dict_response(self):
        resp = make_mock_response(200, json_data={})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._get_json({"test": "1"})

        assert result == {}


class TestNasaPowerValidation:
    @pytest.mark.asyncio
    async def test_start_after_end_raises(self):
        with pytest.raises(InvalidParameterError, match="start.*deve ser"):
            await client.fetch_daily(-15.0, -47.0, date(2024, 12, 31), date(2024, 1, 1))


class TestNasaPowerChunking:
    @pytest.mark.asyncio
    async def test_short_range_single_request(self):
        data = {"properties": {"parameter": {"T2M": {"20240101": 25.0}}}}
        with patch("agrobr.nasa_power.client._get_json", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = data
            result = await client.fetch_daily(-15.0, -47.0, date(2024, 1, 1), date(2024, 1, 10))

        mock_get.assert_called_once()
        assert result == data

    @pytest.mark.asyncio
    async def test_long_range_multiple_chunks(self):
        chunk_data = {"properties": {"parameter": {"T2M": {"20240101": 25.0}}}}

        with patch("agrobr.nasa_power.client._get_json", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = chunk_data
            await client.fetch_daily(-15.0, -47.0, date(2022, 1, 1), date(2024, 1, 1))

        assert mock_get.call_count >= 2

    @pytest.mark.asyncio
    async def test_retriable_chunk_skipped(self):
        from agrobr.exceptions import SourceUnavailableError

        chunk_ok = {"properties": {"parameter": {"T2M": {"20240101": 25.0}}}}

        call_count = 0

        async def side_effect(_params, **_kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise SourceUnavailableError(
                    source="nasa_power", last_error="HTTP 502 after 3 retries"
                )
            return chunk_ok

        with patch("agrobr.nasa_power.client._get_json", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = side_effect
            result = await client.fetch_daily(-15.0, -47.0, date(2023, 1, 1), date(2024, 12, 31))

        assert isinstance(result, dict)
