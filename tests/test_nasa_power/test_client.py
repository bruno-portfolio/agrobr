"""Testes de resiliência HTTP para agrobr.nasa_power.client."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.nasa_power import client


def _mock_response(status_code: int = 200, json_data: dict | None = None) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.headers = {}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    return resp


class TestNasaPowerTimeout:
    @pytest.mark.asyncio
    async def test_timeout_on_get_json(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.TimeoutException),
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
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client._get_json({"test": "1"})

    @pytest.mark.asyncio
    async def test_http_403_raises(self):
        resp_403 = _mock_response(403)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_403)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client._get_json({"test": "1"})

    @pytest.mark.asyncio
    @pytest.mark.xfail(reason="nasa_power._get_json não implementa retry — 429 não tratado")
    async def test_http_429_no_retry(self):
        resp_429 = _mock_response(429)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_429)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._get_json({"test": "1"})
            assert isinstance(result, dict)


class TestNasaPowerEmptyResponse:
    @pytest.mark.asyncio
    async def test_non_dict_response_returns_empty(self):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.json.return_value = "not a dict"
        resp.headers = {}
        resp.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._get_json({"test": "1"})

        assert result == {}

    @pytest.mark.asyncio
    async def test_empty_dict_response(self):
        resp = _mock_response(200, {})
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._get_json({"test": "1"})

        assert result == {}


class TestNasaPowerValidation:
    @pytest.mark.asyncio
    async def test_start_after_end_raises(self):
        with pytest.raises(ValueError, match="start.*deve ser"):
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

        with (
            patch("agrobr.nasa_power.client._get_json", new_callable=AsyncMock) as mock_get,
            patch("agrobr.nasa_power.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            mock_get.return_value = chunk_data
            await client.fetch_daily(-15.0, -47.0, date(2022, 1, 1), date(2024, 1, 1))

        assert mock_get.call_count >= 2

    @pytest.mark.asyncio
    async def test_retriable_chunk_skipped(self):
        chunk_ok = {"properties": {"parameter": {"T2M": {"20240101": 25.0}}}}
        resp_502 = _mock_response(502)

        call_count = 0

        async def side_effect(_params):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.HTTPStatusError("502", request=MagicMock(), response=resp_502)
            return chunk_ok

        with (
            patch("agrobr.nasa_power.client._get_json", new_callable=AsyncMock) as mock_get,
            patch("agrobr.nasa_power.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            mock_get.side_effect = side_effect
            result = await client.fetch_daily(-15.0, -47.0, date(2023, 1, 1), date(2024, 12, 31))

        assert isinstance(result, dict)
