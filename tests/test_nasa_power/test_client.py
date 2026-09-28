"""Testes de resiliência HTTP para agrobr.nasa_power.client."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.nasa_power import client
from tests.helpers import make_mock_async_client, make_mock_response


class TestNasaPowerTimeout:
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
    async def test_http_403_raises(self):
        resp_403 = make_mock_response(403, content=b"{}")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_403)

        with (
            patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError),
        ):
            await client._get_json({"test": "1"})


class TestNasaPowerEmptyResponse:
    @pytest.mark.asyncio
    async def test_non_dict_response_raises_parse_error(self):
        resp = make_mock_response(200, content=b'"not a dict"')
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.nasa_power.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(ParseError, match="deve ser objeto"),
        ):
            await client._get_json({"test": "1"})


class TestNasaPowerValidation:
    @pytest.mark.asyncio
    async def test_start_after_end_raises(self):
        with pytest.raises(InvalidParameterError, match="start.*deve ser"):
            await client.fetch_daily(-15.0, -47.0, date(2024, 12, 31), date(2024, 1, 1))
