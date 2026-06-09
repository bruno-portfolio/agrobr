from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.bcb import sgs_client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import (
    RETRY_SLEEP,
    make_mock_async_client,
    make_mock_response,
    make_sleep_tracker,
)

SGS_SAMPLE = [
    {"data": "02/01/2026", "valor": "12.15"},
    {"data": "03/01/2026", "valor": "12.15"},
]


class TestSgsUrlConstruction:
    @pytest.mark.asyncio
    async def test_url_without_dates(self):
        resp = make_mock_response(200, json_data=SGS_SAMPLE)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.bcb.sgs_client.httpx.AsyncClient", return_value=mock_client):
            data, url = await sgs_client.fetch_sgs(432)

        call_args = mock_client.get.call_args
        assert "432" in call_args[0][0]
        params = call_args[1]["params"]
        assert params["formato"] == "json"
        assert "dataInicial" not in params
        assert "dataFinal" not in params

    @pytest.mark.asyncio
    async def test_url_with_dates(self):
        resp = make_mock_response(200, json_data=SGS_SAMPLE)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.bcb.sgs_client.httpx.AsyncClient", return_value=mock_client):
            await sgs_client.fetch_sgs(432, data_inicial="01/01/2026", data_final="31/01/2026")

        params = mock_client.get.call_args[1]["params"]
        assert params["dataInicial"] == "01/01/2026"
        assert params["dataFinal"] == "31/01/2026"

    @pytest.mark.asyncio
    async def test_url_contains_sgs_base(self):
        resp = make_mock_response(200, json_data=SGS_SAMPLE)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.bcb.sgs_client.httpx.AsyncClient", return_value=mock_client):
            await sgs_client.fetch_sgs(1)

        url = mock_client.get.call_args[0][0]
        assert "bcdata.sgs.1/dados" in url

    @pytest.mark.asyncio
    async def test_url_with_ultimos_uses_ultimos_endpoint(self):
        resp = make_mock_response(200, json_data=SGS_SAMPLE)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.bcb.sgs_client.httpx.AsyncClient", return_value=mock_client):
            await sgs_client.fetch_sgs(432, ultimos=12)

        url = mock_client.get.call_args[0][0]
        assert "bcdata.sgs.432/dados/ultimos/12" in url
        params = mock_client.get.call_args[1]["params"]
        assert "dataInicial" not in params
        assert "dataFinal" not in params

    @pytest.mark.asyncio
    async def test_url_ultimos_with_dates_falls_back_to_range(self):
        resp = make_mock_response(200, json_data=SGS_SAMPLE)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.bcb.sgs_client.httpx.AsyncClient", return_value=mock_client):
            await sgs_client.fetch_sgs(432, data_inicial="01/01/2026", ultimos=12)

        url = mock_client.get.call_args[0][0]
        assert "/dados/ultimos/" not in url
        params = mock_client.get.call_args[1]["params"]
        assert params["dataInicial"] == "01/01/2026"


class TestSgsRetry:
    @pytest.mark.asyncio
    async def test_timeout_retried_raises_source_unavailable(self):
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.TimeoutException("read timeout")

        with (
            patch("agrobr.bcb.sgs_client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await sgs_client.fetch_sgs(432)

    @pytest.mark.asyncio
    async def test_http_500_retried(self):
        resp_500 = make_mock_response(500, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        sleep_calls, track_sleep = make_sleep_tracker()

        with (
            patch("agrobr.bcb.sgs_client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, side_effect=track_sleep),
            pytest.raises(SourceUnavailableError),
        ):
            await sgs_client.fetch_sgs(432)

        assert len(sleep_calls) > 0


class TestSgsEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_list_raises(self):
        resp = make_mock_response(200, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.bcb.sgs_client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="Resposta vazia"),
        ):
            await sgs_client.fetch_sgs(432)

    @pytest.mark.asyncio
    async def test_non_list_response_raises(self):
        resp = make_mock_response(200, json_data={"error": "not found"})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.bcb.sgs_client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="Resposta vazia"),
        ):
            await sgs_client.fetch_sgs(99999)
