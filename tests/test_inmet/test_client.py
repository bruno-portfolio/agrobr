"""Testes de resiliência HTTP para agrobr.inmet.client."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.inmet import client


def _mock_response(status_code: int = 200, json_data: list | dict | None = None) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else []
    resp.headers = {}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    return resp


class TestInmetTimeout:
    @pytest.mark.asyncio
    async def test_timeout_on_get_json(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.TimeoutException),
        ):
            await client._get_json("/estacoes/T")

    @pytest.mark.asyncio
    async def test_timeout_on_fetch_estacoes(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.TimeoutException),
        ):
            await client.fetch_estacoes("T")


class TestInmetHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_raises(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="inmet"),
        ):
            await client._get_json("/estacoes/T")

    @pytest.mark.asyncio
    async def test_http_403_raises(self):
        resp_403 = _mock_response(403)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_403)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client._get_json("/estacoes/T")

    @pytest.mark.asyncio
    async def test_http_429_raises_after_retries(self):
        resp_429 = _mock_response(429)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_429)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="inmet"),
        ):
            await client._get_json("/estacoes/T")

    @pytest.mark.asyncio
    async def test_retriable_status_in_fetch_dados_logged_and_skipped(self):
        resp_ok = _mock_response(200, [{"data": "d1"}])
        resp_502 = _mock_response(502)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[resp_502, resp_ok])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.fetch_dados_estacao("A001", date(2024, 1, 1), date(2024, 1, 2))

        assert isinstance(result, list)


class TestInmetEmptyResponse:
    @pytest.mark.asyncio
    async def test_non_list_response_returns_empty(self):
        resp = _mock_response(200, {"error": "unexpected"})
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._get_json("/test")

        assert result == []

    @pytest.mark.asyncio
    async def test_empty_list_response(self):
        resp = _mock_response(200, [])
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._get_json("/test")

        assert result == []


class TestInmetValidation:
    @pytest.mark.asyncio
    async def test_invalid_tipo_raises(self):
        with pytest.raises(ValueError, match="Tipo deve ser"):
            await client.fetch_estacoes("X")

    @pytest.mark.asyncio
    async def test_inicio_after_fim_raises(self):
        with pytest.raises(ValueError, match="inicio.*deve ser"):
            await client.fetch_dados_estacao("A001", date(2024, 12, 31), date(2024, 1, 1))


class TestInmetFetchDadosEstacaoChunking:
    @pytest.mark.asyncio
    async def test_chunking_respects_max_days(self):
        resp = _mock_response(200, [{"d": "1"}])
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.fetch_dados_estacao("A001", date(2022, 1, 1), date(2024, 1, 1))

        assert mock_client.get.call_count >= 2
        assert isinstance(result, list)
