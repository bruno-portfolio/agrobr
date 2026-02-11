"""Testes de resiliência HTTP para agrobr.imea.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.imea import client


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


class TestImeaTimeout:
    @pytest.mark.asyncio
    async def test_timeout_retries_then_fails(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.imea.client.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError, match="Falhou após"),
        ):
            await client._fetch_json("https://api1.imea.com.br/test")

        assert mock_client.get.call_count == client.MAX_RETRIES

    @pytest.mark.asyncio
    async def test_timeout_succeeds_after_failures(self):
        ok_resp = _mock_response(200, [{"id": 1}])
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[httpx.TimeoutException("t"), ok_resp])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.imea.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await client._fetch_json("https://api1.imea.com.br/test")

        assert result == [{"id": 1}]


class TestImeaHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_retries(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.imea.client.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError, match="HTTP 500"),
        ):
            await client._fetch_json("https://api1.imea.com.br/test")

    @pytest.mark.asyncio
    async def test_http_429_retries_then_succeeds(self):
        resp_429 = _mock_response(429)
        resp_ok = _mock_response(200, [{"id": 1}])
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[resp_429, resp_ok])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.imea.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await client._fetch_json("https://api1.imea.com.br/test")

        assert result == [{"id": 1}]

    @pytest.mark.asyncio
    async def test_http_403_raises_via_raise_for_status(self):
        resp_403 = _mock_response(403)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_403)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.imea.client.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_json("https://api1.imea.com.br/test")


class TestImeaEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_list_response(self):
        resp = _mock_response(200, [])
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._fetch_json("https://api1.imea.com.br/test")

        assert result == []

    @pytest.mark.asyncio
    async def test_non_list_response_returns_empty(self):
        resp = _mock_response(200, {"error": "unexpected"})
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._fetch_json("https://api1.imea.com.br/test")

        assert result == []


class TestImeaRetryBackoff:
    @pytest.mark.asyncio
    async def test_backoff_exponential(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        sleep_calls: list[float] = []

        async def track_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        with (
            patch("agrobr.imea.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.imea.client.asyncio.sleep", side_effect=track_sleep),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_json("https://api1.imea.com.br/test")

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

    @pytest.mark.asyncio
    async def test_fetch_indicadores_non_list_wraps(self):
        with patch("agrobr.imea.client._fetch_json", new_callable=AsyncMock) as mock:
            mock.return_value = [{"key": "val"}]
            result = await client.fetch_indicadores()
            assert isinstance(result, list)
