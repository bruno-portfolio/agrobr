"""Testes de resiliência HTTP para agrobr.usda.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.usda import client


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


class TestUsdaApiKey:
    def test_missing_api_key_raises(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            pytest.raises(SourceUnavailableError, match="API key não configurada"),
        ):
            client._get_api_key(None)

    def test_explicit_api_key_used(self):
        key = client._get_api_key("my-key")
        assert key == "my-key"

    def test_env_var_api_key_used(self):
        with patch.dict("os.environ", {"AGROBR_USDA_API_KEY": "env-key"}):
            key = client._get_api_key(None)
            assert key == "env-key"


class TestUsdaTimeout:
    @pytest.mark.asyncio
    async def test_timeout_retries_then_fails(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.usda.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.usda.client.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError, match="Falhou após"),
        ):
            await client._fetch_json("https://test.usda.gov/api", "key123")

        assert mock_client.get.call_count == client.MAX_RETRIES

    @pytest.mark.asyncio
    async def test_timeout_succeeds_after_failures(self):
        ok_resp = _mock_response(200, [{"id": 1}])
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[httpx.TimeoutException("t"), ok_resp])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.usda.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.usda.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await client._fetch_json("https://test.usda.gov/api", "key123")

        assert result == [{"id": 1}]


class TestUsdaHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_401_raises_immediately(self):
        resp_401 = _mock_response(401)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_401)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.usda.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="API key inválida"),
        ):
            await client._fetch_json("https://test.usda.gov/api", "bad-key")

        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_http_404_returns_empty_list(self):
        resp_404 = _mock_response(404)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_404)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.usda.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._fetch_json("https://test.usda.gov/api", "key")

        assert result == []

    @pytest.mark.asyncio
    async def test_http_500_retries(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.usda.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.usda.client.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError, match="HTTP 500"),
        ):
            await client._fetch_json("https://test.usda.gov/api", "key")

    @pytest.mark.asyncio
    async def test_http_429_retries_then_succeeds(self):
        resp_429 = _mock_response(429)
        resp_ok = _mock_response(200, [{"id": 1}])
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[resp_429, resp_ok])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.usda.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.usda.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await client._fetch_json("https://test.usda.gov/api", "key")

        assert result == [{"id": 1}]

    @pytest.mark.asyncio
    async def test_http_403_raises_via_raise_for_status(self):
        resp_403 = _mock_response(403)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_403)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.usda.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.usda.client.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_json("https://test.usda.gov/api", "key")


class TestUsdaEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_list_response(self):
        resp = _mock_response(200, [])
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.usda.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._fetch_json("https://test.usda.gov/api", "key")

        assert result == []

    @pytest.mark.asyncio
    async def test_non_list_response_returns_empty(self):
        resp = _mock_response(200, {"error": "unexpected"})
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.usda.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._fetch_json("https://test.usda.gov/api", "key")

        assert result == []


class TestUsdaRetryBackoff:
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
            patch("agrobr.usda.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.usda.client.asyncio.sleep", side_effect=track_sleep),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_json("https://test.usda.gov/api", "key")

        for i in range(1, len(sleep_calls)):
            assert sleep_calls[i] > sleep_calls[i - 1]
