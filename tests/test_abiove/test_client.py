"""Testes de resiliência HTTP para agrobr.abiove.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.abiove import client
from agrobr.exceptions import SourceUnavailableError

RETRY_SLEEP = "agrobr.http.retry.asyncio.sleep"


def _mock_response(
    status_code: int = 200, content: bytes = b"xlsx-data", headers: dict | None = None
) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.content = content
    resp.headers = headers or {}
    resp.url = "https://abiove.org.br/test.xlsx"
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    return resp


class TestAbioveTimeout:
    @pytest.mark.asyncio
    async def test_timeout_propagates_immediately(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("connect timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.abiove.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.TimeoutException),
        ):
            await client._fetch_url("https://abiove.org.br/test.xlsx")

        assert mock_client.get.call_count == 1


class TestAbioveHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_retries_then_fails(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.abiove.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_url("https://abiove.org.br/test.xlsx")

        assert mock_client.get.call_count > 1

    @pytest.mark.asyncio
    async def test_http_404_raises_immediately(self):
        resp_404 = _mock_response(404)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_404)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.abiove.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="HTTP 404"),
        ):
            await client._fetch_url("https://abiove.org.br/test.xlsx")

        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_http_403_raises_via_raise_for_status(self):
        resp_403 = _mock_response(403)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_403)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.abiove.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client._fetch_url("https://abiove.org.br/test.xlsx")

    @pytest.mark.asyncio
    async def test_http_429_retries(self):
        resp_429 = _mock_response(429)
        resp_ok = _mock_response(200, b"ok")
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[resp_429, resp_429, resp_ok])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.abiove.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await client._fetch_url("https://abiove.org.br/test.xlsx")

        assert result == b"ok"


class TestAbioveEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_body_returns_empty_bytes(self):
        resp = _mock_response(200, b"")
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.abiove.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._fetch_url("https://abiove.org.br/test.xlsx")

        assert result == b""


class TestAbioveRetry:
    @pytest.mark.asyncio
    async def test_retry_backoff_exponential(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        sleep_calls: list[float] = []

        async def track_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        with (
            patch("agrobr.abiove.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, side_effect=track_sleep),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_url("https://abiove.org.br/test.xlsx")

        assert len(sleep_calls) >= 2
        for i in range(1, len(sleep_calls)):
            assert sleep_calls[i] > sleep_calls[i - 1]


class TestAbioveConnectError:
    @pytest.mark.asyncio
    async def test_connect_error_propagates_immediately(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.ConnectError("connection refused")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.abiove.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.ConnectError),
        ):
            await client._fetch_url("https://abiove.org.br/test.xlsx")

        assert mock_client.get.call_count == 1


class TestFetchExportacaoExcel:
    @pytest.mark.asyncio
    async def test_specific_month_404_raises(self):
        with (
            patch("agrobr.abiove.client._fetch_url", new_callable=AsyncMock) as mock_fetch,
            pytest.raises(SourceUnavailableError),
        ):
            mock_fetch.side_effect = SourceUnavailableError(
                source="abiove", url="test", last_error="HTTP 404"
            )
            await client.fetch_exportacao_excel(2024, mes=6)

    @pytest.mark.asyncio
    async def test_scans_months_backwards(self):
        with patch("agrobr.abiove.client._fetch_url", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = [
                SourceUnavailableError(source="abiove", url="t", last_error="404"),
                SourceUnavailableError(source="abiove", url="t", last_error="404"),
                b"found",
            ]
            data, url = await client.fetch_exportacao_excel(2024)
            assert data == b"found"
            assert mock_fetch.call_count == 3
