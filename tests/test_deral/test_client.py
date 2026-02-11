"""Testes de resiliência HTTP para agrobr.deral.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.deral import client
from agrobr.exceptions import SourceUnavailableError


def _mock_response(status_code: int = 200, content: bytes = b"xls-data") -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.content = content
    resp.headers = {}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    return resp


class TestDeralTimeout:
    @pytest.mark.asyncio
    async def test_timeout_retries_then_fails(self):
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("connect timeout")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.deral.client.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError, match="Falhou após"),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

        assert mock_client.get.call_count == client.MAX_RETRIES

    @pytest.mark.asyncio
    async def test_timeout_succeeds_after_retries(self):
        ok_resp = _mock_response(200, b"valid-xls")
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[httpx.TimeoutException("t"), ok_resp])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.deral.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

        assert result == b"valid-xls"


class TestDeralHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_404_raises_immediately(self):
        resp_404 = _mock_response(404)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_404)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="404"),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_http_500_retries(self):
        resp_500 = _mock_response(500)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_500)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.deral.client.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError, match="HTTP 500"),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

    @pytest.mark.asyncio
    async def test_http_429_retries_then_succeeds(self):
        resp_429 = _mock_response(429)
        resp_ok = _mock_response(200, b"data")
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=[resp_429, resp_ok])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.deral.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

        assert result == b"data"

    @pytest.mark.asyncio
    async def test_http_403_raises_via_raise_for_status(self):
        resp_403 = _mock_response(403)
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp_403)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.deral.client.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")


class TestDeralEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_content_returns_empty_bytes(self):
        resp = _mock_response(200, b"")
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

        assert result == b""


class TestDeralRetryBackoff:
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
            patch("agrobr.deral.client.httpx.AsyncClient", return_value=mock_client),
            patch("agrobr.deral.client.asyncio.sleep", side_effect=track_sleep),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_bytes("https://test.pr.gov.br/PC.xls")

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

    @pytest.mark.asyncio
    async def test_fetch_pss_xlsx_calls_fetch_bytes(self):
        with patch("agrobr.deral.client._fetch_bytes", new_callable=AsyncMock) as mock:
            mock.return_value = b"xlsx"
            result = await client.fetch_pss_xlsx()
            assert result == b"xlsx"
            assert "pss.xlsx" in mock.call_args[0][0]
