"""Testes para agrobr.alt.mapa_psr.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.alt.mapa_psr import client

FAKE_CSV_BYTES = b"ANO_APOLICE;SG_UF_PROPRIEDADE;NM_CULTURA_GLOBAL\n" + b"2023;MT;SOJA\n" * 10


def _mock_response(status_code=200, content=FAKE_CSV_BYTES):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.content = content
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"HTTP {status_code}",
            request=MagicMock(),
            response=resp,
        )
    return resp


class TestDownloadCsv:
    @pytest.mark.asyncio
    @patch("agrobr.alt.mapa_psr.client.retry_on_status", new_callable=AsyncMock)
    async def test_download_ok(self, mock_retry):
        mock_retry.return_value = _mock_response(200, FAKE_CSV_BYTES)
        result = await client.download_csv("https://example.com/test.csv")
        assert result == FAKE_CSV_BYTES

    @pytest.mark.asyncio
    @patch("agrobr.alt.mapa_psr.client.retry_on_status", new_callable=AsyncMock)
    async def test_download_404_raises(self, mock_retry):
        mock_retry.return_value = _mock_response(404)
        with pytest.raises(httpx.HTTPStatusError):
            await client.download_csv("https://example.com/notfound.csv")

    @pytest.mark.asyncio
    @patch("agrobr.alt.mapa_psr.client.retry_on_status", new_callable=AsyncMock)
    async def test_download_500_raises(self, mock_retry):
        mock_retry.return_value = _mock_response(500)
        with pytest.raises(httpx.HTTPStatusError):
            await client.download_csv("https://example.com/error.csv")

    @pytest.mark.asyncio
    @patch("agrobr.alt.mapa_psr.client.retry_on_status", new_callable=AsyncMock)
    async def test_download_retorna_bytes(self, mock_retry):
        content = b"header;col2\n" + b"row1;val1\n" * 15
        mock_retry.return_value = _mock_response(200, content)
        result = await client.download_csv("https://example.com/data.csv")
        assert isinstance(result, bytes)
        assert len(result) > 0


class TestFetchPeriodo:
    @pytest.mark.asyncio
    @patch("agrobr.alt.mapa_psr.client.retry_on_status", new_callable=AsyncMock)
    async def test_fetch_periodo_valido(self, mock_retry):
        mock_retry.return_value = _mock_response(200, FAKE_CSV_BYTES)
        result = await client.fetch_periodo("2025")
        assert result == FAKE_CSV_BYTES

    @pytest.mark.asyncio
    async def test_fetch_periodo_invalido(self):
        with pytest.raises(ValueError, match="invalido"):
            await client.fetch_periodo("2030")

    @pytest.mark.asyncio
    @patch("agrobr.alt.mapa_psr.client.retry_on_status", new_callable=AsyncMock)
    async def test_fetch_periodo_url_correta(self, mock_retry):
        mock_retry.return_value = _mock_response(200, FAKE_CSV_BYTES)
        await client.fetch_periodo("2025")
        mock_retry.assert_called_once()


class TestFetchPeriodos:
    @pytest.mark.asyncio
    @patch("agrobr.alt.mapa_psr.client.retry_on_status", new_callable=AsyncMock)
    async def test_fetch_multiplos(self, mock_retry):
        mock_retry.return_value = _mock_response(200, FAKE_CSV_BYTES)
        result = await client.fetch_periodos(["2016-2024", "2025"])
        assert len(result) == 2
        assert all(isinstance(r, bytes) for r in result)

    @pytest.mark.asyncio
    async def test_fetch_vazio(self):
        result = await client.fetch_periodos([])
        assert result == []

    @pytest.mark.asyncio
    @patch("agrobr.alt.mapa_psr.client.retry_on_status", new_callable=AsyncMock)
    async def test_fetch_unico(self, mock_retry):
        mock_retry.return_value = _mock_response(200, FAKE_CSV_BYTES)
        result = await client.fetch_periodos(["2025"])
        assert len(result) == 1


class TestConfig:
    def test_timeout_read_alta(self):
        assert client.TIMEOUT.read == 180.0

    def test_headers_user_agent(self):
        from agrobr.http.user_agents import UserAgentRotator

        headers = UserAgentRotator.get_headers(source="mapa_psr")
        assert "Mozilla" in headers["User-Agent"]
