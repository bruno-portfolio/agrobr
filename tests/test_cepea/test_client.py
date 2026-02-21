"""Testes de resiliência HTTP para agrobr.cepea.client."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.cepea import client
from agrobr.cepea.client import FetchResult
from agrobr.exceptions import SourceUnavailableError


def _mock_response(
    status_code: int = 200, content: bytes = b"<html>ok</html>", charset: str | None = "utf-8"
) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.content = content
    resp.charset_encoding = charset
    resp.headers = {"content-type": f"text/html; charset={charset}" if charset else "text/html"}
    resp.request = MagicMock()
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=resp.request, response=resp
        )
    return resp


@pytest.fixture(autouse=True)
def _reset_circuit_breaker():
    client._httpx_circuit_open = False
    client._httpx_circuit_opened_at = 0.0
    client._use_browser = False
    client._use_alternative_source = False
    yield
    client._httpx_circuit_open = False
    client._httpx_circuit_opened_at = 0.0
    client._use_browser = False
    client._use_alternative_source = True


class TestCepeaTimeout:
    @pytest.mark.asyncio
    async def test_timeout_propagates_via_retry_async(self):
        with patch("agrobr.cepea.client.retry_async", new_callable=AsyncMock) as mock_retry:
            mock_retry.side_effect = httpx.TimeoutException("timeout")
            with pytest.raises(SourceUnavailableError):
                await client.fetch_indicador_page("soja")

    @pytest.mark.asyncio
    async def test_timeout_on_series_historica(self):
        with patch("agrobr.cepea.client.retry_async", new_callable=AsyncMock) as mock_retry:
            mock_retry.side_effect = httpx.TimeoutException("timeout")
            with pytest.raises(SourceUnavailableError, match="cepea"):
                await client.fetch_series_historica("soja")


class TestCepeaHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_403_opens_circuit_breaker(self):
        with patch("agrobr.cepea.client.retry_async", new_callable=AsyncMock) as mock_retry:
            mock_retry.side_effect = httpx.HTTPStatusError(
                "403 Forbidden", request=MagicMock(), response=MagicMock(status_code=403)
            )
            with pytest.raises(SourceUnavailableError):
                await client.fetch_indicador_page("soja")

        assert client._httpx_circuit_open is True

    @pytest.mark.asyncio
    async def test_http_500_does_not_open_circuit(self):
        with patch("agrobr.cepea.client.retry_async", new_callable=AsyncMock) as mock_retry:
            mock_retry.side_effect = httpx.HTTPStatusError(
                "500 Internal", request=MagicMock(), response=MagicMock(status_code=500)
            )
            with pytest.raises(SourceUnavailableError):
                await client.fetch_indicador_page("soja")

        assert client._httpx_circuit_open is False


class TestCepeaCircuitBreaker:
    def test_circuit_closed_by_default(self):
        assert client._is_circuit_open() is False

    def test_circuit_opens_on_cloudflare(self):
        client._open_circuit()
        assert client._is_circuit_open() is True

    def test_circuit_resets_after_timeout(self):
        client._open_circuit()
        client._httpx_circuit_opened_at = time.monotonic() - client._CIRCUIT_RESET_SECONDS - 1
        assert client._is_circuit_open() is False

    @pytest.mark.asyncio
    async def test_circuit_open_skips_httpx(self):
        client._open_circuit()

        with patch("agrobr.cepea.client._fetch_with_httpx", new_callable=AsyncMock) as mock_httpx:
            with pytest.raises(SourceUnavailableError):
                await client.fetch_indicador_page("soja")

            mock_httpx.assert_not_called()


class TestCepeaFallbackChain:
    @pytest.mark.asyncio
    async def test_force_alternative_skips_httpx(self):
        with patch(
            "agrobr.cepea.client._fetch_with_alternative_source", new_callable=AsyncMock
        ) as mock_alt:
            mock_alt.return_value = FetchResult("<html>NA data</html>", "noticias_agricolas")
            result = await client.fetch_indicador_page("soja", force_alternative=True)

        assert result.html == "<html>NA data</html>"
        assert result.source == "noticias_agricolas"

    @pytest.mark.asyncio
    async def test_httpx_fail_falls_to_browser(self):
        client._use_browser = True
        client._use_alternative_source = False

        with patch("agrobr.cepea.client._fetch_with_httpx", new_callable=AsyncMock) as mock_httpx:
            mock_httpx.side_effect = httpx.HTTPError("failed")
            with patch(
                "agrobr.cepea.client._fetch_with_browser", new_callable=AsyncMock
            ) as mock_browser:
                mock_browser.return_value = FetchResult("<html>browser</html>", "browser")
                result = await client.fetch_indicador_page("soja")

        assert result.html == "<html>browser</html>"
        assert result.source == "browser"

    @pytest.mark.asyncio
    async def test_all_methods_fail_raises(self):
        client._use_browser = True
        client._use_alternative_source = True

        with patch("agrobr.cepea.client._fetch_with_httpx", new_callable=AsyncMock) as mock_httpx:
            mock_httpx.side_effect = httpx.HTTPError("h")
            with patch(
                "agrobr.cepea.client._fetch_with_browser", new_callable=AsyncMock
            ) as mock_browser:
                mock_browser.side_effect = Exception("b")
                with patch(
                    "agrobr.cepea.client._fetch_with_alternative_source", new_callable=AsyncMock
                ) as mock_alt:
                    mock_alt.side_effect = Exception("a")
                    with pytest.raises(SourceUnavailableError, match="All fetch methods"):
                        await client.fetch_indicador_page("soja")


class TestCepeaEncoding:
    @pytest.mark.asyncio
    async def test_encoding_handled_by_decode_content(self):
        iso_content = "Preço médio café".encode("iso-8859-1")
        resp = _mock_response(200, content=iso_content, charset="utf-8")

        with patch("agrobr.cepea.client.retry_async", new_callable=AsyncMock) as mock_retry:
            mock_retry.return_value = resp
            with patch("agrobr.cepea.client.decode_content") as mock_decode:
                mock_decode.return_value = ("Preço médio café", "iso-8859-1")
                result = await client._fetch_with_httpx("https://test.cepea.esalq.usp.br", {})

        assert result.html == "Preço médio café"
        assert result.source == "cepea"
        mock_decode.assert_called_once_with(iso_content, declared_encoding="utf-8", source="cepea")

    @pytest.mark.asyncio
    async def test_no_charset_header(self):
        content = "Produção agrícola".encode("iso-8859-1")
        resp = _mock_response(200, content=content, charset=None)

        with patch("agrobr.cepea.client.retry_async", new_callable=AsyncMock) as mock_retry:
            mock_retry.return_value = resp
            with patch("agrobr.cepea.client.decode_content") as mock_decode:
                mock_decode.return_value = ("Produção agrícola", "iso-8859-1")
                result = await client._fetch_with_httpx("https://test.cepea.esalq.usp.br", {})

        assert "Produção" in result.html
        mock_decode.assert_called_once_with(content, declared_encoding=None, source="cepea")


class TestCepeaEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_body_handled(self):
        resp = _mock_response(200, content=b"", charset="utf-8")

        with patch("agrobr.cepea.client.retry_async", new_callable=AsyncMock) as mock_retry:
            mock_retry.return_value = resp
            with patch("agrobr.cepea.client.decode_content") as mock_decode:
                mock_decode.return_value = ("", "utf-8")
                result = await client._fetch_with_httpx("https://test.cepea.esalq.usp.br", {})

        assert result.html == ""
        assert result.source == "cepea"


class TestCepeaSetters:
    def test_set_use_browser(self):
        client.set_use_browser(True)
        assert client._use_browser is True
        client.set_use_browser(False)
        assert client._use_browser is False

    def test_set_use_alternative_source(self):
        client.set_use_alternative_source(True)
        assert client._use_alternative_source is True
        client.set_use_alternative_source(False)
        assert client._use_alternative_source is False
