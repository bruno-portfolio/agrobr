"""Testes de resiliência HTTP para agrobr.noticias_agricolas.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.noticias_agricolas import client


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


class TestNaTimeout:
    @pytest.mark.asyncio
    async def test_timeout_propagates_as_source_unavailable(self):
        with patch(
            "agrobr.noticias_agricolas.client.retry_async", new_callable=AsyncMock
        ) as mock_retry:
            mock_retry.side_effect = httpx.TimeoutException("timeout")
            with pytest.raises(SourceUnavailableError, match="noticias_agricolas"):
                await client.fetch_indicador_page("soja")


class TestNaHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_propagates_as_source_unavailable(self):
        with patch(
            "agrobr.noticias_agricolas.client.retry_async", new_callable=AsyncMock
        ) as mock_retry:
            mock_retry.side_effect = httpx.HTTPStatusError(
                "500", request=MagicMock(), response=MagicMock(status_code=500)
            )
            with pytest.raises(SourceUnavailableError, match="noticias_agricolas"):
                await client.fetch_indicador_page("soja")

    @pytest.mark.asyncio
    async def test_http_403_propagates(self):
        with patch(
            "agrobr.noticias_agricolas.client.retry_async", new_callable=AsyncMock
        ) as mock_retry:
            mock_retry.side_effect = httpx.HTTPStatusError(
                "403", request=MagicMock(), response=MagicMock(status_code=403)
            )
            with pytest.raises(SourceUnavailableError):
                await client.fetch_indicador_page("soja")

    def test_invalid_produto_raises_value_error(self):
        with pytest.raises(ValueError, match="não disponível"):
            client._get_produto_url("produto_inexistente")


class TestNaEncoding:
    @pytest.mark.asyncio
    async def test_encoding_fallback_charset_wrong(self):
        iso_content = "Cotação soja".encode("iso-8859-1")
        resp = _mock_response(200, content=iso_content, charset="utf-8")
        decoded_html = "<html><table>Cotação soja</table></html>"

        with patch(
            "agrobr.noticias_agricolas.client.retry_async", new_callable=AsyncMock
        ) as mock_retry:
            mock_retry.return_value = resp
            with patch("agrobr.noticias_agricolas.client.decode_content") as mock_decode:
                mock_decode.return_value = (decoded_html, "iso-8859-1")
                result = await client.fetch_indicador_page("soja")

        assert "Cotação" in result
        mock_decode.assert_called_once_with(
            iso_content, declared_encoding="utf-8", source="noticias_agricolas"
        )

    @pytest.mark.asyncio
    async def test_no_charset_header(self):
        content = "Preço café".encode("iso-8859-1")
        resp = _mock_response(200, content=content, charset=None)
        decoded_html = "<html><table>Preço café</table></html>"

        with patch(
            "agrobr.noticias_agricolas.client.retry_async", new_callable=AsyncMock
        ) as mock_retry:
            mock_retry.return_value = resp
            with patch("agrobr.noticias_agricolas.client.decode_content") as mock_decode:
                mock_decode.return_value = (decoded_html, "iso-8859-1")
                await client.fetch_indicador_page("soja")

        mock_decode.assert_called_once_with(
            content, declared_encoding=None, source="noticias_agricolas"
        )


class TestNaEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_body_raises_soft_block(self):
        resp = _mock_response(200, content=b"", charset="utf-8")

        with patch(
            "agrobr.noticias_agricolas.client.retry_async", new_callable=AsyncMock
        ) as mock_retry:
            mock_retry.return_value = resp
            with patch("agrobr.noticias_agricolas.client.decode_content") as mock_decode:
                mock_decode.return_value = ("", "utf-8")
                with pytest.raises(SourceUnavailableError, match="Soft block"):
                    await client.fetch_indicador_page("soja")


class TestNaContentValidation:
    def test_small_page_without_table_raises(self):
        """Soft block: small HTML without <table> should raise SourceUnavailableError."""
        small_html = "<html><body><p>Please verify you are human</p></body></html>"
        assert len(small_html) < 20_000

        with pytest.raises(SourceUnavailableError, match="Soft block"):
            client._validate_html_has_data(
                small_html, "https://www.noticiasagricolas.com.br/cotacoes/soja"
            )

    def test_small_page_with_table_passes(self):
        """Small HTML with a <table> tag is valid (sparse data)."""
        small_html = "<html><body><table><tr><td>data</td></tr></table></body></html>"
        assert len(small_html) < 20_000

        # Should not raise
        client._validate_html_has_data(
            small_html, "https://www.noticiasagricolas.com.br/cotacoes/soja"
        )

    def test_large_page_always_passes(self):
        """Large HTML always passes regardless of content."""
        large_html = "<html><body>" + "x" * 25_000 + "</body></html>"
        assert len(large_html) >= 20_000

        # Should not raise even without <table>
        client._validate_html_has_data(
            large_html, "https://www.noticiasagricolas.com.br/cotacoes/soja"
        )


class TestNaRetry:
    @pytest.mark.asyncio
    async def test_retry_async_called_for_fetch(self):
        resp = _mock_response(200, content=b"<html><table>data</table></html>")

        with patch(
            "agrobr.noticias_agricolas.client.retry_async", new_callable=AsyncMock
        ) as mock_retry:
            mock_retry.return_value = resp
            with patch("agrobr.noticias_agricolas.client.decode_content") as mock_decode:
                mock_decode.return_value = ("<html><table>data</table></html>", "utf-8")
                await client.fetch_indicador_page("soja")

        mock_retry.assert_called_once()
