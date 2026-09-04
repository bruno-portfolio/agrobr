"""Testes de resiliência HTTP para agrobr.anda.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.anda import client
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from tests.helpers import (
    RETRY_SLEEP,
    make_mock_async_client,
    make_mock_response,
    make_sleep_tracker,
)


class TestAndaTimeout:
    @pytest.mark.asyncio
    async def test_timeout_retried_raises_source_unavailable(self):
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.TimeoutException("read timeout")

        with (
            patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._get_with_retry("https://anda.org.br/test")

        assert mock_client.get.call_count == 3


class TestAndaHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_retries_then_succeeds(self):
        resp_500 = make_mock_response(
            500, text="<html></html>", content=b"data", url="https://anda.org.br/test"
        )
        resp_ok = make_mock_response(
            200, text="<html></html>", content=b"data", url="https://anda.org.br/test"
        )
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(side_effect=[resp_500, resp_500, resp_ok])

        with (
            patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await client._get_with_retry("https://anda.org.br/test")

        assert result.status_code == 200

    @pytest.mark.asyncio
    async def test_http_403_raises_via_raise_for_status(self):
        resp_403 = make_mock_response(
            403, text="<html></html>", content=b"data", url="https://anda.org.br/test"
        )
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_403)

        with (
            patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client._get_with_retry("https://anda.org.br/test")

        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_http_404_raises_via_raise_for_status(self):
        resp_404 = make_mock_response(
            404, text="<html></html>", content=b"data", url="https://anda.org.br/test"
        )
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_404)

        with (
            patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client._get_with_retry("https://anda.org.br/test")

        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_http_429_retries(self):
        resp_429 = make_mock_response(
            429, text="<html></html>", content=b"data", url="https://anda.org.br/test"
        )
        resp_ok = make_mock_response(
            200, text="<html></html>", content=b"data", url="https://anda.org.br/test"
        )
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(side_effect=[resp_429, resp_ok])

        with (
            patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await client._get_with_retry("https://anda.org.br/test")

        assert result.status_code == 200


class TestAndaConnectError:
    @pytest.mark.asyncio
    async def test_connect_error_retried_raises_source_unavailable(self):
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.ConnectError("refused")

        with (
            patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._get_with_retry("https://anda.org.br/test")

        assert mock_client.get.call_count == 3


class TestAndaEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_html_raises_source_unavailable(self):
        resp = make_mock_response(200, text="", content=b"data", url="https://anda.org.br/test")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="muito pequena"),
        ):
            await client.fetch_estatisticas_page()

    def test_parse_links_empty_html(self):
        links = client.parse_links_from_html("")
        assert links == []

    def test_parse_links_malformed_html(self):
        links = client.parse_links_from_html("<html><broken><<<")
        assert links == []


class TestAndaRetryBackoff:
    @pytest.mark.asyncio
    async def test_backoff_exponential(self):
        resp_500 = make_mock_response(
            500, text="<html></html>", content=b"data", url="https://anda.org.br/test"
        )
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        sleep_calls, track_sleep = make_sleep_tracker()

        with (
            patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, side_effect=track_sleep),
            pytest.raises(SourceUnavailableError),
        ):
            await client._get_with_retry("https://anda.org.br/test")

        assert len(sleep_calls) >= 2
        for i in range(1, len(sleep_calls)):
            assert sleep_calls[i] > sleep_calls[i - 1]


class TestFetchEstatisticasPage:
    @pytest.mark.asyncio
    async def test_valid_html_returns_text(self):
        valid_html = "<html><body>" + "<a href='link'>x</a>" * 50 + "</body></html>"
        resp = make_mock_response(200, text=valid_html, content=b"data", url="https://anda.org.br")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.fetch_estatisticas_page()

        assert "<a" in result

    @pytest.mark.asyncio
    async def test_html_without_links_raises(self):
        html_no_links = "x" * 600
        resp = make_mock_response(
            200, text=html_no_links, content=b"data", url="https://anda.org.br"
        )
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="sem links"),
        ):
            await client.fetch_estatisticas_page()


class TestDownloadFile:
    @pytest.mark.asyncio
    async def test_valid_file_returns_bytes(self):
        content = b"%PDF" + b"x" * 596
        resp = make_mock_response(200, text="ok", content=content, url="https://anda.org.br/f.pdf")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.download_file("https://anda.org.br/f.pdf")

        assert result == content

    @pytest.mark.asyncio
    async def test_small_file_raises_source_unavailable(self):
        content = b"tiny"
        resp = make_mock_response(200, text="ok", content=content, url="https://anda.org.br/f.pdf")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.anda.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="muito pequeno"),
        ):
            await client.download_file("https://anda.org.br/f.pdf")


class TestFetchEntregasPdf:
    @pytest.mark.asyncio
    async def test_no_pdf_found_raises(self):
        with (
            patch(
                "agrobr.anda.client.fetch_estatisticas_page", new_callable=AsyncMock
            ) as mock_page,
            pytest.raises(InvalidParameterError, match="não encontrado"),
        ):
            mock_page.return_value = "<html><body>no links</body></html>"
            await client.fetch_entregas_pdf(2024)

    @pytest.mark.asyncio
    async def test_finds_pdf_by_text_match(self):
        html = (
            "<html><body>"
            '<a href="https://anda.org.br/docs/entregas_2024.pdf">Entregas fertilizantes 2024</a>'
            "</body></html>"
        )
        pdf_content = b"x" * 600

        with (
            patch(
                "agrobr.anda.client.fetch_estatisticas_page", new_callable=AsyncMock
            ) as mock_page,
            patch("agrobr.anda.client.download_file", new_callable=AsyncMock) as mock_dl,
        ):
            mock_page.return_value = html
            mock_dl.return_value = pdf_content
            result_bytes, ano_real = await client.fetch_entregas_pdf(2024)

        assert result_bytes == pdf_content
        assert ano_real == 2024

    @pytest.mark.asyncio
    async def test_finds_pdf_by_url_when_text_has_no_year(self):
        html = (
            "<html><body>"
            '<a href="https://anda.org.br/docs/report_2023.pdf">Report PDF</a>'
            "</body></html>"
        )
        pdf_content = b"x" * 600

        with (
            patch(
                "agrobr.anda.client.fetch_estatisticas_page", new_callable=AsyncMock
            ) as mock_page,
            patch("agrobr.anda.client.download_file", new_callable=AsyncMock) as mock_dl,
        ):
            mock_page.return_value = html
            mock_dl.return_value = pdf_content
            result_bytes, ano_real = await client.fetch_entregas_pdf(2023)

        assert result_bytes == pdf_content
        assert ano_real == 2023

    @pytest.mark.asyncio
    async def test_priority_selects_entrega_keyword(self):
        html = (
            "<html><body>"
            '<a href="https://anda.org.br/docs/other_2024.pdf">Relatorio 2024</a>'
            '<a href="https://anda.org.br/docs/entregas_2024.pdf">Entregas fertilizantes 2024</a>'
            "</body></html>"
        )
        pdf_content = b"x" * 600

        with (
            patch(
                "agrobr.anda.client.fetch_estatisticas_page", new_callable=AsyncMock
            ) as mock_page,
            patch("agrobr.anda.client.download_file", new_callable=AsyncMock) as mock_dl,
        ):
            mock_page.return_value = html
            mock_dl.return_value = pdf_content
            await client.fetch_entregas_pdf(2024)

        mock_dl.assert_called_once_with("https://anda.org.br/docs/entregas_2024.pdf")

    @pytest.mark.asyncio
    async def test_ano_indisponivel_raises_com_anos_disponiveis(self):
        html = (
            "<html><body>"
            '<a href="https://anda.org.br/docs/entregas_2023.pdf">Entregas 2023</a>'
            "</body></html>"
        )

        with (
            patch(
                "agrobr.anda.client.fetch_estatisticas_page", new_callable=AsyncMock
            ) as mock_page,
            patch("agrobr.anda.client.download_file", new_callable=AsyncMock) as mock_dl,
            pytest.raises(InvalidParameterError) as exc_info,
        ):
            mock_page.return_value = html
            await client.fetch_entregas_pdf(2025)

        assert "2025 não encontrado" in str(exc_info.value)
        assert "2023" in str(exc_info.value)
        mock_dl.assert_not_called()

    @pytest.mark.asyncio
    async def test_year_extracted_from_url_when_text_has_no_year(self):
        html = (
            "<html><body>"
            '<a href="https://anda.org.br/docs/entregas_2024.pdf">Download entrega</a>'
            "</body></html>"
        )
        pdf_content = b"x" * 600

        with (
            patch(
                "agrobr.anda.client.fetch_estatisticas_page", new_callable=AsyncMock
            ) as mock_page,
            patch("agrobr.anda.client.download_file", new_callable=AsyncMock) as mock_dl,
        ):
            mock_page.return_value = html
            mock_dl.return_value = pdf_content
            _, ano_real = await client.fetch_entregas_pdf(2024)

        assert ano_real == 2024

    @pytest.mark.asyncio
    async def test_ano_real_defaults_when_no_year_in_text_or_filename(self):
        html = (
            "<html><body>"
            '<a href="https://anda.org.br/2024/report.pdf">Download report</a>'
            "</body></html>"
        )
        pdf_content = b"x" * 600

        with (
            patch(
                "agrobr.anda.client.fetch_estatisticas_page", new_callable=AsyncMock
            ) as mock_page,
            patch("agrobr.anda.client.download_file", new_callable=AsyncMock) as mock_dl,
        ):
            mock_page.return_value = html
            mock_dl.return_value = pdf_content
            _, ano_real = await client.fetch_entregas_pdf(2024)

        assert ano_real == 2024
