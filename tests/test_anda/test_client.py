"""Testes de resiliência HTTP para agrobr.anda.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.anda import client
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from tests.helpers import (
    make_mock_async_client,
    make_mock_response,
)


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

    def test_parse_links_malformed_html(self):
        links = client.parse_links_from_html("<html><broken><<<")
        assert links == []


class TestFetchEntregasPdf:
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
            _, ano_real, _ = await client.fetch_entregas_pdf(2024)

        assert ano_real == 2024
