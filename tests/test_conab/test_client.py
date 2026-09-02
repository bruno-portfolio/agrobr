"""Testes de resiliência HTTP para agrobr.conab.client (Playwright-based)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.conab import client
from agrobr.exceptions import SourceUnavailableError


class TestConabPlaywrightUnavailable:
    @pytest.mark.asyncio
    async def test_fetch_boletim_no_playwright_raises(self):
        with (
            patch("agrobr.http.browser.is_available", return_value=False),
            pytest.raises(SourceUnavailableError, match="Playwright not available"),
        ):
            await client.fetch_boletim_page()

    @pytest.mark.asyncio
    async def test_download_xlsx_no_playwright_raises(self):
        with (
            patch("agrobr.http.browser.is_available", return_value=False),
            pytest.raises(SourceUnavailableError, match="Playwright not available"),
        ):
            await client.download_xlsx("https://www.gov.br/test.xlsx")


class TestConabListLevantamentos:
    @pytest.mark.asyncio
    async def test_empty_html_returns_empty_list(self):
        result = await client.list_levantamentos(html="<html></html>")
        assert result == []

    @pytest.mark.asyncio
    async def test_malformed_html_returns_empty_list(self):
        result = await client.list_levantamentos(html="<<<broken html>>>")
        assert result == []

    @pytest.mark.asyncio
    async def test_valid_html_extracts_levantamentos(self):
        html = """
        <html><body>
        <a href="https://conab.gov.br/12o-levantamento-safra-2024-25/dados.xlsx">Tabela 12</a>
        <a href="https://conab.gov.br/11o-levantamento-safra-2024-25/dados.xlsx">Tabela 11</a>
        </body></html>
        """
        result = await client.list_levantamentos(html=html)
        assert len(result) == 2
        assert result[0]["levantamento"] >= result[1]["levantamento"]


class TestConabFetchSafra:
    @pytest.mark.asyncio
    async def test_safra_not_found_raises(self):
        with (
            patch("agrobr.conab.client.list_levantamentos", new_callable=AsyncMock) as mock_list,
            pytest.raises(SourceUnavailableError, match="No levantamento found"),
        ):
            mock_list.return_value = [
                {"safra": "2024/25", "levantamento": 12, "url": "test"},
            ]
            await client.fetch_safra_xlsx(safra="2020/21")

    @pytest.mark.asyncio
    async def test_fetch_boletim_missing_chromium_fails_first_attempt(self):
        launch = AsyncMock(side_effect=Exception("Executable doesn't exist at chromium"))

        with (
            patch("agrobr.http.browser.is_available", return_value=True),
            patch("agrobr.conab.client.async_playwright") as mock_pw,
            pytest.raises(
                SourceUnavailableError,
                match="python -m playwright install chromium",
            ),
        ):
            mock_pw.return_value.__aenter__ = AsyncMock(
                return_value=MagicMock(chromium=MagicMock(launch=launch))
            )
            await client.fetch_boletim_page()

        assert mock_pw.call_count == 1
        assert launch.await_count == 1

    @pytest.mark.asyncio
    async def test_fetch_boletim_transient_error_retries(self):
        launch = AsyncMock(side_effect=Exception("Timeout"))
        max_retries = client.constants.HTTPSettings().max_retries

        with (
            patch("agrobr.http.browser.is_available", return_value=True),
            patch("agrobr.conab.client.async_playwright") as mock_pw,
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch("agrobr.http.rate_limiter._async_sleep", new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            mock_pw.return_value.__aenter__ = AsyncMock(
                return_value=MagicMock(chromium=MagicMock(launch=launch))
            )
            await client.fetch_boletim_page()

        assert mock_pw.call_count == max_retries
        assert launch.await_count == max_retries
