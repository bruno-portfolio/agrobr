from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.exceptions import SourceUnavailableError


class TestIsAvailable:
    def test_returns_bool(self):
        from agrobr.http.browser import is_available

        result = is_available()
        assert isinstance(result, bool)


class TestFetchWithBrowser:
    @pytest.mark.asyncio
    async def test_no_response_raises(self):
        from agrobr.http import browser

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=None)

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_context.close = AsyncMock()

        mock_browser = AsyncMock()
        mock_browser.is_connected = MagicMock(return_value=True)
        mock_browser.new_context = AsyncMock(return_value=mock_context)

        with (
            patch.object(browser, "_playwright_available", True),
            patch.object(browser, "_browser", mock_browser),
            patch.object(browser, "_lock", AsyncMock()),
            pytest.raises(SourceUnavailableError),
        ):
            await browser.fetch_with_browser("https://example.com", source="test")

    @pytest.mark.asyncio
    async def test_cloudflare_block_detected(self):
        from agrobr.http import browser

        mock_response = MagicMock()
        mock_response.status = 403

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.content = AsyncMock(return_value="<html>Cloudflare challenge</html>")
        mock_page.wait_for_selector = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.add_init_script = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_context.close = AsyncMock()

        mock_browser_inst = AsyncMock()
        mock_browser_inst.is_connected = MagicMock(return_value=True)
        mock_browser_inst.new_context = AsyncMock(return_value=mock_context)

        with (
            patch.object(browser, "_playwright_available", True),
            patch.object(browser, "_browser", mock_browser_inst),
            patch.object(browser, "_lock", AsyncMock()),
            pytest.raises(SourceUnavailableError, match="Cloudflare"),
        ):
            await browser.fetch_with_browser("https://example.com", source="test")

    @pytest.mark.asyncio
    async def test_successful_fetch(self):
        from agrobr.http import browser

        mock_response = MagicMock()
        mock_response.status = 200

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.content = AsyncMock(return_value="<html>Success</html>")
        mock_page.wait_for_selector = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.add_init_script = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_context.close = AsyncMock()

        mock_browser_inst = AsyncMock()
        mock_browser_inst.is_connected = MagicMock(return_value=True)
        mock_browser_inst.new_context = AsyncMock(return_value=mock_context)

        with (
            patch.object(browser, "_playwright_available", True),
            patch.object(browser, "_browser", mock_browser_inst),
            patch.object(browser, "_lock", AsyncMock()),
        ):
            html = await browser.fetch_with_browser("https://example.com", source="test")
        assert "Success" in html

    @pytest.mark.asyncio
    async def test_with_wait_selector(self):
        from agrobr.http import browser

        mock_response = MagicMock()
        mock_response.status = 200

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.content = AsyncMock(return_value="<html><table></table></html>")
        mock_page.wait_for_selector = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.add_init_script = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_context.close = AsyncMock()

        mock_browser_inst = AsyncMock()
        mock_browser_inst.is_connected = MagicMock(return_value=True)
        mock_browser_inst.new_context = AsyncMock(return_value=mock_context)

        with (
            patch.object(browser, "_playwright_available", True),
            patch.object(browser, "_browser", mock_browser_inst),
            patch.object(browser, "_lock", AsyncMock()),
        ):
            html = await browser.fetch_with_browser(
                "https://example.com", source="test", wait_selector="table"
            )
        assert "table" in html
        mock_page.wait_for_selector.assert_called_once()

    @pytest.mark.asyncio
    async def test_wait_selector_timeout_still_returns(self):
        from agrobr.http import browser

        mock_response = MagicMock()
        mock_response.status = 200

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.content = AsyncMock(return_value="<html>content</html>")
        mock_page.wait_for_selector = AsyncMock(side_effect=Exception("timeout"))
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.add_init_script = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_context.close = AsyncMock()

        mock_browser_inst = AsyncMock()
        mock_browser_inst.is_connected = MagicMock(return_value=True)
        mock_browser_inst.new_context = AsyncMock(return_value=mock_context)

        with (
            patch.object(browser, "_playwright_available", True),
            patch.object(browser, "_browser", mock_browser_inst),
            patch.object(browser, "_lock", AsyncMock()),
        ):
            html = await browser.fetch_with_browser(
                "https://example.com", source="test", wait_selector="table"
            )
        assert "content" in html


class TestGetBrowser:
    @pytest.mark.asyncio
    async def test_not_available_raises(self):
        from agrobr.http import browser

        with (
            patch.object(browser, "_playwright_available", False),
            pytest.raises(SourceUnavailableError, match="Playwright"),
        ):
            await browser._get_browser()


class TestCloseBrowser:
    @pytest.mark.asyncio
    async def test_close_when_active(self):
        from agrobr.http import browser

        mock_browser_inst = AsyncMock()
        mock_pw = AsyncMock()

        original_browser = browser._browser
        original_pw = browser._playwright_instance

        browser._browser = mock_browser_inst
        browser._playwright_instance = mock_pw

        try:
            await browser.close_browser()
            mock_browser_inst.close.assert_called_once()
            mock_pw.stop.assert_called_once()
            assert browser._browser is None
            assert browser._playwright_instance is None
        finally:
            browser._browser = original_browser
            browser._playwright_instance = original_pw

    @pytest.mark.asyncio
    async def test_close_when_none(self):
        from agrobr.http import browser

        original_browser = browser._browser
        original_pw = browser._playwright_instance

        browser._browser = None
        browser._playwright_instance = None

        try:
            await browser.close_browser()
        finally:
            browser._browser = original_browser
            browser._playwright_instance = original_pw
