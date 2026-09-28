from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from agrobr.http import browser


@pytest.fixture
def runtime(monkeypatch):
    page = AsyncMock()
    context = AsyncMock()
    context.new_page.return_value = page
    instance = AsyncMock()
    instance.new_context.return_value = context
    engine = AsyncMock()
    engine.chromium.launch.return_value = instance
    factory = MagicMock()
    factory.start = AsyncMock(return_value=engine)
    monkeypatch.setattr(browser, "async_playwright", lambda: factory, raising=False)
    monkeypatch.setattr(browser, "_playwright_available", True)
    monkeypatch.setattr(browser, "_sessions", {})
    return engine, instance, context, page


async def test_browser_launch_failure_stops_driver(runtime):
    engine, _, _, _ = runtime
    engine.chromium.launch.side_effect = RuntimeError("launch failed")
    with pytest.raises(RuntimeError, match="launch failed"):
        async with browser.get_page():
            pytest.fail("page should not open")
    engine.stop.assert_awaited_once()
    assert not browser._sessions


async def test_cancelled_page_releases_resources(runtime):
    engine, instance, _, _ = runtime
    with pytest.raises(asyncio.CancelledError):
        async with browser.get_page():
            raise asyncio.CancelledError
    engine.stop.assert_awaited_once()
    instance.close.assert_awaited_once()
    assert not browser._sessions
