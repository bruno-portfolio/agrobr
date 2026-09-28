from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog

from agrobr import constants
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

try:
    from playwright.async_api import Browser, Page, Playwright, async_playwright

    _playwright_available = True
except ImportError:
    _playwright_available = False
    Browser = None  # type: ignore[assignment,misc]
    Page = None  # type: ignore[assignment,misc]
    Playwright = None  # type: ignore[assignment,misc]
    logger.warning(
        "playwright_not_available", hint="pip install playwright && playwright install chromium"
    )

_sessions: dict[asyncio.AbstractEventLoop, list[tuple[Playwright, Browser]]] = {}


def is_available() -> bool:
    return _playwright_available


async def _get_browser() -> Browser:
    if not _playwright_available:
        raise SourceUnavailableError(
            source="browser",
            url="",
            last_error="Playwright not installed or incompatible with current Python version",
        )

    logger.info("browser_starting", browser="chromium")
    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
    except BaseException:
        await playwright.stop()
        raise
    _sessions.setdefault(asyncio.get_running_loop(), []).append((playwright, browser))
    logger.info("browser_started")
    return browser


async def close_browser(target: Browser | None = None) -> None:
    loop = asyncio.get_running_loop()
    sessions = _sessions.get(loop, [])
    closing = [session for session in sessions if target is None or session[1] is target]
    for playwright, browser in closing:
        sessions.remove((playwright, browser))
        try:
            await browser.close()
        finally:
            await playwright.stop()
            if not sessions:
                _sessions.pop(loop, None)
        logger.info("browser_closed")


@asynccontextmanager
async def get_page() -> AsyncGenerator[Page, None]:
    browser = await _get_browser()

    try:
        context = await browser.new_context(
            user_agent=UserAgentRotator.get_random(),
            viewport={"width": 1920, "height": 1080},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"},
        )
        try:
            page = await context.new_page()
            await page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )
            yield page
        finally:
            await context.close()
    finally:
        await close_browser(browser)


async def fetch_with_browser(
    url: str,
    source: str = "unknown",
    wait_selector: str | None = None,
    wait_timeout: float = 30000,
) -> str:
    logger.debug("browser_fetch_start_detail", url=url)
    logger.info("browser_fetch_start", source=source)

    try:
        async with get_page() as page:
            response = await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=wait_timeout,
            )

            if response is None:
                raise SourceUnavailableError(
                    source=source,
                    url=url,
                    last_error="No response received",
                )

            if wait_selector:
                try:
                    await page.wait_for_selector(
                        wait_selector,
                        timeout=wait_timeout,
                    )
                except Exception as e:
                    logger.warning(
                        "browser_wait_selector_timeout",
                        selector=wait_selector,
                        error=str(e),
                    )

            await page.wait_for_timeout(5000)

            if response.status in (403, 503):
                check_html: str = await page.content()
                if "cloudflare" in check_html.lower() or "challenge" in check_html.lower():
                    raise SourceUnavailableError(
                        source=source,
                        url=url,
                        last_error=f"Cloudflare block detected (status {response.status})",
                    )

            html: str = await page.content()

            logger.info(
                "browser_fetch_success",
                source=source,
                content_length=len(html),
                status=response.status,
            )

            return html

    except Exception as e:
        logger.debug("browser_fetch_failed_detail", url=url)
        logger.error(
            "browser_fetch_failed",
            source=source,
            error=str(e),
        )
        raise SourceUnavailableError(
            source=source,
            url=url,
            last_error=str(e),
        ) from e


async def fetch_cepea_indicador(produto: str) -> str:
    produto_key = constants.CEPEA_PRODUTOS.get(produto.lower(), produto.lower())
    url = f"{constants.URLS[constants.Fonte.CEPEA]['indicadores']}/{produto_key}.aspx"

    return await fetch_with_browser(
        url=url,
        source="cepea",
        wait_selector="table",
        wait_timeout=90000,
    )
