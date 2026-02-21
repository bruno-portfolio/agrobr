from __future__ import annotations

import asyncio
import atexit
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

_playwright_instance: Playwright | None = None
_browser: Browser | None = None
_lock = asyncio.Lock()


def _sync_cleanup() -> None:
    global _playwright_instance, _browser
    if _browser is None and _playwright_instance is None:
        return
    try:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(close_browser())
        loop.close()
    except Exception:
        logger.warning("browser_sync_cleanup_failed", exc_info=True)


atexit.register(_sync_cleanup)


def is_available() -> bool:
    return _playwright_available


async def _get_browser() -> Browser:
    global _playwright_instance, _browser

    if not _playwright_available:
        raise SourceUnavailableError(
            source="browser",
            url="",
            last_error="Playwright not installed or incompatible with current Python version",
        )

    async with _lock:
        if _browser is None or not _browser.is_connected():
            logger.info("browser_starting", browser="chromium")

            _playwright_instance = await async_playwright().start()
            _browser = await _playwright_instance.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )

            logger.info("browser_started")

        return _browser


async def close_browser() -> None:
    global _playwright_instance, _browser

    async with _lock:
        if _browser is not None:
            await _browser.close()
            _browser = None
            logger.info("browser_closed")

        if _playwright_instance is not None:
            await _playwright_instance.stop()
            _playwright_instance = None


@asynccontextmanager
async def get_page() -> AsyncGenerator[Page, None]:
    browser = await _get_browser()

    ua = UserAgentRotator.get_random()
    context = await browser.new_context(
        user_agent=ua,
        viewport={"width": 1920, "height": 1080},
        locale="pt-BR",
        timezone_id="America/Sao_Paulo",
        extra_http_headers={
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )

    page = await context.new_page()

    await page.add_init_script(
        """
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    """
    )

    try:
        yield page
    finally:
        await context.close()


async def fetch_with_browser(
    url: str,
    source: str = "unknown",
    wait_selector: str | None = None,
    wait_timeout: float = 30000,
) -> str:
    logger.info(
        "browser_fetch_start",
        source=source,
        url=url,
    )

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
                url=url,
                content_length=len(html),
                status=response.status,
            )

            return html

    except Exception as e:
        logger.error(
            "browser_fetch_failed",
            source=source,
            url=url,
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
