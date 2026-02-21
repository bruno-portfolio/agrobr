from __future__ import annotations

import re
from io import BytesIO
from typing import Any

import structlog

from agrobr import constants
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.rate_limiter import RateLimiter
from agrobr.http.user_agents import UserAgentRotator

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover
    async_playwright = None  # type: ignore[assignment,misc]

logger = structlog.get_logger()


async def fetch_boletim_page() -> str:
    import asyncio

    url = constants.URLS[constants.Fonte.CONAB]["boletim_graos"]

    logger.info("conab_fetch_boletim_page", url=url)

    from agrobr.http.browser import is_available

    if not is_available():
        raise SourceUnavailableError(
            source="conab",
            url=url,
            last_error="Playwright not available for CONAB page fetch",
        )

    settings = constants.HTTPSettings()
    last_error: Exception | None = None

    for attempt in range(settings.max_retries):
        try:
            async with RateLimiter.acquire(constants.Fonte.CONAB), async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(
                    user_agent=UserAgentRotator.get_random(),
                    viewport={"width": 1920, "height": 1080},
                )

                await page.goto(url, timeout=60000)
                await page.wait_for_timeout(3000)

                html: str = await page.content()
                await browser.close()

                if len(html) < 5_000 or "levantamento" not in html.lower():
                    raise SourceUnavailableError(
                        source="conab",
                        url=url,
                        last_error=(
                            f"Response too small or missing expected content "
                            f"({len(html)} bytes, no 'levantamento' marker)"
                        ),
                    )

                logger.info(
                    "conab_fetch_boletim_success",
                    content_length=len(html),
                )

                return html

        except Exception as e:
            last_error = e
            if attempt < settings.max_retries - 1:
                delay = settings.retry_base_delay * (settings.retry_exponential_base**attempt)
                logger.warning(
                    "conab_boletim_retry", attempt=attempt + 1, error=str(e), delay=delay
                )
                await asyncio.sleep(delay)

    raise SourceUnavailableError(
        source="conab",
        url=url,
        last_error=str(last_error),
    )


async def list_levantamentos(html: str | None = None) -> list[dict[str, Any]]:
    if html is None:
        html = await fetch_boletim_page()

    levantamentos = []

    pattern = r'href="([^"]+/(\d+)o-levantamento-safra-(\d{4})-(\d{2})/[^"]*\.xlsx)"[^>]*>([^<]*Tabela[^<]*)'

    for match in re.finditer(pattern, html, re.IGNORECASE):
        url = match.group(1)
        num_levantamento = int(match.group(2))
        ano_inicio = int(match.group(3))
        ano_fim = int(match.group(4))

        levantamentos.append(
            {
                "url": url,
                "levantamento": num_levantamento,
                "safra": f"{ano_inicio}/{ano_fim}",
                "ano_inicio": ano_inicio,
                "ano_fim": ano_fim,
            }
        )

    levantamentos.sort(key=lambda x: (x["ano_inicio"], x["levantamento"]), reverse=True)

    logger.info(
        "conab_levantamentos_found",
        count=len(levantamentos),
    )

    return levantamentos


async def download_xlsx(url: str) -> BytesIO:
    logger.info("conab_download_xlsx", url=url)

    from agrobr.http.browser import is_available

    if not is_available():
        raise SourceUnavailableError(
            source="conab",
            url=url,
            last_error="Playwright not available for CONAB download",
        )

    async with RateLimiter.acquire(constants.Fonte.CONAB), async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        try:
            async with page.expect_download(timeout=60000) as download_info:
                await page.evaluate(f'() => {{ window.location.href = "{url}" }}')

            download = await download_info.value

            path = await download.path()
            if path:
                with open(path, "rb") as f:
                    content = f.read()

                if len(content) < 1_000:
                    raise SourceUnavailableError(
                        source="conab",
                        url=url,
                        last_error=(
                            f"Downloaded XLSX too small ({len(content)} bytes), "
                            f"expected a valid spreadsheet"
                        ),
                    )

                logger.info(
                    "conab_download_success",
                    url=url,
                    size_bytes=len(content),
                )

                return BytesIO(content)
            else:
                raise SourceUnavailableError(
                    source="conab",
                    url=url,
                    last_error="Download path not available",
                )

        except Exception as e:
            logger.error(
                "conab_download_failed",
                url=url,
                error=str(e),
            )
            raise SourceUnavailableError(
                source="conab",
                url=url,
                last_error=str(e),
            ) from e

        finally:
            await browser.close()


async def fetch_latest_safra_xlsx() -> tuple[BytesIO, dict[str, Any]]:
    levantamentos = await list_levantamentos()

    if not levantamentos:
        raise SourceUnavailableError(
            source="conab",
            url=constants.URLS[constants.Fonte.CONAB]["boletim_graos"],
            last_error="No levantamentos found",
        )

    latest = levantamentos[0]
    xlsx = await download_xlsx(latest["url"])

    return xlsx, latest


async def fetch_safra_xlsx(
    safra: str | None = None,
    levantamento: int | None = None,
) -> tuple[BytesIO, dict[str, Any]]:
    levantamentos = await list_levantamentos()

    if not levantamentos:
        raise SourceUnavailableError(
            source="conab",
            url=constants.URLS[constants.Fonte.CONAB]["boletim_graos"],
            last_error="No levantamentos found",
        )

    filtered = levantamentos

    if safra:
        filtered = [lev for lev in filtered if lev["safra"] == safra]

    if levantamento:
        filtered = [lev for lev in filtered if lev["levantamento"] == levantamento]

    if not filtered:
        raise SourceUnavailableError(
            source="conab",
            url=constants.URLS[constants.Fonte.CONAB]["boletim_graos"],
            last_error=f"No levantamento found for safra={safra}, levantamento={levantamento}",
        )

    target = filtered[0]
    xlsx = await download_xlsx(target["url"])

    return xlsx, target
