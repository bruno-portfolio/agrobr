"""Cliente HTTP para download de dados da CONAB."""

from __future__ import annotations

import re
from io import BytesIO
from typing import Any

import structlog
from playwright.async_api import async_playwright

from agrobr import constants
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.rate_limiter import RateLimiter
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()


async def fetch_boletim_page() -> str:
    """
    Busca página do boletim de safras de grãos da CONAB.

    Returns:
        HTML da página com lista de levantamentos
    """
    url = constants.URLS[constants.Fonte.CONAB]["boletim_graos"]

    logger.info("conab_fetch_boletim_page", url=url)

    async with RateLimiter.acquire(constants.Fonte.CONAB), async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(
            user_agent=UserAgentRotator.get_random(),
            viewport={"width": 1920, "height": 1080},
        )

        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(3000)

        html = await page.content()
        await browser.close()

        logger.info(
            "conab_fetch_boletim_success",
            content_length=len(html),
        )

        return html


async def list_levantamentos(html: str | None = None) -> list[dict[str, Any]]:
    """
    Lista levantamentos disponíveis na página do boletim.

    Args:
        html: HTML da página (se None, busca automaticamente)

    Returns:
        Lista de dicts com informações dos levantamentos
    """
    if html is None:
        html = await fetch_boletim_page()

    levantamentos = []

    pattern = r'href="([^"]+/(\d+)o-levantamento-safra-(\d{4})-(\d{2})/[^"]*\.xlsx)"[^>]*>([^<]*Tabela[^<]*)'

    for match in re.finditer(pattern, html, re.IGNORECASE):
        url = match.group(1)
        num_levantamento = int(match.group(2))
        ano_inicio = int(match.group(3))
        ano_fim = int(match.group(4))

        levantamentos.append({
            "url": url,
            "levantamento": num_levantamento,
            "safra": f"{ano_inicio}/{ano_fim}",
            "ano_inicio": ano_inicio,
            "ano_fim": ano_fim,
        })

    levantamentos.sort(key=lambda x: (x["ano_inicio"], x["levantamento"]), reverse=True)

    logger.info(
        "conab_levantamentos_found",
        count=len(levantamentos),
    )

    return levantamentos


async def download_xlsx(url: str) -> BytesIO:
    """
    Baixa arquivo XLSX da CONAB.

    Args:
        url: URL do arquivo XLSX

    Returns:
        BytesIO com conteúdo do arquivo

    Raises:
        SourceUnavailableError: Se não conseguir baixar
    """
    logger.info("conab_download_xlsx", url=url)

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
    """
    Baixa planilha do levantamento mais recente.

    Returns:
        tuple: (BytesIO com arquivo, metadata do levantamento)
    """
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
    """
    Baixa planilha de safra específica.

    Args:
        safra: Safra no formato "2024/25" (default: mais recente)
        levantamento: Número do levantamento (default: mais recente)

    Returns:
        tuple: (BytesIO com arquivo, metadata do levantamento)
    """
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
