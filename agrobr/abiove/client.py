from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_XLSX_SIZE, URLS, Fonte, HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BASE_URL = URLS[Fonte.ABIOVE]["exportacao"]

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=60.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)


async def _fetch_url(url: str) -> bytes:
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        logger.debug("abiove_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url),
            source="abiove",
        )

        if response.status_code == 404:
            raise SourceUnavailableError(source="abiove", url=url, last_error="HTTP 404")

        response.raise_for_status()

        content = response.content
        if len(content) < MIN_XLSX_SIZE:
            raise SourceUnavailableError(
                source="abiove",
                url=url,
                last_error=(
                    f"Downloaded file too small ({len(content)} bytes), expected a valid XLSX"
                ),
            )
        return content


async def fetch_exportacao_excel(ano: int, mes: int | None = None) -> tuple[bytes, str]:
    meses = [mes] if mes else list(range(12, 0, -1))

    last_error = ""

    for m in meses:
        url = f"{BASE_URL}/exp_{ano:04d}{m:02d}.xlsx"
        try:
            data = await _fetch_url(url)
            logger.info("abiove_excel_found", url=url, size=len(data))
            return data, url
        except SourceUnavailableError as e:
            last_error = e.last_error
            if mes:
                raise
            logger.debug("abiove_month_not_found", ano=ano, mes=m)
            continue

    raise SourceUnavailableError(
        source="abiove",
        url=f"{BASE_URL}/exp_{ano}*.xlsx",
        last_error=f"Nenhum arquivo encontrado para {ano}: {last_error}",
    )
