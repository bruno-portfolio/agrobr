from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_XLSX_SIZE, URLS, Fonte, HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BASE_URL = URLS[Fonte.DERAL]["downloads"]

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=30.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)


async def _fetch_bytes(url: str) -> bytes:
    headers = UserAgentRotator.get_bot_headers()
    headers["Accept"] = (
        "application/vnd.ms-excel, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, */*"
    )

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers, follow_redirects=True) as client:
        logger.debug("deral_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url),
            source="deral",
        )

        if response.status_code == 404:
            raise SourceUnavailableError(
                source="deral",
                url=url,
                last_error="Arquivo não encontrado (404)",
            )

        response.raise_for_status()

        content = response.content
        if len(content) < MIN_XLSX_SIZE:
            raise SourceUnavailableError(
                source="deral",
                url=url,
                last_error=(
                    f"Downloaded file too small ({len(content)} bytes), "
                    f"expected a valid spreadsheet"
                ),
            )
        return content


async def fetch_pc_xls() -> bytes:
    url = f"{BASE_URL}/PC.xls"
    logger.info("deral_fetch_pc", url=url)
    return await _fetch_bytes(url)


async def fetch_pss_xlsx() -> bytes:
    url = f"{BASE_URL}/pss.xlsx"
    logger.info("deral_fetch_pss", url=url)
    return await _fetch_bytes(url)
