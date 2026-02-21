from __future__ import annotations

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status

logger = structlog.get_logger()

BASE_URL = "https://www.agricultura.pr.gov.br/system/files/publico/Safras"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=30.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {
    "User-Agent": "agrobr/0.8.0 (https://github.com/your-org/agrobr)",
    "Accept": "application/vnd.ms-excel, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, */*",
}


async def _fetch_bytes(url: str) -> bytes:
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
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
        if len(content) < 1_000:
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
