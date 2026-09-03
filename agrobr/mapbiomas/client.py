from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_XLSX_SIZE, URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

logger = structlog.get_logger()

DATAVERSE_BASE = URLS[Fonte.MAPBIOMAS]["dataverse"]
BIOME_STATE_FILE_ID = URLS[Fonte.MAPBIOMAS]["biome_state_file_id"]
BIOME_STATE_MUNICIPALITY_FILE_ID = URLS[Fonte.MAPBIOMAS]["biome_state_municipality_file_id"]

TIMEOUT = get_timeout(read=120.0)


def _build_xlsx_url(nivel: str) -> str:
    if "MUNICIPALITY" in nivel.upper():
        return f"{DATAVERSE_BASE}/{BIOME_STATE_MUNICIPALITY_FILE_ID}"
    return f"{DATAVERSE_BASE}/{BIOME_STATE_FILE_ID}?format=original"


async def _fetch_url(url: str) -> bytes:
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        logger.debug("mapbiomas_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url),
            source="mapbiomas",
        )

        if response.status_code == 404:
            raise SourceUnavailableError(source="mapbiomas", url=url, last_error="HTTP 404")

        response.raise_for_status()

        content = response.content
        io_utils.validate_download(
            content,
            kinds=("xlsx",),
            source="mapbiomas",
            url=url,
            min_size=MIN_XLSX_SIZE,
        )
        return content


async def fetch_biome_state() -> tuple[bytes, str]:
    url = _build_xlsx_url("BIOME_STATE")
    content = await _fetch_url(url)
    logger.info("mapbiomas_xlsx_found", source="mapbiomas", size=len(content))
    return content, url


async def fetch_biome_state_municipality() -> tuple[bytes, str]:
    url = _build_xlsx_url("BIOME_STATE_MUNICIPALITY")
    content = await _fetch_url(url)
    logger.info("mapbiomas_xlsx_found", source="mapbiomas", size=len(content))
    return content, url
