from __future__ import annotations

import httpx

from agrobr import _log
from agrobr.constants import URLS, Fonte
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

logger = _log.get_logger(__name__)

FORMULADOS_URL = URLS[Fonte.DEFENSIVOS]["formulados"]
TECNICOS_URL = URLS[Fonte.DEFENSIVOS]["tecnicos"]

TIMEOUT_FORMULADOS = get_timeout(read=300.0)
TIMEOUT_TECNICOS = get_timeout(read=60.0)

MIN_CSV_FORMULADOS = 100_000_000
MIN_CSV_TECNICOS = 50_000


async def _download(url: str, timeout: httpx.Timeout, min_size: int, label: str) -> bytes:
    logger.debug(f"defensivos_download_{label}", url=url)

    async with httpx.AsyncClient(
        timeout=timeout,
        headers=UserAgentRotator.get_headers(source="defensivos"),
        follow_redirects=True,
    ) as http:
        response = await retry_on_status(
            lambda: http.get(url),
            source="defensivos",
        )
        responses.raise_for_status(response, source="defensivos")
        content = response.content

        if len(content) < min_size:
            from agrobr.exceptions import SourceUnavailableError

            raise SourceUnavailableError(
                source="defensivos",
                url=url,
                last_error=f"CSV {label} too small ({len(content)} bytes)",
            )

    logger.info(f"defensivos_download_{label}_ok", size_bytes=len(content))
    return content


async def download_formulados() -> bytes:
    return await _download(FORMULADOS_URL, TIMEOUT_FORMULADOS, MIN_CSV_FORMULADOS, "formulados")


async def download_tecnicos() -> bytes:
    return await _download(TECNICOS_URL, TIMEOUT_TECNICOS, MIN_CSV_TECNICOS, "tecnicos")
