from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_CSV_SIZE
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

logger = structlog.get_logger()

TIMEOUT = get_timeout(read=180.0)


async def download_csv(url: str) -> bytes:
    logger.debug("mapa_psr_download", url=url)

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="mapa_psr"),
        follow_redirects=True,
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="mapa_psr",
        )
        response.raise_for_status()

        content = response.content
        io_utils.validate_download(
            content,
            kinds=("csv",),
            source="mapa_psr",
            url=url,
            min_size=MIN_CSV_SIZE,
        )

        logger.info(
            "mapa_psr_download_ok",
            source="mapa_psr",
            size_bytes=len(content),
        )
        return content


async def fetch_periodo(periodo: str) -> bytes:
    from agrobr.alt.mapa_psr.models import get_csv_url

    url = get_csv_url(periodo)
    return await download_csv(url)


async def fetch_periodos(periodos: list[str]) -> list[bytes]:
    results: list[bytes] = []
    for periodo in periodos:
        content = await fetch_periodo(periodo)
        results.append(content)
    return results
