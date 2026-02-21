from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_CSV_SIZE, HTTPSettings
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=180.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)


async def download_csv(url: str) -> bytes:
    logger.info("mapa_psr_download", url=url)

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
        if len(content) < MIN_CSV_SIZE:
            from agrobr.exceptions import SourceUnavailableError

            raise SourceUnavailableError(
                source="mapa_psr",
                url=url,
                last_error=(
                    f"Downloaded CSV too small ({len(content)} bytes), expected valid CSV data"
                ),
            )

        logger.info(
            "mapa_psr_download_ok",
            url=url,
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
