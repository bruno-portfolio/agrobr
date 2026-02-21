from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_XLSX_SIZE, URLS, Fonte, HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

from .models import COLECAO_ATUAL

logger = structlog.get_logger()

GCS_BASE = URLS[Fonte.MAPBIOMAS]["gcs"]

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=120.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)


def _build_xlsx_url(nivel: str, colecao: int = COLECAO_ATUAL) -> str:
    sufixo = nivel.upper()
    return (
        f"{GCS_BASE}/collection_{colecao}/lulc/statistics/"
        f"MAPBIOMAS_BRAZIL-COL.{colecao}-{sufixo}.xlsx"
    )


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
        if len(content) < MIN_XLSX_SIZE:
            raise SourceUnavailableError(
                source="mapbiomas",
                url=url,
                last_error=(
                    f"Downloaded XLSX too small ({len(content)} bytes), "
                    f"expected a valid spreadsheet"
                ),
            )
        return content


async def fetch_biome_state(colecao: int = COLECAO_ATUAL) -> tuple[bytes, str]:
    url = _build_xlsx_url("BIOME_STATE", colecao)
    content = await _fetch_url(url)
    logger.info("mapbiomas_xlsx_found", url=url, size=len(content))
    return content, url


async def fetch_biome_state_municipality(colecao: int = COLECAO_ATUAL) -> tuple[bytes, str]:
    url = _build_xlsx_url("BIOME_STATE_MUNICIPALITY", colecao)
    content = await _fetch_url(url)
    logger.info("mapbiomas_xlsx_found", url=url, size=len(content))
    return content, url
