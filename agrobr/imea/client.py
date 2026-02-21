from __future__ import annotations

from typing import Any

import httpx
import structlog

from agrobr.constants import URLS, Fonte, HTTPSettings
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BASE_URL = URLS[Fonte.IMEA]["base"]

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=30.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)


async def _fetch_json(url: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        logger.debug("imea_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url),
            source="imea",
        )

        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []


async def fetch_cotacoes(cadeia_id: int) -> list[dict[str, Any]]:
    url = f"{BASE_URL}/v2/mobile/cadeias/{cadeia_id}/cotacoes"
    logger.info("imea_fetch_cotacoes", cadeia_id=cadeia_id, url=url)
    return await _fetch_json(url)


async def fetch_indicadores() -> list[dict[str, Any]]:
    url = f"{BASE_URL}/indicador"
    logger.info("imea_fetch_indicadores", url=url)
    data = await _fetch_json(url)
    return data if isinstance(data, list) else [data] if data else []
