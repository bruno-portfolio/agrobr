from __future__ import annotations

from typing import Any

import httpx

from agrobr import _log
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

logger = _log.get_logger(__name__)

BASE_URL = URLS[Fonte.IMEA]["base"]

TIMEOUT = get_timeout()


async def _fetch_json(url: str) -> tuple[list[dict[str, Any]], bytes]:
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        logger.debug("imea_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url),
            source="imea",
        )

        responses.raise_for_status(response, source="imea")
        data = responses.parse_json_response(response, source="imea", url=url)
        if not isinstance(data, list):
            raise SourceUnavailableError(
                source="imea",
                url=url,
                last_error=f"JSON inesperado: {type(data).__name__}",
            )
        return data, response.content


def cotacoes_url(cadeia_id: int) -> str:
    return f"{BASE_URL}/v2/mobile/cadeias/{cadeia_id}/cotacoes"


def indicadores_url(cadeia_id: int) -> str:
    return f"{BASE_URL}/v2/mobile/cadeias/{cadeia_id}/indicadores"


async def fetch_cotacoes(cadeia_id: int) -> tuple[list[dict[str, Any]], bytes]:
    url = cotacoes_url(cadeia_id)
    logger.debug("imea_fetch_cotacoes", url=url)
    logger.info("imea_fetch_cotacoes", source="imea", cadeia_id=cadeia_id)
    return await _fetch_json(url)


async def fetch_indicadores(cadeia_id: int) -> tuple[list[dict[str, Any]], bytes]:
    url = indicadores_url(cadeia_id)
    logger.info("imea_fetch_indicadores", source="imea", cadeia_id=cadeia_id)
    return await _fetch_json(url)
