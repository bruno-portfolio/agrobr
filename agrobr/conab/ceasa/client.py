from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

from .models import (
    CDA_PROHORT,
    PENTAHO_AUTH,
    PENTAHO_BASE,
    QUERY_CEASAS,
    QUERY_PRECOS,
    QUERY_PRODUTOS,
)

logger = structlog.get_logger()

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=30.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)


def _build_url(cda_path: str, query_id: str) -> str:
    params = {
        "path": cda_path,
        "dataAccessId": query_id,
        **PENTAHO_AUTH,
    }
    return f"{PENTAHO_BASE}?{urlencode(params)}"


async def _fetch_query(cda_path: str, query_id: str) -> tuple[dict[str, Any], str]:
    url = _build_url(cda_path, query_id)
    logger.debug("conab_ceasa_fetch", query=query_id, url=url)

    headers = UserAgentRotator.get_headers(source="conab_ceasa")
    headers["Accept"] = "application/json"

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=headers,
        follow_redirects=True,
    ) as http:
        resp = await retry_on_status(
            lambda: http.get(url),
            source="conab_ceasa",
        )

    if resp.status_code != 200:
        raise SourceUnavailableError(
            source="conab_ceasa",
            url=url,
            last_error=f"HTTP {resp.status_code}",
        )

    resp.raise_for_status()
    return resp.json(), url


async def fetch_precos() -> tuple[dict[str, Any], str]:
    return await _fetch_query(CDA_PROHORT, QUERY_PRECOS)


async def fetch_ceasas() -> tuple[dict[str, Any], str]:
    return await _fetch_query(CDA_PROHORT, QUERY_CEASAS)


async def fetch_produtos() -> tuple[dict[str, Any], str]:
    return await _fetch_query(CDA_PROHORT, QUERY_PRODUTOS)
