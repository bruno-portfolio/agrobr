from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

import httpx

from agrobr import _log
from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

from .models import (
    CDA_PROHORT,
    PENTAHO_AUTH,
    PENTAHO_BASE,
    QUERY_CEASAS,
    QUERY_PRECOS,
)

logger = _log.get_logger(__name__)

TIMEOUT = get_timeout()


def _build_url(cda_path: str, query_id: str) -> str:
    params = {
        "path": cda_path,
        "dataAccessId": query_id,
    }
    return f"{PENTAHO_BASE}?{urlencode(params)}"


async def _fetch_query(cda_path: str, query_id: str) -> tuple[dict[str, Any], str]:
    url = _build_url(cda_path, query_id)
    logger.debug("conab_ceasa_fetch", query=query_id)

    headers = UserAgentRotator.get_headers(source="conab_ceasa")
    headers["Accept"] = "application/json"

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=headers,
        auth=PENTAHO_AUTH,
        follow_redirects=True,
    ) as http:
        resp = await retry_on_status(
            lambda: http.get(url),
            source="conab_ceasa",
        )

    if resp.status_code != 200:
        raise SourceUnavailableError(
            source="conab_ceasa",
            url=PENTAHO_BASE,
            last_error=f"HTTP {resp.status_code}",
        )

    responses.raise_for_status(resp, source="conab_ceasa")
    data = responses.parse_json_response(resp, source="conab_ceasa", url=url)
    return data, PENTAHO_BASE


async def fetch_precos() -> tuple[dict[str, Any], str]:
    return await _fetch_query(CDA_PROHORT, QUERY_PRECOS)


async def fetch_ceasas() -> tuple[dict[str, Any], str]:
    return await _fetch_query(CDA_PROHORT, QUERY_CEASAS)
