from __future__ import annotations

import math
import re
from urllib.parse import quote

import httpx
import structlog

from agrobr.constants import MIN_WFS_SIZE, HTTPSettings
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

from .models import PAGE_SIZE, PROPERTY_NAMES, WFS_BASE, WFS_VERSION, layer_name

logger = structlog.get_logger()

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=180.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)


def _build_wfs_url(
    uf: str,
    *,
    cql_filter: str | None = None,
    count: int = PAGE_SIZE,
    start_index: int = 0,
    result_type: str | None = None,
) -> str:
    props = ",".join(PROPERTY_NAMES)
    layer = layer_name(uf)

    url = (
        f"{WFS_BASE}"
        f"?service=WFS&version={WFS_VERSION}&request=GetFeature"
        f"&typeNames=sicar:{layer}"
        f"&outputFormat=csv"
        f"&propertyName={props}"
        f"&count={count}"
        f"&startIndex={start_index}"
    )
    if result_type:
        url += f"&resultType={result_type}"
    if cql_filter:
        url += f"&CQL_FILTER={quote(cql_filter)}"
    return url


async def _fetch_url(url: str, *, base_delay: float | None = None) -> bytes:
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        logger.debug("sicar_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url),
            source="sicar",
            base_delay=base_delay,
        )

        if response.status_code == 404:
            raise SourceUnavailableError(source="sicar", url=url, last_error="HTTP 404")

        response.raise_for_status()

        content = response.content
        if len(content) < MIN_WFS_SIZE:
            raise SourceUnavailableError(
                source="sicar",
                url=url,
                last_error=(
                    f"WFS response too small ({len(content)} bytes), expected CSV feature data"
                ),
            )
        return content


async def fetch_hits(uf: str, cql_filter: str | None = None) -> int:
    url = _build_wfs_url(uf, cql_filter=cql_filter, result_type="hits")
    content = await _fetch_url(url)

    text = content.decode("utf-8", errors="replace")
    match = re.search(r'numberMatched="(\d+)"', text)
    if match:
        return int(match.group(1))

    match = re.search(r"numberMatched=(\d+)", text)
    if match:
        return int(match.group(1))

    raise ParseError(
        source="sicar",
        parser_version=1,
        reason=f"Nao encontrou numberMatched na resposta hits: {text[:200]}",
    )


async def fetch_imoveis(uf: str, cql_filter: str | None = None) -> tuple[list[bytes], str]:
    total = await fetch_hits(uf, cql_filter)
    logger.info("sicar_hits", uf=uf, total=total, cql_filter=cql_filter)

    if total == 0:
        url = _build_wfs_url(uf, cql_filter=cql_filter)
        return [], url

    n_pages = math.ceil(total / PAGE_SIZE)
    pages: list[bytes] = []
    base_url = _build_wfs_url(uf, cql_filter=cql_filter)

    for i in range(n_pages):
        start_index = i * PAGE_SIZE
        url = _build_wfs_url(
            uf,
            cql_filter=cql_filter,
            count=PAGE_SIZE,
            start_index=start_index,
        )
        delay = 2.0 if i >= 5 else None
        content = await _fetch_url(url, base_delay=delay)
        pages.append(content)
        logger.debug(
            "sicar_page",
            uf=uf,
            page=i + 1,
            total_pages=n_pages,
            size=len(content),
        )

    return pages, base_url
