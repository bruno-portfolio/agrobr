from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_CSV_SIZE
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

from .models import (
    DATASET_SLUG,
    build_ckan_package_url,
    extract_safras,
    match_safra_resource,
)

logger = structlog.get_logger()

TIMEOUT = get_timeout(read=180.0)


async def discover_resources() -> list[dict[str, str]]:
    url = build_ckan_package_url(DATASET_SLUG)
    logger.info("zarc_ckan_discover", slug=DATASET_SLUG)

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="zarc"),
        follow_redirects=True,
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="zarc",
        )
        response.raise_for_status()
        data = responses.parse_json_response(response, source="zarc", url=url)

    if not isinstance(data, dict) or "result" not in data:
        raise SourceUnavailableError(
            source="zarc",
            url=url,
            last_error=f"CKAN response missing 'result', got: {type(data).__name__}",
        )

    resources = data["result"].get("resources", [])
    result = [
        {
            "id": r.get("id", ""),
            "name": r.get("name", ""),
            "url": r.get("url", ""),
            "format": r.get("format", ""),
        }
        for r in resources
    ]
    logger.info("zarc_ckan_discover_ok", resources_count=len(result))
    return result


async def download_csv(url: str) -> bytes:
    logger.debug("zarc_download", url=url)

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="zarc"),
        follow_redirects=True,
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="zarc",
        )
        response.raise_for_status()
        content = response.content

        io_utils.validate_download(
            content,
            kinds=("csv",),
            source="zarc",
            url=url,
            min_size=MIN_CSV_SIZE,
        )

    logger.info("zarc_download_ok", source="zarc", size_bytes=len(content))
    return content


async def fetch_tabua_risco(
    safra: str, resources: list[dict[str, str]] | None = None
) -> tuple[bytes, str]:
    if resources is None:
        resources = await discover_resources()
    url = match_safra_resource(resources, safra)
    if not url:
        raise InvalidParameterError(
            f"Safra '{safra}' não encontrada. Disponíveis: {extract_safras(resources)}"
        )
    content = await download_csv(url)
    return content, url
