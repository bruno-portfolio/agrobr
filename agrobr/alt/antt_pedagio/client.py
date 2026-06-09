from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_CSV_SIZE
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

from .models import (
    DATASET_PRACAS_SLUG,
    DATASET_TRAFEGO_SLUG,
    build_ckan_package_url,
)

logger = structlog.get_logger()

TIMEOUT = get_timeout(read=180.0)


async def _get_ckan_resources(slug: str) -> list[dict[str, str]]:
    url = build_ckan_package_url(slug)
    logger.info("antt_pedagio_ckan_discover", slug=slug)

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="antt_pedagio"),
        follow_redirects=True,
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="antt_pedagio",
        )
        response.raise_for_status()
        try:
            data = response.json()
        except ValueError as e:
            from agrobr.exceptions import SourceUnavailableError

            preview = response.text[:200].strip().replace("\n", " ")
            raise SourceUnavailableError(
                source="antt_pedagio",
                url=url,
                last_error=(
                    f"CKAN response is not JSON (likely WAF block or portal outage). "
                    f"Content-Type: {response.headers.get('content-type', 'unknown')!r}. "
                    f"Body preview: {preview!r}"
                ),
            ) from e

    if not isinstance(data, dict) or "result" not in data:
        from agrobr.exceptions import SourceUnavailableError

        raise SourceUnavailableError(
            source="antt_pedagio",
            url=url,
            last_error=(
                "CKAN API response missing 'result' key, "
                f"got keys: {list(data.keys()) if isinstance(data, dict) else type(data).__name__}"
            ),
        )

    resources = data.get("result", {}).get("resources", [])
    result = []
    for r in resources:
        result.append(
            {
                "id": r.get("id", ""),
                "name": r.get("name", ""),
                "url": r.get("url", ""),
                "format": r.get("format", ""),
            }
        )

    logger.info(
        "antt_pedagio_ckan_discover_ok",
        slug=slug,
        resources_count=len(result),
    )
    return result


def _match_trafego_resource(resources: list[dict[str, str]], ano: int) -> str | None:
    ano_str = str(ano)
    for r in resources:
        name = r["name"].lower()
        url = r["url"].lower()
        if (ano_str in name or ano_str in url) and r["format"].upper() in ("CSV", ""):
            return r["url"]
    return None


def _match_pracas_resource(resources: list[dict[str, str]]) -> str | None:
    for r in resources:
        fmt = r["format"].upper()
        if fmt in ("CSV", ""):
            return r["url"]
    if resources:
        return resources[0]["url"]
    return None


async def download_csv(url: str) -> bytes:
    logger.debug("antt_pedagio_download", url=url)

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="antt_pedagio"),
        follow_redirects=True,
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="antt_pedagio",
        )
        response.raise_for_status()

        content = response.content
        if content[:50].lstrip().lower().startswith((b"<!doctype", b"<html")):
            from agrobr.exceptions import SourceUnavailableError

            preview = content[:200].decode("utf-8", errors="replace").replace("\n", " ")
            raise SourceUnavailableError(
                source="antt_pedagio",
                url=url,
                last_error=(
                    f"Server returned HTML instead of CSV (likely WAF block or portal outage). "
                    f"Size: {len(content)} bytes. Preview: {preview!r}"
                ),
            )

        if len(content) < MIN_CSV_SIZE:
            from agrobr.exceptions import SourceUnavailableError

            raise SourceUnavailableError(
                source="antt_pedagio",
                url=url,
                last_error=(
                    f"Downloaded CSV too small ({len(content)} bytes), expected valid CSV data"
                ),
            )

        logger.info(
            "antt_pedagio_download_ok",
            source="antt_pedagio",
            size_bytes=len(content),
        )
        return content


async def fetch_trafego(ano: int) -> bytes:
    resources = await _get_ckan_resources(DATASET_TRAFEGO_SLUG)
    url = _match_trafego_resource(resources, ano)
    if not url:
        raise ValueError(
            f"Recurso de trafego nao encontrado para ano {ano}. "
            f"Resources disponiveis: {[r['name'] for r in resources]}"
        )
    return await download_csv(url)


async def fetch_trafego_anos(anos: list[int]) -> list[tuple[int, bytes]]:
    resources = await _get_ckan_resources(DATASET_TRAFEGO_SLUG)
    results: list[tuple[int, bytes]] = []

    for ano in anos:
        url = _match_trafego_resource(resources, ano)
        if url:
            content = await download_csv(url)
            results.append((ano, content))
        else:
            logger.warning("antt_pedagio_resource_missing", ano=ano)

    return results


async def fetch_pracas() -> bytes:
    resources = await _get_ckan_resources(DATASET_PRACAS_SLUG)
    url = _match_pracas_resource(resources)
    if not url:
        raise ValueError(
            f"Recurso de cadastro de pracas nao encontrado. "
            f"Resources: {[r['name'] for r in resources]}"
        )
    return await download_csv(url)
