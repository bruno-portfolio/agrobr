"""Cliente HTTP para download de dados ANTT Pedagio (CKAN)."""

from __future__ import annotations

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.http.retry import retry_on_status

from .models import (
    DATASET_PRACAS_SLUG,
    DATASET_TRAFEGO_SLUG,
    build_ckan_package_url,
)

logger = structlog.get_logger()

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=180.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


async def _get_ckan_resources(slug: str) -> list[dict[str, str]]:
    """Busca lista de resources de um dataset CKAN.

    Returns:
        Lista de dicts com 'id', 'name', 'url', 'format'.
    """
    url = build_ckan_package_url(slug)
    logger.info("antt_pedagio_ckan_discover", slug=slug)

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="antt_pedagio",
        )
        response.raise_for_status()
        data = response.json()

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
    """Encontra URL do CSV de trafego para um ano especifico."""
    ano_str = str(ano)
    for r in resources:
        name = r["name"].lower()
        url = r["url"].lower()
        # Match by year in name or URL
        if (ano_str in name or ano_str in url) and r["format"].upper() in ("CSV", ""):
            return r["url"]
    return None


def _match_pracas_resource(resources: list[dict[str, str]]) -> str | None:
    """Encontra URL do CSV de cadastro de pracas."""
    for r in resources:
        fmt = r["format"].upper()
        if fmt in ("CSV", ""):
            return r["url"]
    # Fallback: return first resource
    if resources:
        return resources[0]["url"]
    return None


async def download_csv(url: str) -> bytes:
    """Baixa arquivo CSV da ANTT.

    Args:
        url: URL completa do arquivo.

    Returns:
        Conteudo raw em bytes.

    Raises:
        httpx.HTTPStatusError: Se download falhar.
    """
    logger.info("antt_pedagio_download", url=url)

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="antt_pedagio",
        )
        response.raise_for_status()

        content = response.content
        logger.info(
            "antt_pedagio_download_ok",
            url=url,
            size_bytes=len(content),
        )
        return content


async def fetch_trafego(ano: int) -> bytes:
    """Baixa CSV de trafego para um ano via CKAN discovery.

    Args:
        ano: Ano dos dados (2010-2025).

    Returns:
        Conteudo raw do CSV.

    Raises:
        ValueError: Se recurso nao encontrado para o ano.
    """
    resources = await _get_ckan_resources(DATASET_TRAFEGO_SLUG)
    url = _match_trafego_resource(resources, ano)
    if not url:
        raise ValueError(
            f"Recurso de trafego nao encontrado para ano {ano}. "
            f"Resources disponiveis: {[r['name'] for r in resources]}"
        )
    return await download_csv(url)


async def fetch_trafego_anos(anos: list[int]) -> list[tuple[int, bytes]]:
    """Baixa CSVs de trafego para multiplos anos em sequencia.

    Args:
        anos: Lista de anos.

    Returns:
        Lista de tuplas (ano, bytes_raw).
    """
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
    """Baixa CSV do cadastro de pracas de pedagio.

    Returns:
        Conteudo raw do CSV.

    Raises:
        ValueError: Se recurso nao encontrado.
    """
    resources = await _get_ckan_resources(DATASET_PRACAS_SLUG)
    url = _match_pracas_resource(resources)
    if not url:
        raise ValueError(
            f"Recurso de cadastro de pracas nao encontrado. "
            f"Resources: {[r['name'] for r in resources]}"
        )
    return await download_csv(url)
