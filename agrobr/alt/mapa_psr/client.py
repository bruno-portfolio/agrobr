"""Cliente HTTP para download de dados MAPA PSR (CSV bulk)."""

from __future__ import annotations

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.http.retry import retry_on_status

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


async def download_csv(url: str) -> bytes:
    """Baixa arquivo CSV do portal MAPA dados abertos.

    Args:
        url: URL completa do arquivo CSV.

    Returns:
        Conteudo raw do arquivo em bytes.

    Raises:
        httpx.HTTPStatusError: Se download falhar.
    """
    logger.info("mapa_psr_download", url=url)

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="mapa_psr",
        )
        response.raise_for_status()

        content = response.content
        logger.info(
            "mapa_psr_download_ok",
            url=url,
            size_bytes=len(content),
        )
        return content


async def fetch_periodo(periodo: str) -> bytes:
    """Baixa CSV de apolices PSR para um periodo.

    Args:
        periodo: Chave do periodo (ex: "2006-2015", "2016-2024", "2025").

    Returns:
        Conteudo raw do CSV.

    Raises:
        ValueError: Se periodo invalido.
    """
    from agrobr.alt.mapa_psr.models import get_csv_url

    url = get_csv_url(periodo)
    return await download_csv(url)


async def fetch_periodos(periodos: list[str]) -> list[bytes]:
    """Baixa multiplos CSVs em sequencia.

    Args:
        periodos: Lista de chaves de periodo.

    Returns:
        Lista de bytes raw, um por periodo.
    """
    results: list[bytes] = []
    for periodo in periodos:
        content = await fetch_periodo(periodo)
        results.append(content)
    return results
