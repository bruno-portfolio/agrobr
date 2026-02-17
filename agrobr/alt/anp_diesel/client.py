"""Cliente HTTP para download de dados ANP Diesel (XLSX/XLS bulk)."""

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


async def download_xlsx(url: str) -> bytes:
    """Baixa arquivo XLSX/XLS da ANP.

    Args:
        url: URL completa do arquivo.

    Returns:
        Conteudo raw do arquivo em bytes.

    Raises:
        httpx.HTTPStatusError: Se download falhar.
    """
    logger.info("anp_diesel_download", url=url)

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="anp_diesel",
        )

        response.raise_for_status()

        content = response.content
        logger.info(
            "anp_diesel_download_ok",
            url=url,
            size_bytes=len(content),
        )
        return content


async def fetch_precos_municipios(periodo: str) -> bytes:
    """Baixa XLSX de precos por municipio para um periodo.

    Args:
        periodo: Chave do periodo (ex: "2022-2023", "2024-2025", "2026").

    Returns:
        Conteudo raw do XLSX.

    Raises:
        ValueError: Se periodo invalido.
    """
    from agrobr.alt.anp_diesel.models import PRECOS_MUNICIPIOS_URLS

    if periodo not in PRECOS_MUNICIPIOS_URLS:
        raise ValueError(
            f"Periodo '{periodo}' invalido. Disponiveis: {sorted(PRECOS_MUNICIPIOS_URLS.keys())}"
        )

    url = PRECOS_MUNICIPIOS_URLS[periodo]
    return await download_xlsx(url)


async def fetch_precos_estados() -> bytes:
    """Baixa XLSX de precos por estado (todos desde 2013).

    Returns:
        Conteudo raw do XLSX.
    """
    from agrobr.alt.anp_diesel.models import PRECOS_ESTADOS_URL

    return await download_xlsx(PRECOS_ESTADOS_URL)


async def fetch_precos_brasil() -> bytes:
    """Baixa XLSX de precos Brasil (todos desde 2013).

    Returns:
        Conteudo raw do XLSX.
    """
    from agrobr.alt.anp_diesel.models import PRECOS_BRASIL_URL

    return await download_xlsx(PRECOS_BRASIL_URL)


async def fetch_vendas_m3() -> bytes:
    """Baixa XLS de volumes de venda por UF.

    Returns:
        Conteudo raw do XLS.
    """
    from agrobr.alt.anp_diesel.models import VENDAS_M3_URL

    return await download_xlsx(VENDAS_M3_URL)
