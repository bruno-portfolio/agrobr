"""Cliente HTTP para download de dados ComexStat (bulk CSV).

Fonte primária: CSVs anuais em
    https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/

Arquivos: EXP_YYYY.csv (exportação), IMP_YYYY.csv (importação)
Separador: ponto-e-vírgula (;)
"""

from __future__ import annotations

import httpx
import structlog

from agrobr.constants import RETRIABLE_STATUS_CODES, HTTPSettings
from agrobr.exceptions import SourceUnavailableError

logger = structlog.get_logger()

BULK_CSV_BASE = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=120.0,  # Arquivos grandes, download pode demorar
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

MAX_RETRIES = 3


async def download_csv(url: str) -> str:
    """Baixa arquivo CSV da ComexStat.

    Args:
        url: URL completa do CSV.

    Returns:
        Conteúdo do CSV como string.

    Raises:
        SourceUnavailableError: Se não conseguir baixar.
    """
    import asyncio

    logger.info("comexstat_download_csv", url=url)

    last_error = ""

    for attempt in range(MAX_RETRIES):
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
                response = await client.get(url)

                if response.status_code in RETRIABLE_STATUS_CODES:
                    last_error = f"HTTP {response.status_code}"
                    delay = 2.0 * (2 ** attempt)
                    logger.warning(
                        "comexstat_retriable_error",
                        status=response.status_code,
                        attempt=attempt + 1,
                        delay=delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                response.raise_for_status()

                content = response.text
                logger.info(
                    "comexstat_download_ok",
                    url=url,
                    size_chars=len(content),
                )
                return content

        except httpx.HTTPError as e:
            last_error = str(e)
            delay = 2.0 * (2 ** attempt)
            logger.warning(
                "comexstat_download_error",
                url=url,
                error=str(e),
                attempt=attempt + 1,
                delay=delay,
            )
            await asyncio.sleep(delay)
            continue

    raise SourceUnavailableError(
        source="comexstat",
        url=url,
        last_error=f"Falhou após {MAX_RETRIES} tentativas: {last_error}",
    )


async def fetch_exportacao_csv(ano: int) -> str:
    """Baixa CSV de exportação de um ano específico.

    Args:
        ano: Ano (ex: 2024).

    Returns:
        Conteúdo do CSV como string.
    """
    url = f"{BULK_CSV_BASE}/EXP_{ano}.csv"
    return await download_csv(url)


async def fetch_importacao_csv(ano: int) -> str:
    """Baixa CSV de importação de um ano específico.

    Args:
        ano: Ano (ex: 2024).

    Returns:
        Conteúdo do CSV como string.
    """
    url = f"{BULK_CSV_BASE}/IMP_{ano}.csv"
    return await download_csv(url)
