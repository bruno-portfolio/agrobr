from __future__ import annotations

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status

logger = structlog.get_logger()

BASE_URL = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=60.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {"User-Agent": "agrobr (https://github.com/bruno-portfolio/agrobr)"}


async def _fetch_url(url: str) -> bytes:
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        logger.debug("queimadas_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url),
            source="queimadas",
        )

        if response.status_code == 404:
            raise SourceUnavailableError(source="queimadas", url=url, last_error="HTTP 404")

        response.raise_for_status()
        return response.content


async def fetch_focos_diario(data: str) -> tuple[bytes, str]:
    """Baixa CSV de focos diarios.

    Args:
        data: Data no formato YYYYMMDD.

    Returns:
        Tupla (bytes_csv, url_usada).

    Raises:
        SourceUnavailableError: Se CSV nao encontrado.
    """
    url = f"{BASE_URL}/diario/Brasil/focos_diario_br_{data}.csv"
    content = await _fetch_url(url)
    logger.info("queimadas_csv_found", url=url, size=len(content))
    return content, url


async def fetch_focos_mensal(ano: int, mes: int) -> tuple[bytes, str]:
    """Baixa CSV de focos mensais.

    Args:
        ano: Ano (ex: 2024).
        mes: Mes (1-12).

    Returns:
        Tupla (bytes_csv, url_usada).

    Raises:
        SourceUnavailableError: Se CSV nao encontrado.
    """
    periodo = f"{ano:04d}{mes:02d}"
    url = f"{BASE_URL}/mensal/Brasil/focos_mensal_br_{periodo}.csv"
    content = await _fetch_url(url)
    logger.info("queimadas_csv_found", url=url, size=len(content))
    return content, url
