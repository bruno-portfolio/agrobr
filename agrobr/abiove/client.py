"""Cliente HTTP para download de dados ABIOVE.

ABIOVE publica planilhas Excel de exportação do complexo soja+milho.
URL pattern: https://abiove.org.br/abiove_content/Abiove/exp_YYYYMM.xlsx

O cliente tenta do mês mais recente para trás até encontrar arquivo válido.
"""

from __future__ import annotations

import asyncio

import httpx
import structlog

from agrobr.constants import RETRIABLE_STATUS_CODES, HTTPSettings
from agrobr.exceptions import SourceUnavailableError

logger = structlog.get_logger()

BASE_URL = "https://abiove.org.br/abiove_content/Abiove"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=60.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {"User-Agent": "agrobr/0.8.0 (https://github.com/your-org/agrobr)"}

MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0


async def _fetch_url(url: str) -> bytes:
    """Fetch URL com retry exponencial.

    Args:
        url: URL para download.

    Returns:
        Bytes do conteúdo.

    Raises:
        SourceUnavailableError: Se URL indisponível após retries.
    """
    last_error = ""

    for attempt in range(MAX_RETRIES):
        try:
            async with httpx.AsyncClient(
                timeout=TIMEOUT, headers=HEADERS, follow_redirects=True
            ) as client:
                logger.debug("abiove_request", url=url, attempt=attempt + 1)
                response = await client.get(url)

                if response.status_code == 404:
                    raise SourceUnavailableError(source="abiove", url=url, last_error="HTTP 404")

                if response.status_code in RETRIABLE_STATUS_CODES:
                    last_error = f"HTTP {response.status_code}"
                    delay = RETRY_BASE_DELAY * (2**attempt)
                    logger.warning(
                        "abiove_retriable_error",
                        status=response.status_code,
                        attempt=attempt + 1,
                        delay=delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                response.raise_for_status()
                return response.content

        except SourceUnavailableError:
            raise

        except httpx.TimeoutException as e:
            last_error = f"Timeout: {e}"
            delay = RETRY_BASE_DELAY * (2**attempt)
            logger.warning("abiove_timeout", attempt=attempt + 1, delay=delay)
            await asyncio.sleep(delay)

        except httpx.HTTPError as e:
            last_error = str(e)
            delay = RETRY_BASE_DELAY * (2**attempt)
            logger.warning("abiove_http_error", error=str(e), attempt=attempt + 1, delay=delay)
            await asyncio.sleep(delay)

    raise SourceUnavailableError(
        source="abiove",
        url=url,
        last_error=f"Falhou após {MAX_RETRIES} tentativas: {last_error}",
    )


async def fetch_exportacao_excel(ano: int, mes: int | None = None) -> tuple[bytes, str]:
    """Baixa planilha de exportação ABIOVE.

    Tenta do mês mais recente para trás até encontrar arquivo válido.

    Args:
        ano: Ano de referência.
        mes: Mês específico. Se None, tenta do 12 ao 1.

    Returns:
        Tupla (bytes_excel, url_usada).

    Raises:
        SourceUnavailableError: Se nenhum arquivo encontrado.
    """
    meses = [mes] if mes else list(range(12, 0, -1))

    last_error = ""

    for m in meses:
        url = f"{BASE_URL}/exp_{ano:04d}{m:02d}.xlsx"
        try:
            data = await _fetch_url(url)
            logger.info("abiove_excel_found", url=url, size=len(data))
            return data, url
        except SourceUnavailableError as e:
            last_error = e.last_error
            if mes:
                raise
            logger.debug("abiove_month_not_found", ano=ano, mes=m)
            continue

    raise SourceUnavailableError(
        source="abiove",
        url=f"{BASE_URL}/exp_{ano}*.xlsx",
        last_error=f"Nenhum arquivo encontrado para {ano}: {last_error}",
    )
