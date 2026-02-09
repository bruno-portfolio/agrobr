"""Cliente HTTP para dados DERAL (SEAB/PR).

Dados semanais: https://www.agricultura.pr.gov.br/system/files/publico/Safras/PC.xls
Resumo mensal:  https://www.agricultura.pr.gov.br/system/files/publico/Safras/pss.xlsx
"""

from __future__ import annotations

import asyncio

import httpx
import structlog

from agrobr.constants import RETRIABLE_STATUS_CODES, HTTPSettings
from agrobr.exceptions import SourceUnavailableError

logger = structlog.get_logger()

BASE_URL = "https://www.agricultura.pr.gov.br/system/files/publico/Safras"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=30.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {
    "User-Agent": "agrobr/0.8.0 (https://github.com/your-org/agrobr)",
    "Accept": "application/vnd.ms-excel, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, */*",
}

MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0


async def _fetch_bytes(url: str) -> bytes:
    """Fetch arquivo binário com retry.

    Args:
        url: URL completa do arquivo.

    Returns:
        Conteúdo binário do arquivo.

    Raises:
        SourceUnavailableError: Se fonte indisponível.
    """
    last_error = ""

    for attempt in range(MAX_RETRIES):
        try:
            async with httpx.AsyncClient(
                timeout=TIMEOUT, headers=HEADERS, follow_redirects=True
            ) as client:
                logger.debug("deral_request", url=url, attempt=attempt + 1)
                response = await client.get(url)

                if response.status_code == 404:
                    raise SourceUnavailableError(
                        source="deral",
                        url=url,
                        last_error="Arquivo não encontrado (404)",
                    )

                if response.status_code in RETRIABLE_STATUS_CODES:
                    last_error = f"HTTP {response.status_code}"
                    delay = RETRY_BASE_DELAY * (2**attempt)
                    logger.warning(
                        "deral_retriable_error",
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
            logger.warning("deral_timeout", attempt=attempt + 1, delay=delay)
            await asyncio.sleep(delay)

        except httpx.HTTPError as e:
            last_error = str(e)
            delay = RETRY_BASE_DELAY * (2**attempt)
            logger.warning("deral_http_error", error=str(e), attempt=attempt + 1, delay=delay)
            await asyncio.sleep(delay)

    raise SourceUnavailableError(
        source="deral",
        url=url,
        last_error=f"Falhou após {MAX_RETRIES} tentativas: {last_error}",
    )


async def fetch_pc_xls() -> bytes:
    """Baixa planilha semanal PC (Painel de Culturas).

    Atualizada semanalmente às terças-feiras.

    Returns:
        Bytes do arquivo .xls.
    """
    url = f"{BASE_URL}/PC.xls"
    logger.info("deral_fetch_pc", url=url)
    return await _fetch_bytes(url)


async def fetch_pss_xlsx() -> bytes:
    """Baixa planilha mensal PSS (resumo safra).

    Returns:
        Bytes do arquivo .xlsx.
    """
    url = f"{BASE_URL}/pss.xlsx"
    logger.info("deral_fetch_pss", url=url)
    return await _fetch_bytes(url)
