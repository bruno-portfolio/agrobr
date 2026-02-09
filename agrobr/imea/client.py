"""Cliente HTTP para API pública IMEA.

API: https://api1.imea.com.br/api/v2/mobile/cadeias/{CadeiaId}/cotacoes
Sem autenticação requerida.

Dados de cotações, indicadores e progresso de safra para Mato Grosso.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import structlog

from agrobr.constants import RETRIABLE_STATUS_CODES, HTTPSettings
from agrobr.exceptions import SourceUnavailableError

logger = structlog.get_logger()

BASE_URL = "https://api1.imea.com.br/api"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=30.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {
    "User-Agent": "agrobr/0.8.0 (https://github.com/your-org/agrobr)",
    "Accept": "application/json",
}

MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0


async def _fetch_json(url: str) -> list[dict[str, Any]]:
    """Fetch JSON da API IMEA com retry.

    Args:
        url: URL completa.

    Returns:
        Lista de dicts com dados JSON.

    Raises:
        SourceUnavailableError: Se API indisponível.
    """
    last_error = ""

    for attempt in range(MAX_RETRIES):
        try:
            async with httpx.AsyncClient(
                timeout=TIMEOUT, headers=HEADERS, follow_redirects=True
            ) as client:
                logger.debug("imea_request", url=url, attempt=attempt + 1)
                response = await client.get(url)

                if response.status_code in RETRIABLE_STATUS_CODES:
                    last_error = f"HTTP {response.status_code}"
                    delay = RETRY_BASE_DELAY * (2**attempt)
                    logger.warning(
                        "imea_retriable_error",
                        status=response.status_code,
                        attempt=attempt + 1,
                        delay=delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                response.raise_for_status()
                data = response.json()
                return data if isinstance(data, list) else []

        except SourceUnavailableError:
            raise

        except httpx.TimeoutException as e:
            last_error = f"Timeout: {e}"
            delay = RETRY_BASE_DELAY * (2**attempt)
            logger.warning("imea_timeout", attempt=attempt + 1, delay=delay)
            await asyncio.sleep(delay)

        except httpx.HTTPError as e:
            last_error = str(e)
            delay = RETRY_BASE_DELAY * (2**attempt)
            logger.warning("imea_http_error", error=str(e), attempt=attempt + 1, delay=delay)
            await asyncio.sleep(delay)

    raise SourceUnavailableError(
        source="imea",
        url=url,
        last_error=f"Falhou após {MAX_RETRIES} tentativas: {last_error}",
    )


async def fetch_cotacoes(cadeia_id: int) -> list[dict[str, Any]]:
    """Busca cotações/indicadores para uma cadeia produtiva.

    Args:
        cadeia_id: ID da cadeia (1=algodão, 2=boi, 3=milho, 4=soja).

    Returns:
        Lista de dicts com registros de cotações.
    """
    url = f"{BASE_URL}/v2/mobile/cadeias/{cadeia_id}/cotacoes"
    logger.info("imea_fetch_cotacoes", cadeia_id=cadeia_id, url=url)
    return await _fetch_json(url)


async def fetch_indicadores() -> list[dict[str, Any]]:
    """Busca resumo geral de indicadores de todas as cadeias.

    Returns:
        Lista de dicts com indicadores resumidos.
    """
    url = f"{BASE_URL}/indicador"
    logger.info("imea_fetch_indicadores", url=url)
    data = await _fetch_json(url)
    # A API retorna um único dict com arrays internas
    return data if isinstance(data, list) else [data] if data else []
