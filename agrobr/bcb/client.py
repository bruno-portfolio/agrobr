"""Cliente HTTP para API Olinda BCB/SICOR (OData REST).

A API Olinda do BCB expõe dados do SICOR via endpoints OData.
Base: https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata/

Endpoints usados:
  - CusteioMunicipio: crédito de custeio por município
  - InvestimentoMunicipio: crédito de investimento por município
"""

from __future__ import annotations

from typing import Any

import httpx
import structlog

from agrobr.constants import RETRIABLE_STATUS_CODES, HTTPSettings
from agrobr.exceptions import SourceUnavailableError

logger = structlog.get_logger()

BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=60.0,  # BCB é notoriamente lento
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

PAGE_SIZE = 10000
MAX_RETRIES = 4
RETRY_BASE_DELAY = 2.0

# Mapeamento de finalidade para endpoint
ENDPOINT_MAP: dict[str, str] = {
    "custeio": "CusteioMunicipio",
    "investimento": "InvestimentoMunicipio",
    "comercializacao": "ComercializacaoMunicipio",
    "industrializacao": "IndustrializacaoMunicipio",
}


async def _fetch_odata(
    endpoint: str,
    filters: list[str] | None = None,
    select: list[str] | None = None,
    top: int = PAGE_SIZE,
    skip: int = 0,
) -> dict[str, Any]:
    """Faz query OData na API Olinda BCB.

    Args:
        endpoint: Nome do entity set (ex: "CusteioMunicipio").
        filters: Lista de filtros OData (ex: ["Safra eq '2023/2024'"]).
        select: Colunas a retornar.
        top: Tamanho da página.
        skip: Offset para paginação.

    Returns:
        Dict com resposta JSON da API.

    Raises:
        NetworkError: Erro de rede.
        SourceUnavailableError: API indisponível após retries.
    """
    url = f"{BASE_URL}/{endpoint}"

    params: dict[str, str] = {
        "$format": "json",
        "$top": str(top),
        "$skip": str(skip),
    }

    if filters:
        params["$filter"] = " and ".join(filters)

    if select:
        params["$select"] = ",".join(select)

    import asyncio

    last_error: str = ""

    for attempt in range(MAX_RETRIES):
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
                logger.debug(
                    "bcb_odata_request",
                    endpoint=endpoint,
                    attempt=attempt + 1,
                    skip=skip,
                    top=top,
                )

                response = await client.get(url, params=params)

                if response.status_code in RETRIABLE_STATUS_CODES:
                    last_error = f"HTTP {response.status_code}"
                    delay = RETRY_BASE_DELAY * (2**attempt)
                    logger.warning(
                        "bcb_retriable_error",
                        status=response.status_code,
                        attempt=attempt + 1,
                        delay=delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                response.raise_for_status()
                return response.json()

        except httpx.TimeoutException as e:
            last_error = f"Timeout: {e}"
            delay = RETRY_BASE_DELAY * (2**attempt)
            logger.warning(
                "bcb_timeout",
                attempt=attempt + 1,
                delay=delay,
            )
            await asyncio.sleep(delay)
            continue

        except httpx.HTTPError as e:
            last_error = str(e)
            delay = RETRY_BASE_DELAY * (2**attempt)
            logger.warning(
                "bcb_http_error",
                error=str(e),
                attempt=attempt + 1,
                delay=delay,
            )
            await asyncio.sleep(delay)
            continue

    raise SourceUnavailableError(
        source="bcb",
        url=url,
        last_error=f"Falhou após {MAX_RETRIES} tentativas: {last_error}",
    )


async def fetch_credito_rural(
    finalidade: str = "custeio",
    produto_sicor: str | None = None,
    safra_sicor: str | None = None,
    cd_uf: str | None = None,
) -> list[dict[str, Any]]:
    """Busca dados de crédito rural da API SICOR com paginação automática.

    Args:
        finalidade: "custeio", "investimento", "comercializacao", "industrializacao".
        produto_sicor: Nome do produto no formato SICOR (ex: "SOJA").
        safra_sicor: Safra no formato SICOR (ex: "2023/2024").
        cd_uf: Código UF IBGE (ex: "51" para MT).

    Returns:
        Lista de dicts com registros de crédito.

    Raises:
        SourceUnavailableError: Se API indisponível.
        ValueError: Se finalidade inválida.
    """
    endpoint = ENDPOINT_MAP.get(finalidade.lower())
    if not endpoint:
        raise ValueError(
            f"Finalidade inválida: '{finalidade}'. Opções: {list(ENDPOINT_MAP.keys())}"
        )

    filters: list[str] = []
    if produto_sicor:
        filters.append(f"Produto eq '{produto_sicor}'")
    if safra_sicor:
        filters.append(f"Safra eq '{safra_sicor}'")
    if cd_uf:
        filters.append(f"cdUF eq '{cd_uf}'")

    logger.info(
        "bcb_fetch_credito",
        endpoint=endpoint,
        produto=produto_sicor,
        safra=safra_sicor,
        uf=cd_uf,
    )

    all_records: list[dict[str, Any]] = []
    skip = 0

    while True:
        data = await _fetch_odata(
            endpoint=endpoint,
            filters=filters if filters else None,
            top=PAGE_SIZE,
            skip=skip,
        )

        records = data.get("value", [])
        if not records:
            break

        all_records.extend(records)
        logger.debug(
            "bcb_page_fetched",
            skip=skip,
            records_in_page=len(records),
            total_so_far=len(all_records),
        )

        if len(records) < PAGE_SIZE:
            break

        skip += PAGE_SIZE

    logger.info(
        "bcb_fetch_credito_ok",
        total_records=len(all_records),
        endpoint=endpoint,
    )

    return all_records
