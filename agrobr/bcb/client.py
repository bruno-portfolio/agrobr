"""Cliente HTTP para API Olinda BCB/SICOR (OData REST).

A API Olinda do BCB expõe dados do SICOR via endpoints OData.
Base: https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata/

Endpoints usados (API reestruturada ~2024):
  - CusteioRegiaoUFProduto: crédito de custeio por UF/produto
  - InvestRegiaoUFProduto: crédito de investimento por UF/produto
  - ComercRegiaoUFProduto: crédito de comercialização por UF/produto

NOTA: A API Olinda v2 NÃO suporta $filter OData nos novos endpoints.
      Filtragem é feita client-side após download paginado.

TODO v0.8: fallback Base dos Dados / CSV bulk download quando API Olinda indisponível
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
    read=120.0,  # BCB Olinda é notoriamente lento
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {"User-Agent": "agrobr/0.7.1 (https://github.com/your-org/agrobr)"}

PAGE_SIZE = 10000
MAX_RETRIES = 6
RETRY_BASE_DELAY = 2.0

# Mapeamento de finalidade para endpoint (API reestruturada ~2024)
# Endpoints antigos (CusteioMunicipio, InvestimentoMunicipio) foram removidos.
# Novos endpoints UF-level não suportam $filter — filtragem client-side.
ENDPOINT_MAP: dict[str, str] = {
    "custeio": "CusteioRegiaoUFProduto",
    "investimento": "InvestRegiaoUFProduto",
    "comercializacao": "ComercRegiaoUFProduto",
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
            async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
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
                return response.json()  # type: ignore[no-any-return]

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

    A API Olinda v2 NÃO suporta $filter nos novos endpoints.
    Filtragem por produto/ano é feita client-side após download.

    Args:
        finalidade: "custeio", "investimento", "comercializacao".
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

    # API Olinda v2: $filter suporta apenas contains() isolado (sem 'and').
    # Filtramos por produto server-side e o resto client-side.
    server_filter: list[str] | None = None
    if produto_sicor:
        server_filter = [f"contains(nomeProduto,'{produto_sicor}')"]

    logger.info(
        "bcb_fetch_credito",
        endpoint=endpoint,
        produto=produto_sicor,
        safra=safra_sicor,
        uf=cd_uf,
        server_filter=server_filter,
    )

    all_records: list[dict[str, Any]] = []
    skip = 0

    while True:
        data = await _fetch_odata(
            endpoint=endpoint,
            filters=server_filter,
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
        "bcb_fetch_credito_raw",
        total_records=len(all_records),
        endpoint=endpoint,
    )

    if not all_records:
        return all_records

    # Filtragem client-side para campos que o server não suporta filtrar.
    # Produto já é filtrado server-side via contains().
    filtered = all_records

    if safra_sicor:
        ano_emissao = safra_sicor.split("/")[0]
        filtered = [
            r for r in filtered
            if str(r.get("AnoEmissao", "")) == ano_emissao
        ]

    if cd_uf:
        filtered = [
            r for r in filtered
            if str(r.get("cdEstado", "")) == cd_uf
               or str(r.get("nomeUF", "")).upper() == cd_uf.upper()
        ]

    logger.info(
        "bcb_fetch_credito_ok",
        total_raw=len(all_records),
        total_filtered=len(filtered),
        endpoint=endpoint,
    )

    return filtered


def _match_produto(nome_api: str, produto_upper: str) -> bool:
    """Verifica match de produto considerando aspas embarcadas do BCB."""
    cleaned = nome_api.strip().strip('"').upper()
    return cleaned == produto_upper
