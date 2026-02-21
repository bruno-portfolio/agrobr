from __future__ import annotations

from typing import Any

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status

logger = structlog.get_logger()

BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=120.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {"User-Agent": "agrobr/0.7.1 (https://github.com/your-org/agrobr)"}

PAGE_SIZE = 10000
BCB_MAX_RETRIES = 6

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

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        logger.debug(
            "bcb_odata_request",
            endpoint=endpoint,
            skip=skip,
            top=top,
        )

        response = await retry_on_status(
            lambda: client.get(url, params=params),
            source="bcb",
            max_attempts=BCB_MAX_RETRIES,
        )

        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]


async def fetch_credito_rural(
    finalidade: str = "custeio",
    produto_sicor: str | None = None,
    safra_sicor: str | None = None,
    cd_uf: str | None = None,
) -> list[dict[str, Any]]:
    endpoint = ENDPOINT_MAP.get(finalidade.lower())
    if not endpoint:
        raise ValueError(
            f"Finalidade inválida: '{finalidade}'. Opções: {list(ENDPOINT_MAP.keys())}"
        )

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

    filtered = all_records

    if safra_sicor:
        ano_emissao = safra_sicor.split("/")[0]
        filtered = [r for r in filtered if str(r.get("AnoEmissao", "")) == ano_emissao]

    if cd_uf:
        filtered = [
            r
            for r in filtered
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
    cleaned = nome_api.strip().strip('"').upper()
    return cleaned == produto_upper


async def fetch_credito_rural_with_fallback(
    finalidade: str = "custeio",
    produto_sicor: str | None = None,
    safra_sicor: str | None = None,
    cd_uf: str | None = None,
) -> tuple[list[dict[str, Any]], str]:
    odata_error_msg = ""
    try:
        records = await fetch_credito_rural(
            finalidade=finalidade,
            produto_sicor=produto_sicor,
            safra_sicor=safra_sicor,
            cd_uf=cd_uf,
        )
        return records, "odata"

    except SourceUnavailableError as odata_err:
        odata_error_msg = odata_err.last_error
        logger.warning(
            "bcb_odata_fallback",
            error=str(odata_err),
            reason="Tentando fallback BigQuery",
        )

    try:
        from agrobr.bcb.bigquery_client import fetch_credito_rural_bigquery

        records = await fetch_credito_rural_bigquery(
            finalidade=finalidade,
            produto_sicor=produto_sicor,
            safra_sicor=safra_sicor,
            cd_uf=cd_uf,
        )
        return records, "bigquery"

    except SourceUnavailableError as bq_err:
        raise SourceUnavailableError(
            source="bcb",
            url=f"{BASE_URL}/{ENDPOINT_MAP.get(finalidade.lower(), '')}",
            last_error=(
                f"Ambas as fontes falharam. OData: {odata_error_msg}; BigQuery: {bq_err.last_error}"
            ),
        ) from bq_err
