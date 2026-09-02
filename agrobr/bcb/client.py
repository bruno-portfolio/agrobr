from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx
import structlog

from agrobr.constants import URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.normalize.dates import INICIO_SAFRA_MES

from . import models

logger = structlog.get_logger()

BASE_URL = URLS[Fonte.BCB]["base"]

TIMEOUT = get_timeout(read=120.0)

PAGE_SIZE = 10000
FALLBACK_PAGE_SIZES = (2000, 500)
BCB_MAX_RETRIES = 6

ENDPOINT_MAP: dict[str, str] = {
    "custeio": "CusteioRegiaoUFProduto",
    "investimento": "InvestRegiaoUFProduto",
    "comercializacao": "ComercRegiaoUFProduto",
}

_SELECT_COMUM = [
    "nomeProduto",
    "nomeRegiao",
    "nomeUF",
    "MesEmissao",
    "AnoEmissao",
    "cdPrograma",
    "cdSubPrograma",
    "cdFonteRecurso",
    "cdTipoSeguro",
    "Atividade",
    "cdModalidade",
]

SELECT_MAP: dict[str, list[str]] = {
    "custeio": [*_SELECT_COMUM, "QtdCusteio", "VlCusteio", "AreaCusteio"],
    "investimento": [*_SELECT_COMUM, "QtdInvest", "VlInvest"],
    "comercializacao": [*_SELECT_COMUM, "QtdComerc", "VlComerc"],
}


async def _fetch_odata(
    endpoint: str,
    filters: list[str] | None = None,
    select: list[str] | None = None,
    top: int = PAGE_SIZE,
    skip: int = 0,
) -> dict[str, Any]:
    parts = [f"$format=json&$top={top}&$skip={skip}"]

    if filters:
        parts.append("$filter=" + quote(" and ".join(filters), safe="(),'"))

    if select:
        parts.append("$select=" + ",".join(select))

    url = f"{BASE_URL}/{endpoint}?" + "&".join(parts)

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        logger.debug(
            "bcb_odata_request",
            endpoint=endpoint,
            skip=skip,
            top=top,
        )

        response = await retry_on_status(
            lambda: client.get(url),
            source="bcb",
            max_attempts=BCB_MAX_RETRIES,
        )

        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]


def _pertence_a_safra(record: dict[str, Any], ano_inicio: int) -> bool:
    try:
        ano = int(record.get("AnoEmissao") or 0)
        mes = int(record.get("MesEmissao") or 0)
    except (TypeError, ValueError):
        return False
    if mes >= INICIO_SAFRA_MES:
        return ano == ano_inicio
    return ano == ano_inicio + 1


def _resolve_uf_sigla(cd_uf: str | None) -> str | None:
    if cd_uf is None:
        return None

    value = cd_uf.strip().upper()
    if value in models.UF_CODES:
        return value

    for sigla, codigo in models.UF_CODES.items():
        if codigo == value:
            return sigla

    raise ValueError(f"Codigo de UF invalido: {cd_uf!r}")


def _safra_odata_filter(safra_sicor: str) -> str:
    try:
        ano_inicio = int(safra_sicor.split("/")[0])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Safra SICOR invalida: {safra_sicor!r}") from exc

    mes_inicio = f"{INICIO_SAFRA_MES:02d}"
    return (
        f"((AnoEmissao eq '{ano_inicio}' and MesEmissao ge '{mes_inicio}') or "
        f"(AnoEmissao eq '{ano_inicio + 1}' and MesEmissao lt '{mes_inicio}'))"
    )


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

    uf_sigla = _resolve_uf_sigla(cd_uf)
    server_filters: list[str] = []
    if produto_sicor:
        safe = produto_sicor.replace("'", "''")
        server_filters.append(f"contains(nomeProduto,'{safe}')")
    if uf_sigla:
        server_filters.append(f"nomeUF eq '{uf_sigla}'")
    if safra_sicor:
        server_filters.append(_safra_odata_filter(safra_sicor))

    logger.info(
        "bcb_fetch_credito",
        endpoint=endpoint,
        produto=produto_sicor,
        safra=safra_sicor,
        uf=uf_sigla,
        server_filters=server_filters,
    )

    all_records: list[dict[str, Any]] = []
    skip = 0

    select = SELECT_MAP.get(finalidade.lower())
    page_size = PAGE_SIZE

    while True:
        try:
            data = await _fetch_odata(
                endpoint=endpoint,
                filters=server_filters or None,
                select=select,
                top=page_size,
                skip=skip,
            )
        except SourceUnavailableError:
            menores = [p for p in FALLBACK_PAGE_SIZES if p < page_size]
            if not menores:
                raise
            page_size = menores[0]
            logger.warning(
                "bcb_page_size_reduzido",
                endpoint=endpoint,
                page_size=page_size,
                hint="Olinda instavel com paginas grandes; reduzindo",
            )
            continue

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

        if len(records) < page_size:
            break

        skip += page_size

    logger.info(
        "bcb_fetch_credito_raw",
        total_records=len(all_records),
        endpoint=endpoint,
    )

    if not all_records:
        return all_records

    filtered = all_records

    if safra_sicor:
        ano_inicio = int(safra_sicor.split("/")[0])
        filtered = [r for r in filtered if _pertence_a_safra(r, ano_inicio)]

    if uf_sigla:
        filtered = [r for r in filtered if str(r.get("nomeUF", "")).strip().upper() == uf_sigla]

    logger.info(
        "bcb_fetch_credito_ok",
        total_raw=len(all_records),
        total_filtered=len(filtered),
        endpoint=endpoint,
    )

    return filtered


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

    except (SourceUnavailableError, httpx.HTTPStatusError) as odata_err:
        odata_error_msg = getattr(odata_err, "last_error", str(odata_err))
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
