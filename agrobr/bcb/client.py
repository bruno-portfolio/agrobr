from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, cast
from urllib.parse import quote

import httpx
import pandas as pd
import pydantic

from agrobr import _log
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.normalize.dates import INICIO_SAFRA_MES
from agrobr.utils.time import utcnow_aware

from . import models, parser

logger = _log.get_logger(__name__)

BASE_URL = URLS[Fonte.BCB]["base"]

TIMEOUT = get_timeout(read=120.0)

SICOR_RECORD_LIMIT = 100_000
BCB_MAX_RETRIES = 6

ENDPOINT_MAP: dict[str, str] = {
    "custeio": "CusteioRegiaoUFProduto",
    "investimento": "InvestRegiaoUFProduto",
    "comercializacao": "ComercRegiaoUFProduto",
}
TOTAL_ENDPOINT = "RegiaoUF"

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


@dataclass
class AquisicaoOData:
    consulta: str | None = None
    paginas: list[dict[str, Any]] = field(default_factory=list)


_AQUISICAO: ContextVar[AquisicaoOData | None] = ContextVar("bcb_sicor_aquisicao", default=None)


class _EnvelopeOData(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="allow")

    value: list[dict[str, Any]]


@contextmanager
def registrar_aquisicao() -> Iterator[AquisicaoOData]:
    """Recolhe a consulta e cada página OData (URL, SHA-256, bytes e hora) pedidas no bloco."""
    aquisicao = AquisicaoOData()
    token = _AQUISICAO.set(aquisicao)
    try:
        yield aquisicao
    finally:
        _AQUISICAO.reset(token)


def odata_url(
    endpoint: str,
    filters: list[str] | None = None,
    select: list[str] | None = None,
    top: int | None = SICOR_RECORD_LIMIT,
) -> str:
    parts = ["$format=json"] + ([f"$top={top}"] if top is not None else [])

    if filters:
        parts.append("$filter=" + quote(" and ".join(filters), safe="(),'"))

    if select:
        parts.append("$select=" + ",".join(select))

    return f"{BASE_URL}/{endpoint}?" + "&".join(parts)


def _registrar_consulta(endpoint: str, filters: list[str], select: list[str] | None) -> None:
    if (aquisicao := _AQUISICAO.get()) is not None:
        aquisicao.consulta = odata_url(endpoint, filters or None, select, top=None)


async def _fetch_odata(
    endpoint: str,
    filters: list[str] | None = None,
    select: list[str] | None = None,
    top: int = SICOR_RECORD_LIMIT,
) -> dict[str, Any]:
    url = odata_url(endpoint, filters, select, top)

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        logger.debug(
            "bcb_odata_request",
            endpoint=endpoint,
            top=top,
        )

        response = await retry_on_status(
            lambda: client.get(url),
            source="bcb",
            max_attempts=BCB_MAX_RETRIES,
        )

        responses.raise_for_status(response, source="bcb")
        if (aquisicao := _AQUISICAO.get()) is not None:
            aquisicao.paginas.append(
                {
                    "url": url,
                    "sha256": hashlib.sha256(response.content).hexdigest(),
                    "bytes": len(response.content),
                    "fetched_at": utcnow_aware(),
                }
            )
        payload = responses.parse_json_response(response, source="bcb", url=url)
        try:
            _EnvelopeOData.model_validate(payload)
        except pydantic.ValidationError as exc:
            raise ParseError(
                source="bcb",
                parser_version=2,
                reason="Envelope SICOR inválido: exige objeto com value lista de registros",
                errors=[("bcb", "parse", str(exc))],
            ) from exc
        return cast(dict[str, Any], payload)


def _na_safra(records: list[dict[str, Any]], ano_inicio: int) -> list[dict[str, Any]]:
    """Os registros emitidos na safra que começa em julho de `ano_inicio`.

    Ano ou mês que não identifica a safra (texto, fração ou ausente) levanta `ParseError` com a
    coluna e o registro, em vez de tirar o registro da safra calado.
    """
    emissao = {
        campo: parser._inteiros(
            pd.Series([record.get(campo) for record in records], name=campo, dtype=object)
        )
        for campo in ("AnoEmissao", "MesEmissao")
    }
    for campo, valores in emissao.items():
        if valores.isna().any():
            posicao = int(valores.isna().to_numpy().argmax())
            raise ParseError(
                source="bcb",
                parser_version=parser.PARSER_VERSION,
                reason=f"{campo} ausente no registro {posicao + 1}: a safra do registro não tem como "
                "ser identificada",
            )
    anos, meses = emissao["AnoEmissao"], emissao["MesEmissao"]
    inicio = anos.where(meses >= INICIO_SAFRA_MES, anos - 1)
    return [record for record, ano in zip(records, inicio, strict=True) if ano == ano_inicio]


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


def _safra_odata_filter(ano_inicio: int) -> str:
    mes_inicio = f"{INICIO_SAFRA_MES:02d}"
    return (
        f"((AnoEmissao eq '{ano_inicio}' and MesEmissao ge '{mes_inicio}') or "
        f"(AnoEmissao eq '{ano_inicio + 1}' and MesEmissao lt '{mes_inicio}'))"
    )


async def _fetch_credito_por_mes(
    endpoint: str,
    filters: list[str],
    select: list[str] | None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for mes in range(1, 13):
        month_filters = [*filters, f"MesEmissao eq '{mes:02d}'"]
        data = await _fetch_odata(
            endpoint=endpoint,
            filters=month_filters,
            select=select,
            top=SICOR_RECORD_LIMIT,
        )
        month_records = cast(list[dict[str, Any]], data["value"])
        if len(month_records) == SICOR_RECORD_LIMIT:
            raise SourceUnavailableError(
                source="bcb",
                url=f"{BASE_URL}/{endpoint}",
                last_error="volume acima do limite da Olinda",
            )
        records.extend(month_records)

    return records


async def _fetch_credito_records(
    endpoint: str,
    filters: list[str],
    select: list[str] | None,
) -> list[dict[str, Any]]:
    data = await _fetch_odata(
        endpoint=endpoint,
        filters=filters or None,
        select=select,
        top=SICOR_RECORD_LIMIT,
    )
    records = cast(list[dict[str, Any]], data["value"])
    if len(records) != SICOR_RECORD_LIMIT:
        return records

    logger.warning(
        "bcb_volume_fatiado_por_mes",
        endpoint=endpoint,
        record_limit=SICOR_RECORD_LIMIT,
    )
    return await _fetch_credito_por_mes(endpoint, filters, select)


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
    ano_inicio = (
        int(models.normalize_safra_sicor(safra_sicor).split("/")[0]) if safra_sicor else None
    )
    server_filters: list[str] = []
    if produto_sicor:
        safe = produto_sicor.replace("'", "''")
        server_filters.append(f"nomeProduto eq '{safe}'")
    if uf_sigla:
        server_filters.append(f"nomeUF eq '{uf_sigla}'")
    if ano_inicio is not None:
        server_filters.append(_safra_odata_filter(ano_inicio))

    logger.info(
        "bcb_fetch_credito",
        endpoint=endpoint,
        produto=produto_sicor,
        safra=safra_sicor,
        uf=uf_sigla,
        server_filters=server_filters,
    )

    select = SELECT_MAP.get(finalidade.lower())
    _registrar_consulta(endpoint, server_filters, select)
    all_records = await _fetch_credito_records(endpoint, server_filters, select)

    logger.info(
        "bcb_fetch_credito_raw",
        total_records=len(all_records),
        endpoint=endpoint,
    )

    if not all_records:
        return all_records

    filtered = all_records

    if ano_inicio is not None:
        filtered = _na_safra(filtered, ano_inicio)

    if uf_sigla:
        filtered = [r for r in filtered if str(r.get("nomeUF", "")).strip().upper() == uf_sigla]

    logger.info(
        "bcb_fetch_credito_ok",
        total_raw=len(all_records),
        total_filtered=len(filtered),
        endpoint=endpoint,
    )

    return filtered


async def fetch_credito_rural_total(
    safra_sicor: str | None = None,
    cd_uf: str | None = None,
) -> list[dict[str, Any]]:
    uf_sigla = _resolve_uf_sigla(cd_uf)
    ano_inicio = (
        int(models.normalize_safra_sicor(safra_sicor).split("/")[0]) if safra_sicor else None
    )
    server_filters = [f"nomeUF eq '{uf_sigla}'"] if uf_sigla else []
    if ano_inicio is not None:
        server_filters.append(_safra_odata_filter(ano_inicio))
    _registrar_consulta(TOTAL_ENDPOINT, server_filters, list(models.SICOR_TOTAL_CAMPOS))
    records = await _fetch_credito_records(
        TOTAL_ENDPOINT, server_filters, list(models.SICOR_TOTAL_CAMPOS)
    )
    if ano_inicio is not None:
        records = _na_safra(records, ano_inicio)
    return [
        record
        for record in records
        if uf_sigla is None or str(record.get("nomeUF", "")).strip().upper() == uf_sigla
    ]


async def fetch_credito_rural_with_fallback(
    finalidade: str = "custeio",
    produto_sicor: str | None = None,
    safra_sicor: str | None = None,
    cd_uf: str | None = None,
    sem_fallback: str | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """Consulta o OData e, se ele falhar, a Base dos Dados; com `sem_fallback`, o erro do OData
    sobe com esse motivo, sem tentar o BigQuery."""
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
        if sem_fallback:
            raise SourceUnavailableError(
                source="bcb",
                url=f"{BASE_URL}/{ENDPOINT_MAP.get(finalidade.lower(), '')}",
                last_error=f"OData: {odata_error_msg}; sem fallback BigQuery: {sem_fallback}",
            ) from odata_err
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
