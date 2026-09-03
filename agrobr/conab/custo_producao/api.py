from __future__ import annotations

import re
import time
from io import BytesIO
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.time import utcnow

from . import client
from .parser import (
    PARSER_VERSION,
    _parse_sheet_info,
    items_to_dataframe,
    parse_planilha,
    select_data_sheet,
)

logger = structlog.get_logger()

_SAFRA_RE = re.compile(r"^\d{4}/\d{2}$")


def _validate_safra(safra: str | None) -> str | None:
    if safra is not None and (not isinstance(safra, str) or not _SAFRA_RE.fullmatch(safra)):
        raise InvalidParameterError(
            f"Safra inválida: {safra!r}. Use o padrão AAAA/AA, por exemplo '2024/25'"
        )
    return safra


def _resolve_sheet_context(
    xlsx: bytes | BytesIO,
    metadata: dict[str, Any],
    uf: str | None,
    safra: str | None,
) -> tuple[str, str, str]:
    sheet = select_data_sheet(xlsx, uf=uf, safra=safra)
    sheet_uf, sheet_year = _parse_sheet_info(sheet)
    resolved_uf = metadata.get("uf") or sheet_uf or uf or "BR"
    resolved_safra = metadata.get("safra") or safra
    if not resolved_safra and sheet_year:
        resolved_safra = f"{sheet_year}/{(int(sheet_year[-2:]) + 1) % 100:02d}"
    if not isinstance(resolved_safra, str) or not _SAFRA_RE.fullmatch(resolved_safra):
        raise ParseError(
            source="conab_custo",
            parser_version=PARSER_VERSION,
            reason="safra não identificada na planilha nem nos metadados",
        )
    return sheet, resolved_uf, resolved_safra


@overload
async def custo_producao(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def custo_producao(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def custo_producao(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    safra = _validate_safra(safra)
    t0 = time.monotonic()

    logger.info(
        "conab_custo_producao_request",
        cultura=cultura,
        uf=uf,
        safra=safra,
        tecnologia=tecnologia,
    )

    xlsx, metadata = await client.fetch_xlsx_for_cultura(
        cultura=cultura,
        uf=uf,
        safra=safra,
    )

    sheet, resolved_uf, resolved_safra = _resolve_sheet_context(xlsx, metadata, uf, safra)

    t1 = time.monotonic()
    items, custo_total = parse_planilha(
        xlsx=xlsx,
        cultura=cultura,
        uf=resolved_uf,
        safra=resolved_safra,
        tecnologia=tecnologia,
        sheet_name=sheet,
    )
    parse_ms = int((time.monotonic() - t1) * 1000)

    df = items_to_dataframe(items)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    logger.info(
        "conab_custo_producao_ok",
        cultura=cultura,
        uf=resolved_uf,
        safra=resolved_safra,
        items=len(items),
        coe=custo_total.coe_ha if custo_total else None,
    )

    meta = build_source_meta(
        "conab_custo",
        metadata.get("url", client.CUSTOS_PAGE),
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
        attempted_sources=["conab_custo"],
        selected_source="conab_custo",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def custo_producao_total(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    *,
    return_meta: Literal[False] = False,
) -> dict[str, Any]: ...


@overload
async def custo_producao_total(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    *,
    return_meta: Literal[True],
) -> tuple[dict[str, Any], MetaInfo]: ...


async def custo_producao_total(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    return_meta: bool = False,
) -> dict[str, Any] | tuple[dict[str, Any], MetaInfo]:
    safra = _validate_safra(safra)
    t0 = time.monotonic()

    logger.info(
        "conab_custo_total_request",
        cultura=cultura,
        uf=uf,
        safra=safra,
        tecnologia=tecnologia,
    )

    xlsx, metadata = await client.fetch_xlsx_for_cultura(
        cultura=cultura,
        uf=uf,
        safra=safra,
    )

    sheet, resolved_uf, resolved_safra = _resolve_sheet_context(xlsx, metadata, uf, safra)

    t1 = time.monotonic()
    _, custo_total = parse_planilha(
        xlsx=xlsx,
        cultura=cultura,
        uf=resolved_uf,
        safra=resolved_safra,
        tecnologia=tecnologia,
        sheet_name=sheet,
    )
    parse_ms = int((time.monotonic() - t1) * 1000)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    if custo_total is None:
        result: dict[str, Any] = {
            "cultura": cultura.lower(),
            "uf": resolved_uf,
            "safra": resolved_safra,
            "tecnologia": tecnologia.lower(),
            "coe_ha": 0.0,
            "cot_ha": None,
            "ct_ha": None,
        }
    else:
        result = custo_total.model_dump()

    logger.info(
        "conab_custo_total_ok",
        cultura=cultura,
        uf=resolved_uf,
        coe=result.get("coe_ha"),
    )

    if return_meta:
        now = utcnow()
        meta = MetaInfo(
            source="conab_custo",
            source_url=metadata.get("url", client.CUSTOS_PAGE),
            source_method="httpx",
            fetched_at=now,
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=1,
            columns=list(result.keys()),
            parser_version=PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["conab_custo"],
            selected_source="conab_custo",
            fetch_timestamp=now,
        )
        return result, meta

    return result
