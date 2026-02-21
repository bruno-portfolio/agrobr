from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client
from .parser import PARSER_VERSION, items_to_dataframe, parse_planilha

logger = structlog.get_logger()


@overload
async def custo_producao(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def custo_producao(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def custo_producao(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
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

    resolved_uf = metadata.get("uf", uf or "BR")
    resolved_safra = metadata.get("safra", safra or "latest")

    t1 = time.monotonic()
    items, custo_total = parse_planilha(
        xlsx=xlsx,
        cultura=cultura,
        uf=resolved_uf,
        safra=resolved_safra,
        tecnologia=tecnologia,
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

    if return_meta:
        meta = MetaInfo(
            source="conab_custo",
            source_url=metadata.get("url", client.CUSTOS_PAGE),
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["conab_custo"],
            selected_source="conab_custo",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


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

    resolved_uf = metadata.get("uf", uf or "BR")
    resolved_safra = metadata.get("safra", safra or "latest")

    t1 = time.monotonic()
    _, custo_total = parse_planilha(
        xlsx=xlsx,
        cultura=cultura,
        uf=resolved_uf,
        safra=resolved_safra,
        tecnologia=tecnologia,
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
        meta = MetaInfo(
            source="conab_custo",
            source_url=metadata.get("url", client.CUSTOS_PAGE),
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=1,
            columns=list(result.keys()),
            parser_version=PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["conab_custo"],
            selected_source="conab_custo",
            fetch_timestamp=datetime.now(UTC),
        )
        return result, meta

    return result
