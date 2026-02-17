from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import (
    COMTRADE_PAISES_INV,
    HS_PRODUTOS_AGRO,
    resolve_hs,
    resolve_pais,
)

logger = structlog.get_logger()


@overload
async def comercio(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def comercio(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def comercio(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    hs_codes = resolve_hs(produto)
    reporter_code = resolve_pais(reporter)
    partner_code = resolve_pais(partner) if partner else 0

    if periodo is None:
        periodo = str(datetime.now(UTC).year)
    period_str = str(periodo)

    logger.info(
        "comtrade_comercio",
        produto=produto,
        hs_codes=hs_codes,
        reporter=reporter,
        partner=partner,
        fluxo=fluxo,
        periodo=period_str,
        freq=freq,
    )

    t0 = time.monotonic()

    records, source_url = await client.fetch_trade_data(
        reporter=reporter_code,
        partner=partner_code,
        hs_codes=hs_codes,
        flow=fluxo,
        period=period_str,
        freq=freq,
        api_key=api_key,
    )

    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_trade_data(records)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = MetaInfo(
            source="comtrade",
            source_url=source_url,
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["comtrade"],
            selected_source="comtrade",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


@overload
async def trade_mirror(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str = "CN",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def trade_mirror(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str = "CN",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def trade_mirror(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str = "CN",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    reporter_code = resolve_pais(reporter)
    partner_code = resolve_pais(partner)
    reporter_iso = COMTRADE_PAISES_INV.get(reporter_code, reporter.upper())
    partner_iso = COMTRADE_PAISES_INV.get(partner_code, partner.upper())

    logger.info(
        "comtrade_trade_mirror",
        produto=produto,
        reporter=reporter_iso,
        partner=partner_iso,
        periodo=periodo,
        freq=freq,
    )

    t0 = time.monotonic()

    df_export = await comercio(
        produto,
        reporter=reporter,
        partner=partner,
        fluxo="X",
        periodo=periodo,
        freq=freq,
        api_key=api_key,
    )

    df_import = await comercio(
        produto,
        reporter=partner,
        partner=reporter,
        fluxo="M",
        periodo=periodo,
        freq=freq,
        api_key=api_key,
    )

    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_mirror(df_export, df_import, reporter_iso, partner_iso)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = MetaInfo(
            source="comtrade_mirror",
            source_url=client.BASE_URL,
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["comtrade_export", "comtrade_import"],
            selected_source="comtrade_mirror",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


def paises() -> list[str]:
    return sorted(set(COMTRADE_PAISES_INV.values()))


def produtos() -> dict[str, list[str]]:
    return dict(HS_PRODUTOS_AGRO)
