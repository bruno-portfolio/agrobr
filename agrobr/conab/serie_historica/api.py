from __future__ import annotations

import time
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result

from . import client
from .parser import PARSER_VERSION, parse_serie_historica, records_to_dataframe

logger = structlog.get_logger()


@overload
async def serie_historica(
    produto: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def serie_historica(
    produto: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def serie_historica(
    produto: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    client.get_xls_url(produto)
    t0 = time.monotonic()

    logger.info(
        "conab_serie_historica_request",
        produto=produto,
        inicio=inicio,
        fim=fim,
        uf=uf,
    )

    xls, metadata = await client.download_xls(produto)

    t1 = time.monotonic()
    records = parse_serie_historica(
        xls=xls,
        produto=produto,
        inicio=inicio,
        fim=fim,
        uf=uf,
    )
    parse_ms = int((time.monotonic() - t1) * 1000)

    df = records_to_dataframe(records)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    logger.info(
        "conab_serie_historica_ok",
        produto=produto,
        records=len(records),
        safras=len(df["safra"].unique()) if not df.empty else 0,
        ufs=len(df["uf"].dropna().unique()) if not df.empty else 0,
    )

    meta = build_source_meta(
        "conab_serie_historica",
        metadata.get("url", client.SERIES_HISTORICAS_URL),
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
        attempted_sources=["conab_serie_historica"],
        selected_source="conab_serie_historica",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def produtos_disponiveis() -> list[dict[str, str]]:
    return client.list_produtos()
