from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client
from .parser import PARSER_VERSION, parse_serie_historica, records_to_dataframe

logger = structlog.get_logger()


@overload
async def serie_historica(
    produto: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def serie_historica(
    produto: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def serie_historica(
    produto: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf: str | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
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

    if return_meta:
        meta = MetaInfo(
            source="conab_serie_historica",
            source_url=metadata.get("url", client.SERIES_HISTORICAS_URL),
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["conab_serie_historica"],
            selected_source="conab_serie_historica",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


def produtos_disponiveis() -> list[dict[str, str]]:
    return client.list_produtos()
