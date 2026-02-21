from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser

logger = structlog.get_logger()


@overload
async def prodes(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def prodes(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def prodes(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    logger.info("desmatamento_prodes", bioma=bioma, ano=ano, uf=uf)

    t0 = time.monotonic()
    csv_bytes, source_url = await client.fetch_prodes(bioma, ano=ano, uf=uf)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_prodes_csv(csv_bytes, bioma)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if uf is not None:
        uf_upper = uf.strip().upper()
        df = df[df["uf"] == uf_upper].reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="desmatamento",
            source_url=source_url,
            source_method="httpx+wfs+csv",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["terrabrasilis_prodes"],
            selected_source="terrabrasilis_prodes",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


@overload
async def deter(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def deter(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def deter(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    logger.info(
        "desmatamento_deter",
        bioma=bioma,
        uf=uf,
        data_inicio=data_inicio,
        data_fim=data_fim,
        classe=classe,
    )

    t0 = time.monotonic()
    csv_bytes, source_url = await client.fetch_deter(
        bioma, uf=uf, data_inicio=data_inicio, data_fim=data_fim
    )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_deter_csv(csv_bytes, bioma)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if classe is not None:
        df = df[df["classe"] == classe].reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="desmatamento",
            source_url=source_url,
            source_method="httpx+wfs+csv",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["terrabrasilis_deter"],
            selected_source="terrabrasilis_deter",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
