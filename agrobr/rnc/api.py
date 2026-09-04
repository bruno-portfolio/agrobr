from __future__ import annotations

import asyncio
import time
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result

from . import cache, client, parser

logger = structlog.get_logger()

_lock_registradas = asyncio.Lock()
_lock_protegidas = asyncio.Lock()


async def _ensure_registradas() -> tuple[int, str, pd.DataFrame]:
    async with _lock_registradas:
        cached = cache.read_cached("registradas")
        if cached is not None:
            return 0, client._REGISTRADAS_URL, cached

        t0 = time.monotonic()
        raw, source_url = await client.fetch_registradas()
        fetch_ms = int((time.monotonic() - t0) * 1000)

        df = parser.parse_registradas_csv(raw)
        cache.write_cache("registradas", df)
        return fetch_ms, source_url, df


async def _ensure_protegidas() -> tuple[int, str, pd.DataFrame]:
    async with _lock_protegidas:
        cached = cache.read_cached("protegidas")
        if cached is not None:
            return 0, client._PROTEGIDAS_URL, cached

        t0 = time.monotonic()
        raw, source_url = await client.fetch_protegidas()
        fetch_ms = int((time.monotonic() - t0) * 1000)

        df = parser.parse_protegidas_csv(raw)
        cache.write_cache("protegidas", df)
        return fetch_ms, source_url, df


@overload
async def registradas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    grupo: str | None = None,
    situacao: str | None = None,
    mantenedor: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def registradas(
    *,
    cultivar: str | None = ...,
    especie: str | None = ...,
    grupo: str | None = ...,
    situacao: str | None = ...,
    mantenedor: str | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def registradas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    grupo: str | None = None,
    situacao: str | None = None,
    mantenedor: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    logger.info("rnc_registradas", cultivar=cultivar, especie=especie, grupo=grupo)

    fetch_ms, source_url, df = await _ensure_registradas()

    t1 = time.monotonic()
    df = df.copy()

    if cultivar is not None:
        df = df[df["cultivar"].str.contains(cultivar, case=False, na=False, regex=False)]
    if especie is not None:
        df = df[df["nome_comum"].str.contains(especie, case=False, na=False, regex=False)]
    if grupo is not None:
        df = df[df["grupo"].str.contains(grupo, case=False, na=False, regex=False)]
    if situacao is not None:
        df = df[df["situacao"].str.contains(situacao, case=False, na=False, regex=False)]
    if mantenedor is not None:
        df = df[df["mantenedor"].str.contains(mantenedor, case=False, na=False, regex=False)]

    df = df.reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "rnc",
        source_url,
        "httpx+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def protegidas(
    *,
    cultivar: str | None = ...,
    especie: str | None = ...,
    situacao: str | None = ...,
    titular: str | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    logger.info("rnc_protegidas", cultivar=cultivar, especie=especie)

    fetch_ms, source_url, df = await _ensure_protegidas()

    t1 = time.monotonic()
    df = df.copy()

    if cultivar is not None:
        df = df[df["cultivar"].str.contains(cultivar, case=False, na=False, regex=False)]
    if especie is not None:
        df = df[df["nome_comum"].str.contains(especie, case=False, na=False, regex=False)]
    if situacao is not None:
        df = df[df["situacao"].str.contains(situacao, case=False, na=False, regex=False)]
    if titular is not None:
        df = df[df["titular"].str.contains(titular, case=False, na=False, regex=False)]

    df = df.reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "rnc",
        source_url,
        "httpx+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
