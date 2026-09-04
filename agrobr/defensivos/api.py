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

_lock_formulados = asyncio.Lock()
_lock_tecnicos = asyncio.Lock()


async def _ensure_formulados_cached() -> tuple[int, str, pd.DataFrame, pd.DataFrame]:
    async with _lock_formulados:
        form_cached = cache.read_cached("formulados")
        auth_cached = cache.read_cached("autorizacoes")
        if form_cached is not None and auth_cached is not None:
            return 0, client.FORMULADOS_URL, form_cached, auth_cached

        t0 = time.monotonic()
        raw = await client.download_formulados()
        fetch_ms = int((time.monotonic() - t0) * 1000)

        form_df, auth_df = parser.parse_formulados_csv(raw)
        cache.write_formulados_pair(form_df, auth_df)
        return fetch_ms, client.FORMULADOS_URL, form_df, auth_df


@overload
async def formulados(
    *,
    ingrediente_ativo: str | None = None,
    classe_toxicologica: str | None = None,
    classe_ambiental: str | None = None,
    titular: str | None = None,
    organicos: str | None = None,
    marca: str | None = None,
    formulacao: str | None = None,
    classe: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def formulados(
    *,
    ingrediente_ativo: str | None = ...,
    classe_toxicologica: str | None = ...,
    classe_ambiental: str | None = ...,
    titular: str | None = ...,
    organicos: str | None = ...,
    marca: str | None = ...,
    formulacao: str | None = ...,
    classe: str | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def formulados(
    *,
    ingrediente_ativo: str | None = None,
    classe_toxicologica: str | None = None,
    classe_ambiental: str | None = None,
    titular: str | None = None,
    organicos: str | None = None,
    marca: str | None = None,
    formulacao: str | None = None,
    classe: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_ms, source_url, df, _ = await _ensure_formulados_cached()

    t1 = time.monotonic()
    df = df.copy()

    if ingrediente_ativo is not None:
        df = df[
            df["ingrediente_ativo"].str.contains(
                ingrediente_ativo, case=False, na=False, regex=False
            )
        ]
    if classe_toxicologica is not None:
        df = df[
            df["classe_toxicologica"].str.contains(
                classe_toxicologica, case=False, na=False, regex=False
            )
        ]
    if classe_ambiental is not None:
        df = df[
            df["classe_ambiental"].str.contains(classe_ambiental, case=False, na=False, regex=False)
        ]
    if titular is not None:
        df = df[df["titular"].str.contains(titular, case=False, na=False, regex=False)]
    if organicos is not None:
        df = df[df["organicos"] == organicos]
    if marca is not None:
        df = df[df["marca_comercial"].str.contains(marca, case=False, na=False, regex=False)]
    if formulacao is not None:
        df = df[df["formulacao"].str.contains(formulacao, case=False, na=False, regex=False)]
    if classe is not None:
        df = df[df["classe"].str.contains(classe, case=False, na=False, regex=False)]

    df = df.reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "defensivos",
        source_url,
        "httpx+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def autorizacoes(
    *,
    nr_registro: str | None = None,
    cultura: str | None = None,
    ingrediente_ativo: str | None = None,
    classe: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def autorizacoes(
    *,
    nr_registro: str | None = ...,
    cultura: str | None = ...,
    ingrediente_ativo: str | None = ...,
    classe: str | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def autorizacoes(
    *,
    nr_registro: str | None = None,
    cultura: str | None = None,
    ingrediente_ativo: str | None = None,
    classe: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_ms, source_url, _, df = await _ensure_formulados_cached()

    t1 = time.monotonic()
    df = df.copy()

    if nr_registro is not None:
        df = df[df["nr_registro"] == nr_registro]
    if cultura is not None:
        df = df[df["cultura"].str.contains(cultura, case=False, na=False, regex=False)]
    if ingrediente_ativo is not None:
        df = df[
            df["ingrediente_ativo"].str.contains(
                ingrediente_ativo, case=False, na=False, regex=False
            )
        ]
    if classe is not None:
        df = df[df["classe"].str.contains(classe, case=False, na=False, regex=False)]

    df = df.reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "defensivos",
        source_url,
        "httpx+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def tecnicos(
    *,
    ingrediente_ativo: str | None = None,
    titular: str | None = None,
    classe: str | None = None,
    marca: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def tecnicos(
    *,
    ingrediente_ativo: str | None = ...,
    titular: str | None = ...,
    classe: str | None = ...,
    marca: str | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def tecnicos(
    *,
    ingrediente_ativo: str | None = None,
    titular: str | None = None,
    classe: str | None = None,
    marca: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    async with _lock_tecnicos:
        cached = cache.read_cached("tecnicos")
        if cached is not None:
            fetch_ms = 0
            df = cached
        else:
            t0 = time.monotonic()
            raw = await client.download_tecnicos()
            fetch_ms = int((time.monotonic() - t0) * 1000)

            df = parser.parse_tecnicos_csv(raw)
            cache.write_cache("tecnicos", df)

    source_url = client.TECNICOS_URL
    t1 = time.monotonic()
    df = df.copy()

    if ingrediente_ativo is not None:
        df = df[
            df["ingrediente_ativo"].str.contains(
                ingrediente_ativo, case=False, na=False, regex=False
            )
        ]
    if titular is not None:
        df = df[df["titular"].str.contains(titular, case=False, na=False, regex=False)]
    if classe is not None:
        df = df[df["classe"].str.contains(classe, case=False, na=False, regex=False)]
    if marca is not None:
        df = df[df["marca_comercial"].str.contains(marca, case=False, na=False, regex=False)]

    df = df.reset_index(drop=True)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "defensivos",
        source_url,
        "httpx+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
