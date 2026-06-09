from __future__ import annotations

import time
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result

from . import sgs_client
from .sgs_models import COLUNAS_SAIDA, PARSER_VERSION, SGS_SERIES

logger = structlog.get_logger()


@overload
async def sgs(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def sgs(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def sgs(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    nome_serie: str | None = None

    if isinstance(codigo, str):
        nome_serie = codigo
        resolved = SGS_SERIES.get(codigo)
        if resolved is None:
            raise ValueError(f"Serie '{codigo}' nao encontrada. Opcoes: {list(SGS_SERIES.keys())}")
        codigo = resolved

    t0 = time.monotonic()

    records, source_url = await sgs_client.fetch_sgs(
        codigo,
        data_inicial=data_inicial,
        data_final=data_final,
        ultimos=ultimos,
    )

    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = _parse_sgs(records, codigo, nome_serie)

    if ultimos is not None and ultimos > 0:
        df = df.tail(ultimos).reset_index(drop=True)

    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "bcb_sgs",
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
        attempted_sources=["bcb_sgs"],
        selected_source="bcb_sgs",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def _parse_sgs(records: list[dict[str, str]], codigo: int, nome_serie: str | None) -> pd.DataFrame:
    df = pd.DataFrame(records)

    df["data"] = pd.to_datetime(df["data"], dayfirst=True)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df["codigo"] = codigo
    df["nome_serie"] = nome_serie or _reverse_lookup(codigo)

    return df[COLUNAS_SAIDA]


def _reverse_lookup(codigo: int) -> str | None:
    for name, code in SGS_SERIES.items():
        if code == codigo:
            return name
    return None
