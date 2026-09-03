from __future__ import annotations

import time
from datetime import date
from typing import Any

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result

from . import client, parser
from .models import UF_COORDS

logger = structlog.get_logger()


async def clima_ponto(
    lat: float,
    lon: float,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "diario",
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    if isinstance(inicio, str):
        inicio = date.fromisoformat(inicio)
    if isinstance(fim, str):
        fim = date.fromisoformat(fim)

    t0 = time.monotonic()
    dados = await client.fetch_daily(lat, lon, inicio, fim)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_daily(dados, lat, lon)
    if agregacao == "mensal":
        df = parser.agregar_mensal(df)
    parse_ms = int((time.monotonic() - t1) * 1000)

    source_url = (
        f"{client.BASE_URL}?latitude={lat}&longitude={lon}"
        f"&start={inicio.strftime('%Y%m%d')}&end={fim.strftime('%Y%m%d')}"
    )
    meta = build_source_meta(
        "nasa_power",
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def clima_uf(
    uf: str,
    ano: int,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    uf_upper = uf.upper()
    if uf_upper not in UF_COORDS:
        raise InvalidParameterError(
            f"UF '{uf_upper}' nao reconhecida. UFs disponiveis: {sorted(UF_COORDS.keys())}"
        )

    lat, lon = UF_COORDS[uf_upper]
    inicio = date(ano, 1, 1)
    fim = date(ano, 12, 31)

    t0 = time.monotonic()
    dados = await client.fetch_daily(lat, lon, inicio, fim)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_daily(dados, lat, lon, uf=uf_upper)
    if agregacao == "mensal":
        df = parser.agregar_mensal(df)
    parse_ms = int((time.monotonic() - t1) * 1000)

    source_url = (
        f"{client.BASE_URL}?latitude={lat}&longitude={lon}"
        f"&start={inicio.strftime('%Y%m%d')}&end={fim.strftime('%Y%m%d')}"
    )
    meta = build_source_meta(
        "nasa_power",
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
