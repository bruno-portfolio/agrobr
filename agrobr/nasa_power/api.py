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


def _validate_point(lat: object, lon: object) -> tuple[float, float]:
    if not isinstance(lat, (int, float)) or isinstance(lat, bool) or not -90 <= lat <= 90:
        raise InvalidParameterError("lat deve estar entre -90 e 90")
    if not isinstance(lon, (int, float)) or isinstance(lon, bool) or not -180 <= lon <= 180:
        raise InvalidParameterError("lon deve estar entre -180 e 180")
    return float(lat), float(lon)


def _normalize_range(inicio: str | date, fim: str | date) -> tuple[date, date]:
    try:
        start = date.fromisoformat(inicio) if isinstance(inicio, str) else inicio
        end = date.fromisoformat(fim) if isinstance(fim, str) else fim
    except ValueError as exc:
        raise InvalidParameterError("inicio e fim devem usar o formato YYYY-MM-DD") from exc
    if not isinstance(start, date) or not isinstance(end, date):
        raise InvalidParameterError("inicio e fim devem ser datas ou strings YYYY-MM-DD")
    if start > end:
        raise InvalidParameterError("inicio deve ser anterior ou igual a fim")
    return start, end


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
    lat, lon = _validate_point(lat, lon)
    inicio, fim = _normalize_range(inicio, fim)
    if agregacao not in {"diario", "mensal"}:
        raise InvalidParameterError("agregacao deve ser 'diario' ou 'mensal'")

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
