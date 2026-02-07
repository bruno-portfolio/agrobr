"""API publica do modulo NASA POWER."""

from __future__ import annotations

import time
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import UF_COORDS

logger = structlog.get_logger()


async def clima_ponto(
    lat: float,
    lon: float,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "diario",
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Dados climaticos diarios ou mensais de um ponto (lat/lon).

    Args:
        lat: Latitude (-90 a 90).
        lon: Longitude (-180 a 180).
        inicio: Data inicial (str "YYYY-MM-DD" ou date).
        fim: Data final (str "YYYY-MM-DD" ou date).
        agregacao: "diario" (padrao) ou "mensal".
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com dados climaticos do ponto.
    """
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

    if return_meta:
        meta = MetaInfo(
            source="nasa_power",
            source_url=(
                f"{client.BASE_URL}?latitude={lat}&longitude={lon}"
                f"&start={inicio.strftime('%Y%m%d')}&end={fim.strftime('%Y%m%d')}"
            ),
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["nasa_power"],
            selected_source="nasa_power",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


async def clima_uf(
    uf: str,
    ano: int,
    agregacao: str = "mensal",
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Dados climaticos agregados por UF (ponto central).

    Usa coordenadas centrais da UF como ponto representativo.
    Para analises precisas, usar clima_ponto() com coordenadas especificas.

    Args:
        uf: Sigla da UF (ex: "MT", "SP").
        ano: Ano de referencia.
        agregacao: "diario" ou "mensal" (padrao).
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com dados climaticos da UF.

    Raises:
        ValueError: Se UF nao reconhecida.
    """
    uf_upper = uf.upper()
    if uf_upper not in UF_COORDS:
        raise ValueError(
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

    if return_meta:
        meta = MetaInfo(
            source="nasa_power",
            source_url=(
                f"{client.BASE_URL}?latitude={lat}&longitude={lon}"
                f"&start={inicio.strftime('%Y%m%d')}&end={fim.strftime('%Y%m%d')}"
            ),
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["nasa_power"],
            selected_source="nasa_power",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
