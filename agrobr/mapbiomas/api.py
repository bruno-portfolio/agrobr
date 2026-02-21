from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import BIOMAS_VALIDOS, normalizar_bioma

logger = structlog.get_logger()


@overload
async def cobertura(
    *,
    bioma: str | None = None,
    estado: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    colecao: int | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cobertura(
    *,
    bioma: str | None = None,
    estado: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    colecao: int | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def cobertura(
    *,
    bioma: str | None = None,
    estado: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    colecao: int | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    logger.info("mapbiomas_cobertura", bioma=bioma, estado=estado, ano=ano)

    fetch_kwargs = {}
    if colecao is not None:
        fetch_kwargs["colecao"] = colecao

    t0 = time.monotonic()
    xlsx_bytes, source_url = await client.fetch_biome_state(**fetch_kwargs)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_cobertura_xlsx(xlsx_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if bioma is not None:
        bioma_norm = normalizar_bioma(bioma)
        if bioma_norm in BIOMAS_VALIDOS:
            df = df[df["bioma"] == bioma_norm].reset_index(drop=True)
        else:
            df = df[df["bioma"].str.lower().str.contains(bioma.lower())].reset_index(drop=True)

    if estado is not None:
        estado_upper = estado.strip().upper()
        df = df[df["estado"].str.upper() == estado_upper].reset_index(drop=True)

    if ano is not None:
        df = df[df["ano"] == ano].reset_index(drop=True)

    if classe_id is not None:
        df = df[df["classe_id"] == classe_id].reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="mapbiomas",
            source_url=source_url,
            source_method="httpx+xlsx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["mapbiomas_gcs"],
            selected_source="mapbiomas_gcs",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


@overload
async def transicao(
    *,
    bioma: str | None = None,
    estado: str | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def transicao(
    *,
    bioma: str | None = None,
    estado: str | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def transicao(
    *,
    bioma: str | None = None,
    estado: str | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    logger.info("mapbiomas_transicao", bioma=bioma, estado=estado, periodo=periodo)

    fetch_kwargs = {}
    if colecao is not None:
        fetch_kwargs["colecao"] = colecao

    t0 = time.monotonic()
    xlsx_bytes, source_url = await client.fetch_biome_state(**fetch_kwargs)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_transicao_xlsx(xlsx_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if bioma is not None:
        bioma_norm = normalizar_bioma(bioma)
        if bioma_norm in BIOMAS_VALIDOS:
            df = df[df["bioma"] == bioma_norm].reset_index(drop=True)
        else:
            df = df[df["bioma"].str.lower().str.contains(bioma.lower())].reset_index(drop=True)

    if estado is not None:
        estado_upper = estado.strip().upper()
        df = df[df["estado"].str.upper() == estado_upper].reset_index(drop=True)

    if periodo is not None:
        df = df[df["periodo"] == periodo].reset_index(drop=True)

    if classe_de_id is not None:
        df = df[df["classe_de_id"] == classe_de_id].reset_index(drop=True)

    if classe_para_id is not None:
        df = df[df["classe_para_id"] == classe_para_id].reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="mapbiomas",
            source_url=source_url,
            source_method="httpx+xlsx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["mapbiomas_gcs"],
            selected_source="mapbiomas_gcs",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
