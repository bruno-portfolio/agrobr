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
    """Busca dados de cobertura e uso da terra do MapBiomas.

    Area em hectares por classe de cobertura x bioma x estado x ano.
    Serie historica anual de 1985 a 2024 (Colecao 10).

    Args:
        bioma: Filtrar por bioma (ex: "Cerrado", "Amazonia").
            Se None, todos os biomas.
        estado: Filtrar por UF (ex: "MT", "SP") ou nome do estado.
        ano: Filtrar por ano (ex: 2020). Se None, todos os anos.
        classe_id: Filtrar por codigo de classe MapBiomas (ex: 15 para Pastagem).
        colecao: Numero da colecao MapBiomas (default: colecao atual).
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com colunas: bioma, estado, classe_id, classe,
        nivel_0, ano, area_ha.

    Raises:
        SourceUnavailableError: Se dados MapBiomas indisponiveis.
        ParseError: Se nao conseguir parsear o XLSX.

    Example:
        >>> df = await mapbiomas.cobertura(bioma="Cerrado", ano=2020)
        >>> df.columns.tolist()
        ['bioma', 'estado', 'classe_id', 'classe', 'nivel_0', 'ano', 'area_ha']
    """
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
    """Busca dados de transicao entre classes de uso da terra do MapBiomas.

    Area em hectares de transicao entre classes x bioma x estado x periodo.
    Periodos anuais consecutivos (ex: 2019-2020) e agregados (ex: 1985-2024).

    Args:
        bioma: Filtrar por bioma (ex: "Cerrado", "Amazonia").
        estado: Filtrar por UF (ex: "MT") ou nome do estado.
        periodo: Filtrar por periodo (ex: "2019-2020", "1985-2024").
        classe_de_id: Filtrar por codigo da classe de origem.
        classe_para_id: Filtrar por codigo da classe de destino.
        colecao: Numero da colecao MapBiomas (default: colecao atual).
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com colunas: bioma, estado, classe_de_id, classe_de,
        classe_para_id, classe_para, periodo, area_ha.

    Raises:
        SourceUnavailableError: Se dados MapBiomas indisponiveis.
        ParseError: Se nao conseguir parsear o XLSX.

    Example:
        >>> df = await mapbiomas.transicao(bioma="Cerrado", periodo="2019-2020")
        >>> df.columns.tolist()
        ['bioma', 'estado', 'classe_de_id', 'classe_de', ...]
    """
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
