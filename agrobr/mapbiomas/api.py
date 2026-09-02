from __future__ import annotations

import time
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo
from agrobr.normalize import regions
from agrobr.utils.result import build_source_meta, finalize_result

from . import client, parser
from .models import BIOMAS_VALIDOS, COLECAO_ATUAL, normalizar_bioma

logger = structlog.get_logger()


def _validar_colecao(colecao: int | None) -> None:
    if colecao is not None and colecao != COLECAO_ATUAL:
        raise ValueError(
            f"colecao {colecao} nao suportada; apenas a colecao {COLECAO_ATUAL} (atual) esta disponivel"
        )


def _normalizar_estado(estado: str | None) -> str | None:
    if estado is None:
        return None

    estado_key = regions.remover_acentos(estado.strip().lower())
    estado_uf = regions.NOMES_PARA_UF.get(estado_key)
    if estado_uf is None:
        raise ValueError(f"Estado inválido: {estado!r}. Use a sigla ou o nome completo de uma UF")
    return estado_uf


@overload
async def cobertura(
    *,
    bioma: str | None = None,
    estado: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: Literal["estado", "municipio"] = "estado",
    municipio: str | None = None,
    colecao: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cobertura(
    *,
    bioma: str | None = None,
    estado: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: Literal["estado", "municipio"] = "estado",
    municipio: str | None = None,
    colecao: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def cobertura(
    *,
    bioma: str | None = None,
    estado: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: Literal["estado", "municipio"] = "estado",
    municipio: str | None = None,
    colecao: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    _validar_colecao(colecao)
    estado = _normalizar_estado(estado)

    logger.info(
        "mapbiomas_cobertura",
        bioma=bioma,
        estado=estado,
        ano=ano,
        nivel=nivel,
        municipio=municipio,
    )

    t0 = time.monotonic()
    if nivel == "municipio":
        logger.warning(
            "mapbiomas_municipal_download",
            hint="Arquivo municipal ~660 MB — download pode demorar",
        )
        xlsx_bytes, source_url = await client.fetch_biome_state_municipality()
    else:
        xlsx_bytes, source_url = await client.fetch_biome_state()
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
        df = df[df["estado"] == estado].reset_index(drop=True)

    if municipio is not None and "municipio" in df.columns:
        mun_lower = municipio.strip().lower()
        df = df[df["municipio"].str.lower().str.contains(mun_lower)].reset_index(drop=True)

    if ano is not None:
        df = df[df["ano"] == ano].reset_index(drop=True)

    if classe_id is not None:
        df = df[df["classe_id"] == classe_id].reset_index(drop=True)

    meta = build_source_meta(
        "mapbiomas",
        source_url,
        "httpx+xlsx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["mapbiomas_dataverse"],
        selected_source="mapbiomas_dataverse",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def transicao(
    *,
    bioma: str | None = None,
    estado: str | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    as_polars: bool = False,
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
    as_polars: bool = False,
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
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    _validar_colecao(colecao)
    estado = _normalizar_estado(estado)

    logger.info("mapbiomas_transicao", bioma=bioma, estado=estado, periodo=periodo)

    t0 = time.monotonic()
    xlsx_bytes, source_url = await client.fetch_biome_state()
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
        df = df[df["estado"] == estado].reset_index(drop=True)

    if periodo is not None:
        df = df[df["periodo"] == periodo].reset_index(drop=True)

    if classe_de_id is not None:
        df = df[df["classe_de_id"] == classe_de_id].reset_index(drop=True)

    if classe_para_id is not None:
        df = df[df["classe_para_id"] == classe_para_id].reset_index(drop=True)

    meta = build_source_meta(
        "mapbiomas",
        source_url,
        "httpx+xlsx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["mapbiomas_dataverse"],
        selected_source="mapbiomas_dataverse",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
