from __future__ import annotations

import time
import warnings
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.normalize.regions import normalizar_bioma
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_uf

from . import client, parser
from .models import DETER_WORKSPACES, PRODES_WORKSPACES

if TYPE_CHECKING:
    import geopandas as gpd

logger = structlog.get_logger()


def _validate_bioma(bioma: object, valid: dict[str, str]) -> str:
    if not isinstance(bioma, str):
        raise InvalidParameterError("bioma deve ser uma string")
    normalized = normalizar_bioma(bioma)
    if normalized not in valid:
        raise InvalidParameterError(f"Bioma inválido: {bioma!r}. Opções: {sorted(valid)}")
    return normalized


def _warn_if_truncated(df: pd.DataFrame, *, dataset: str, hint: str) -> None:
    if len(df) < client.MAX_FEATURES_PER_REQUEST:
        return
    message = f"{dataset} atingiu o teto de {client.MAX_FEATURES_PER_REQUEST:,} registros; {hint}"
    warnings.warn(message, UserWarning, stacklevel=3)
    logger.warning(
        f"desmatamento_{dataset}_truncated",
        records=len(df),
        limit=client.MAX_FEATURES_PER_REQUEST,
        hint=hint,
    )


@overload
async def prodes(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def prodes(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def prodes(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    bioma = _validate_bioma(bioma, PRODES_WORKSPACES)
    uf = validate_uf(uf)
    logger.info("desmatamento_prodes", bioma=bioma, ano=ano, uf=uf)

    t0 = time.monotonic()
    csv_bytes, source_url = await client.fetch_prodes(bioma, ano=ano, uf=uf)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_prodes_csv(csv_bytes, bioma)
    parse_ms = int((time.monotonic() - t1) * 1000)

    _warn_if_truncated(df, dataset="prodes", hint="filtre por ano e/ou UF")

    if uf is not None:
        uf_upper = uf.strip().upper()
        df = df[df["uf"] == uf_upper].reset_index(drop=True)

    meta = build_source_meta(
        "desmatamento",
        source_url,
        "httpx+wfs+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["terrabrasilis_prodes"],
        selected_source="terrabrasilis_prodes",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def prodes_geo(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def prodes_geo(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def prodes_geo(
    *,
    bioma: str = "Cerrado",
    ano: int | None = None,
    uf: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    bioma = _validate_bioma(bioma, PRODES_WORKSPACES)
    uf = validate_uf(uf)
    logger.info("desmatamento_prodes_geo", bioma=bioma, ano=ano, uf=uf)

    t0 = time.monotonic()
    geojson_bytes, source_url = await client.fetch_prodes_geo(bioma, ano=ano, uf=uf)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_prodes_geojson(geojson_bytes, bioma)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if uf is not None:
        uf_upper = uf.strip().upper()
        gdf = gdf[gdf["uf"] == uf_upper].reset_index(drop=True)

    if return_meta:
        meta = build_source_meta(
            "desmatamento",
            source_url,
            "httpx+wfs+geojson",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            attempted_sources=["terrabrasilis_prodes_geo"],
            selected_source="terrabrasilis_prodes_geo",
        )
        return gdf, meta

    return gdf


@overload
async def deter(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    as_polars: bool = False,
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
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def deter(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    bioma = _validate_bioma(bioma, DETER_WORKSPACES)
    uf = validate_uf(uf)
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

    _warn_if_truncated(df, dataset="deter", hint="filtre por UF e/ou período")

    if classe is not None:
        df = df[df["classe"] == classe].reset_index(drop=True)

    meta = build_source_meta(
        "desmatamento",
        source_url,
        "httpx+wfs+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["terrabrasilis_deter"],
        selected_source="terrabrasilis_deter",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def deter_geo(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def deter_geo(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def deter_geo(
    *,
    bioma: str = "Amazônia",
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    bioma = _validate_bioma(bioma, DETER_WORKSPACES)
    uf = validate_uf(uf)
    logger.info(
        "desmatamento_deter_geo",
        bioma=bioma,
        uf=uf,
        data_inicio=data_inicio,
        data_fim=data_fim,
        classe=classe,
    )

    t0 = time.monotonic()
    geojson_bytes, source_url = await client.fetch_deter_geo(
        bioma, uf=uf, data_inicio=data_inicio, data_fim=data_fim
    )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_deter_geojson(geojson_bytes, bioma)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if classe is not None:
        gdf = gdf[gdf["classe"] == classe].reset_index(drop=True)

    if return_meta:
        meta = build_source_meta(
            "desmatamento",
            source_url,
            "httpx+wfs+geojson",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            attempted_sources=["terrabrasilis_deter_geo"],
            selected_source="terrabrasilis_deter_geo",
        )
        return gdf, meta

    return gdf
