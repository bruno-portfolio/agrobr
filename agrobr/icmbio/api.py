from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.geo import validate_bbox
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_uf

from . import client, parser
from .models import GRUPOS_VALIDOS

if TYPE_CHECKING:
    import geopandas as gpd

logger = structlog.get_logger()


def _validate_grupo(grupo: str | None) -> str | None:
    if grupo is None:
        return None
    grupo_upper = grupo.strip().upper()
    if grupo_upper not in GRUPOS_VALIDOS:
        raise InvalidParameterError(
            f"Grupo invalido: {grupo!r}. Valores validos: {sorted(GRUPOS_VALIDOS)}"
        )
    return grupo_upper


@overload
async def ucs(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ucs(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def ucs(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    uf = validate_uf(uf)
    grupo = _validate_grupo(grupo)
    validate_bbox(bbox)
    logger.info("icmbio_ucs", uf=uf, grupo=grupo, bioma=bioma, bbox=bbox)

    t0 = time.monotonic()
    csv_bytes, source_url = await client.fetch_ucs(uf=uf, grupo=grupo, bioma=bioma, bbox=bbox)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_ucs_csv(csv_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "icmbio",
        source_url,
        "httpx+wfs+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["icmbio_wfs"],
        selected_source="icmbio_wfs",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def ucs_geo(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def ucs_geo(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def ucs_geo(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    uf = validate_uf(uf)
    grupo = _validate_grupo(grupo)
    validate_bbox(bbox)
    logger.info("icmbio_ucs_geo", uf=uf, grupo=grupo, bioma=bioma, bbox=bbox)

    t0 = time.monotonic()
    geojson_bytes, source_url = await client.fetch_ucs_geo(bbox=bbox)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_ucs_geojson(geojson_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if uf is not None and not gdf.empty:
        gdf = gdf[gdf["uf"].str.contains(uf, na=False, regex=False)].reset_index(drop=True)
    if grupo is not None and not gdf.empty:
        gdf = gdf[gdf["grupo"] == grupo].reset_index(drop=True)
    if bioma is not None and not gdf.empty:
        gdf = gdf[gdf["bioma"] == bioma].reset_index(drop=True)

    if return_meta:
        meta = build_source_meta(
            "icmbio",
            source_url,
            "httpx+wfs+geojson",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            attempted_sources=["icmbio_wfs_geo"],
            selected_source="icmbio_wfs_geo",
        )
        return gdf, meta

    return gdf
