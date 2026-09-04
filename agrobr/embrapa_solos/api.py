from __future__ import annotations

import time
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo
from agrobr.utils.geo import validate_bbox
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_uf
from agrobr.utils.warnings import warn_once

from . import client, parser

logger = structlog.get_logger()

_NC_WARNING = (
    "EMBRAPA Solos: CC BY-NC 3.0 BR — uso comercial requer autorizacao. "
    "Classificacao: nc. Veja docs/licenses.md."
)


@overload
async def perfis(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def perfis(
    *,
    uf: str | None = ...,
    bbox: tuple[float, float, float, float] | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def perfis(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    warn_once("embrapa_solos", _NC_WARNING)
    uf = validate_uf(uf)
    bbox = validate_bbox(bbox)
    logger.info("embrapa_solos_perfis", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    pages, source_url = await client.fetch_perfis(bbox=bbox)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_perfis_csv(pages)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if uf is not None and not df.empty:
        df = df[df["uf"] == uf].reset_index(drop=True)

    meta = build_source_meta(
        "embrapa_solos",
        source_url,
        "httpx+wfs+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["embrapa_geoinfo"],
        selected_source="embrapa_geoinfo",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def perfis_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[False] = False,
) -> Any: ...


@overload
async def perfis_geo(
    *,
    uf: str | None = ...,
    bbox: tuple[float, float, float, float] | None = ...,
    return_meta: Literal[True],
) -> tuple[Any, MetaInfo]: ...


async def perfis_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    warn_once("embrapa_solos", _NC_WARNING)
    uf = validate_uf(uf)
    bbox = validate_bbox(bbox)
    logger.info("embrapa_solos_perfis_geo", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    geojson_bytes, source_url = await client.fetch_perfis_geo(bbox=bbox)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_perfis_geojson(geojson_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if uf is not None and not gdf.empty:
        gdf = gdf[gdf["uf"] == uf].reset_index(drop=True)

    if return_meta:
        meta = build_source_meta(
            "embrapa_solos",
            source_url,
            "httpx+wfs+geojson",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            attempted_sources=["embrapa_geoinfo_geo"],
            selected_source="embrapa_geoinfo_geo",
        )
        return gdf, meta

    return gdf


@overload
async def mapa_solos(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def mapa_solos(
    *,
    ordem: str | None = ...,
    bbox: tuple[float, float, float, float] | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def mapa_solos(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    warn_once("embrapa_solos", _NC_WARNING)
    bbox = validate_bbox(bbox)
    logger.info("embrapa_solos_mapa", ordem=ordem, bbox=bbox)

    t0 = time.monotonic()
    pages, source_url = await client.fetch_mapa_solos(bbox=bbox)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_mapa_csv(pages)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if ordem is not None and not df.empty:
        df = df[df["ordem1"].str.contains(ordem, case=False, na=False, regex=False)].reset_index(
            drop=True
        )

    meta = build_source_meta(
        "embrapa_solos",
        source_url,
        "httpx+wfs+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["embrapa_geoinfo"],
        selected_source="embrapa_geoinfo",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def mapa_solos_geo(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[False] = False,
) -> Any: ...


@overload
async def mapa_solos_geo(
    *,
    ordem: str | None = ...,
    bbox: tuple[float, float, float, float] | None = ...,
    return_meta: Literal[True],
) -> tuple[Any, MetaInfo]: ...


async def mapa_solos_geo(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    warn_once("embrapa_solos", _NC_WARNING)
    bbox = validate_bbox(bbox)
    logger.info("embrapa_solos_mapa_geo", ordem=ordem, bbox=bbox)

    t0 = time.monotonic()
    geojson_bytes, source_url = await client.fetch_mapa_solos_geo(bbox=bbox)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_mapa_geojson(geojson_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if ordem is not None and not gdf.empty:
        gdf = gdf[gdf["ordem1"].str.contains(ordem, case=False, na=False, regex=False)].reset_index(
            drop=True
        )

    if return_meta:
        meta = build_source_meta(
            "embrapa_solos",
            source_url,
            "httpx+wfs+geojson",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            attempted_sources=["embrapa_geoinfo_geo"],
            selected_source="embrapa_geoinfo_geo",
        )
        return gdf, meta

    return gdf
