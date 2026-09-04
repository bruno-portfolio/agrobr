from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.geo import validate_bbox
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_bioma, validate_uf

from . import client, models, parser

if TYPE_CHECKING:
    import geopandas as gpd

logger = structlog.get_logger()

_WHERE_FIELDS: dict[str, dict[str, str]] = {
    "cnfp": {"uf": "uf", "bioma": "bioma", "categoria": "categoria"},
    "concessoes": {"uf": "uf"},
    "ifn_conglomerados": {"uf": "no_uf", "bioma": "no_bioma"},
}


def _escape_filter_value(value: str) -> str:
    return value.replace("'", "''")


def _validate_categoria(categoria: str | None) -> str | None:
    if categoria is None:
        return None
    normalized = categoria.strip().upper()
    if normalized not in models.CATEGORIAS_CNFP:
        raise InvalidParameterError(
            f"Categoria inválida: {categoria!r}. Valores válidos: {sorted(models.CATEGORIAS_CNFP)}"
        )
    return normalized


def _build_where(
    layer_key: str,
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
) -> str:
    fields = _WHERE_FIELDS.get(layer_key, {})
    clauses: list[str] = []
    if uf:
        field = fields.get("uf", "uf")
        clauses.append(f"{field}='{_escape_filter_value(uf.upper())}'")
    if bioma:
        field = fields.get("bioma", "bioma")
        clauses.append(f"{field}='{_escape_filter_value(bioma.upper())}'")
    if categoria:
        field = fields.get("categoria", "categoria")
        clauses.append(f"{field}='{_escape_filter_value(categoria.upper())}'")

    return " AND ".join(clauses) if clauses else "1=1"


async def _fetch_and_parse_tabular(
    layer_key: str,
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    where = _build_where(layer_key, uf=uf, bioma=bioma, categoria=categoria)
    logger.info(f"sfb_{layer_key}", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    pages, source_url = await client.fetch_layer(layer_key, where=where, bbox=bbox, f="json")
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_layer_tabular(pages, layer_key=layer_key)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "sfb",
        source_url,
        "httpx+arcgis+json",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=[f"sfb_{layer_key}"],
        selected_source=f"sfb_{layer_key}",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def _fetch_and_parse_geo(
    layer_key: str,
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> Any:
    where = _build_where(layer_key, uf=uf, bioma=bioma, categoria=categoria)
    logger.info(f"sfb_{layer_key}_geo", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    pages, source_url = await client.fetch_layer(layer_key, where=where, bbox=bbox, f="geojson")
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_layer_geojson(pages, layer_key=layer_key)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = build_source_meta(
            "sfb",
            source_url,
            "httpx+arcgis+geojson",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            attempted_sources=[f"sfb_{layer_key}_geo"],
            selected_source=f"sfb_{layer_key}_geo",
        )
        return gdf, meta
    return gdf


# --- cnfp ---


@overload
async def cnfp(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cnfp(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def cnfp(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    uf = validate_uf(uf)
    bioma = validate_bioma(bioma)
    categoria = _validate_categoria(categoria)
    bbox = validate_bbox(bbox)
    return await _fetch_and_parse_tabular(
        "cnfp",
        uf=uf,
        bioma=bioma,
        categoria=categoria,
        bbox=bbox,
        as_polars=as_polars,
        return_meta=return_meta,
    )


@overload
async def cnfp_geo(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def cnfp_geo(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def cnfp_geo(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    uf = validate_uf(uf)
    bioma = validate_bioma(bioma)
    categoria = _validate_categoria(categoria)
    bbox = validate_bbox(bbox)
    return await _fetch_and_parse_geo(
        "cnfp",
        uf=uf,
        bioma=bioma,
        categoria=categoria,
        bbox=bbox,
        return_meta=return_meta,
    )


# --- concessoes ---


@overload
async def concessoes(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def concessoes(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def concessoes(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    uf = validate_uf(uf)
    bbox = validate_bbox(bbox)
    return await _fetch_and_parse_tabular(
        "concessoes",
        uf=uf,
        bbox=bbox,
        as_polars=as_polars,
        return_meta=return_meta,
    )


@overload
async def concessoes_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def concessoes_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def concessoes_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    uf = validate_uf(uf)
    bbox = validate_bbox(bbox)
    return await _fetch_and_parse_geo(
        "concessoes",
        uf=uf,
        bbox=bbox,
        return_meta=return_meta,
    )


# --- ifn_conglomerados ---


@overload
async def ifn_conglomerados(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ifn_conglomerados(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def ifn_conglomerados(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    uf = validate_uf(uf)
    bioma = validate_bioma(bioma)
    bbox = validate_bbox(bbox)
    return await _fetch_and_parse_tabular(
        "ifn_conglomerados",
        uf=uf,
        bioma=bioma,
        bbox=bbox,
        as_polars=as_polars,
        return_meta=return_meta,
    )


@overload
async def ifn_conglomerados_geo(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def ifn_conglomerados_geo(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def ifn_conglomerados_geo(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    uf = validate_uf(uf)
    bioma = validate_bioma(bioma)
    bbox = validate_bbox(bbox)
    return await _fetch_and_parse_geo(
        "ifn_conglomerados",
        uf=uf,
        bioma=bioma,
        bbox=bbox,
        return_meta=return_meta,
    )
