from __future__ import annotations

import hashlib
import time
from typing import TYPE_CHECKING, Literal, cast, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.geo import validate_bbox
from agrobr.utils.result import (
    DataFrameResult,
    GeoDataFrameResult,
    build_source_meta,
    finalize_result,
)
from agrobr.utils.validation import validate_bioma, validate_uf

from . import client, models, parser

if TYPE_CHECKING:
    import geopandas as gpd
    import polars as pl

logger = _log.get_logger(__name__)

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
    normalized = categoria.strip().upper() if isinstance(categoria, str) else ""
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
        clauses.append(f"{field}='{_escape_filter_value(uf)}'")
    if bioma:
        field = fields.get("bioma", "bioma")
        clauses.append(f"{field}='{_escape_filter_value(bioma.upper())}'")
    if categoria:
        field = fields.get("categoria", "categoria")
        clauses.append(f"{field}='{_escape_filter_value(categoria)}'")

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
) -> DataFrameResult:
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
        schema_version=models.SCHEMA_VERSIONS.get(layer_key, "1.0"),
        attempted_sources=[f"sfb_{layer_key}"],
        selected_source=f"sfb_{layer_key}",
        raw_content_hash=hashlib.sha256(pages[0]).hexdigest() if len(pages) == 1 else None,
        raw_content_size=len(pages[0]) if len(pages) == 1 else 0,
    )
    return finalize_result(
        df,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=tuple(
            str(column) for column in df if pd.api.types.is_string_dtype(df[column].dtype)
        ),
    )


async def _fetch_and_parse_geo(
    layer_key: str,
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    where = _build_where(layer_key, uf=uf, bioma=bioma, categoria=categoria)
    logger.info(f"sfb_{layer_key}_geo", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    pages, source_url = await client.fetch_layer(layer_key, where=where, bbox=bbox, f="geojson")
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = cast("gpd.GeoDataFrame", parser.parse_layer_geojson(pages, layer_key=layer_key))
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
            schema_version=models.SCHEMA_VERSIONS.get(layer_key, "1.0"),
            attempted_sources=[f"sfb_{layer_key}_geo"],
            selected_source=f"sfb_{layer_key}_geo",
            raw_content_hash=hashlib.sha256(pages[0]).hexdigest() if len(pages) == 1 else None,
            raw_content_size=len(pages[0]) if len(pages) == 1 else 0,
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
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cnfp(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def cnfp(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def cnfp(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def cnfp(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def cnfp(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
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


@overload
async def cnfp_geo(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult: ...


async def cnfp_geo(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    categoria: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
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
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def concessoes(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def concessoes(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def concessoes(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def concessoes(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def concessoes(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
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


@overload
async def concessoes_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult: ...


async def concessoes_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
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
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ifn_conglomerados(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def ifn_conglomerados(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def ifn_conglomerados(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def ifn_conglomerados(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def ifn_conglomerados(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
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


@overload
async def ifn_conglomerados_geo(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult: ...


async def ifn_conglomerados_geo(
    *,
    uf: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
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
