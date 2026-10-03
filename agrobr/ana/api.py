from __future__ import annotations

import hashlib
import json
import time
from typing import TYPE_CHECKING, Any, Literal, cast, overload

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
from agrobr.utils.validation import validate_uf

from . import client, parser

if TYPE_CHECKING:
    import geopandas as gpd
    import polars as pl

logger = _log.get_logger(__name__)

_UF_TO_ESTADO: dict[str, str] = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AM": "Amazonas",
    "AP": "Amapá",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MG": "Minas Gerais",
    "MS": "Mato Grosso do Sul",
    "MT": "Mato Grosso",
    "PA": "Pará",
    "PB": "Paraíba",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "PR": "Paraná",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RO": "Rondônia",
    "RR": "Roraima",
    "RS": "Rio Grande do Sul",
    "SC": "Santa Catarina",
    "SE": "Sergipe",
    "SP": "São Paulo",
    "TO": "Tocantins",
}


def _build_where(*, uf: str | None = None) -> str:
    if not uf:
        return "1=1"
    estado = _UF_TO_ESTADO.get(uf, uf).upper()
    return f"NM_ESTADO='{estado}'"


def _compor_hash_paginas(
    meta: MetaInfo,
    pages: list[bytes],
    *,
    layer_key: str,
    where: str,
    bbox: tuple[float, float, float, float] | None,
    max_registros: int | None,
    f: str,
) -> None:
    if len(pages) <= 1:
        return
    query = {
        "fonte": "ana",
        "recurso": layer_key,
        "where": where,
        "bbox": list(bbox) if bbox is not None else None,
        "max_registros": max_registros,
        "formato": f,
    }
    resources = [
        {"pagina": numero, "sha256": hashlib.sha256(page).hexdigest(), "bytes": len(page)}
        for numero, page in enumerate(pages, 1)
    ]
    manifesto = json.dumps(
        {"query": query, "resources": resources},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    meta.raw_content_hash = hashlib.sha256(manifesto).hexdigest()
    meta.raw_content_size = len(manifesto)
    meta.source_details = {
        "hash_kind": "resource_manifest_sha256",
        "manifest_encoding": "canonical_json_utf8",
        "manifest_fields": ["query", "resources"],
        "query": query,
        "resources": resources,
        "resource_bytes": sum(len(page) for page in pages),
    }


async def _fetch_and_parse_tabular(
    layer_key: str,
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    where = _build_where(uf=uf)
    logger.info(f"ana_{layer_key}", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    pages, source_url = await client.fetch_layer(
        layer_key,
        where=where,
        bbox=bbox,
        max_registros=max_registros,
        f="json",
    )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_layer_tabular(pages, layer_key=layer_key)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "ana",
        source_url,
        "httpx+arcgis+json",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=[f"ana_{layer_key}"],
        selected_source=f"ana_{layer_key}",
        raw_content_hash=hashlib.sha256(pages[0]).hexdigest() if len(pages) == 1 else None,
        raw_content_size=len(pages[0]) if len(pages) == 1 else 0,
    )
    _compor_hash_paginas(
        meta,
        pages,
        layer_key=layer_key,
        where=where,
        bbox=bbox,
        max_registros=max_registros,
        f="json",
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
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    where = _build_where(uf=uf)
    logger.info(f"ana_{layer_key}_geo", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    pages, source_url = await client.fetch_layer(
        layer_key,
        where=where,
        bbox=bbox,
        max_registros=max_registros,
        f="geojson",
    )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = cast("gpd.GeoDataFrame", parser.parse_layer_geojson(pages, layer_key=layer_key))
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = build_source_meta(
            "ana",
            source_url,
            "httpx+arcgis+geojson",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            attempted_sources=[f"ana_{layer_key}_geo"],
            selected_source=f"ana_{layer_key}_geo",
            raw_content_hash=hashlib.sha256(pages[0]).hexdigest() if len(pages) == 1 else None,
            raw_content_size=len(pages[0]) if len(pages) == 1 else 0,
        )
        _compor_hash_paginas(
            meta,
            pages,
            layer_key=layer_key,
            where=where,
            bbox=bbox,
            max_registros=max_registros,
            f="geojson",
        )
        return gdf, meta
    return gdf


# ---------------------------------------------------------------------------
# hidrografia (bbox required)
# ---------------------------------------------------------------------------


@overload
async def hidrografia(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def hidrografia(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def hidrografia(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def hidrografia(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def hidrografia(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def hidrografia(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    bbox = validate_bbox(bbox)  # type: ignore[assignment]
    return await _fetch_and_parse_tabular(
        "hidrografia",
        bbox=bbox,
        max_registros=max_registros,
        as_polars=as_polars,
        return_meta=return_meta,
    )


@overload
async def hidrografia_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def hidrografia_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


@overload
async def hidrografia_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult: ...


async def hidrografia_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    bbox = validate_bbox(bbox)  # type: ignore[assignment]
    return await _fetch_and_parse_geo(
        "hidrografia",
        bbox=bbox,
        max_registros=max_registros,
        return_meta=return_meta,
    )


# ---------------------------------------------------------------------------
# pivos_irrigacao (uf optional, bbox optional)
# ---------------------------------------------------------------------------


@overload
async def pivos_irrigacao(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def pivos_irrigacao(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def pivos_irrigacao(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def pivos_irrigacao(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def pivos_irrigacao(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def pivos_irrigacao(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    uf = validate_uf(uf)
    bbox = validate_bbox(bbox)
    return await _fetch_and_parse_tabular(
        "pivos_irrigacao",
        uf=uf,
        bbox=bbox,
        max_registros=max_registros,
        as_polars=as_polars,
        return_meta=return_meta,
    )


@overload
async def pivos_irrigacao_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def pivos_irrigacao_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


@overload
async def pivos_irrigacao_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult: ...


async def pivos_irrigacao_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    uf = validate_uf(uf)
    bbox = validate_bbox(bbox)
    return await _fetch_and_parse_geo(
        "pivos_irrigacao",
        uf=uf,
        bbox=bbox,
        max_registros=max_registros,
        return_meta=return_meta,
    )


# ---------------------------------------------------------------------------
# demanda_irrigacao (bbox required)
# ---------------------------------------------------------------------------


@overload
async def demanda_irrigacao(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def demanda_irrigacao(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def demanda_irrigacao(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def demanda_irrigacao(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def demanda_irrigacao(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def demanda_irrigacao(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    bbox = validate_bbox(bbox)  # type: ignore[assignment]
    return await _fetch_and_parse_tabular(
        "demanda_irrigacao",
        bbox=bbox,
        max_registros=max_registros,
        as_polars=as_polars,
        return_meta=return_meta,
    )


@overload
async def demanda_irrigacao_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def demanda_irrigacao_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


@overload
async def demanda_irrigacao_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult: ...


async def demanda_irrigacao_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    bbox = validate_bbox(bbox)  # type: ignore[assignment]
    return await _fetch_and_parse_geo(
        "demanda_irrigacao",
        bbox=bbox,
        max_registros=max_registros,
        return_meta=return_meta,
    )


# ---------------------------------------------------------------------------
# disponibilidade_hidrica (bbox optional, no UF field in service)
# ---------------------------------------------------------------------------


@overload
async def disponibilidade_hidrica(
    *,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def disponibilidade_hidrica(
    *,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def disponibilidade_hidrica(
    *,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def disponibilidade_hidrica(
    *,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def disponibilidade_hidrica(
    *,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def disponibilidade_hidrica(
    *,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    bbox = validate_bbox(bbox)
    return await _fetch_and_parse_tabular(
        "disponibilidade_hidrica",
        bbox=bbox,
        max_registros=max_registros,
        as_polars=as_polars,
        return_meta=return_meta,
    )


@overload
async def disponibilidade_hidrica_geo(
    *,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def disponibilidade_hidrica_geo(
    *,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


@overload
async def disponibilidade_hidrica_geo(
    *,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult: ...


async def disponibilidade_hidrica_geo(
    *,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    bbox = validate_bbox(bbox)
    return await _fetch_and_parse_geo(
        "disponibilidade_hidrica",
        bbox=bbox,
        max_registros=max_registros,
        return_meta=return_meta,
    )


# ---------------------------------------------------------------------------
# massas_dagua (uf or bbox required)
# ---------------------------------------------------------------------------


def _recorte_massas(
    uf: str | None, bbox: tuple[float, float, float, float] | None
) -> tuple[str, tuple[float, float, float, float] | None]:
    uf = validate_uf(uf)
    bbox = validate_bbox(bbox)
    if uf is None and bbox is None:
        raise InvalidParameterError(
            "massas_dagua exige uf ou bbox: o Brasil inteiro tem 240 mil polígonos (cerca de 1 GB)"
        )
    return client.massas_where(uf), bbox


async def _fetch_massas(
    *,
    uf: str | None,
    bbox: tuple[float, float, float, float] | None,
    max_registros: int | None,
    geo: bool,
) -> tuple[Any, MetaInfo]:
    where, bbox = _recorte_massas(uf, bbox)
    formato = "geojson" if geo else "json"
    logger.info("ana_massas_dagua", uf=uf, bbox=bbox, geo=geo)

    t0 = time.monotonic()
    pages, source_url = await client.fetch_massas_dagua(
        where=where, bbox=bbox, max_registros=max_registros, f=formato
    )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_massas_dagua(pages, geo=geo)
    parse_ms = int((time.monotonic() - t1) * 1000)

    fonte = "ana_massas_dagua_geo" if geo else "ana_massas_dagua"
    meta = build_source_meta(
        "ana",
        source_url,
        f"httpx+arcgis+{formato}",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=[fonte],
        selected_source=fonte,
        raw_content_hash=hashlib.sha256(pages[0]).hexdigest() if len(pages) == 1 else None,
        raw_content_size=len(pages[0]) if len(pages) == 1 else 0,
    )
    _compor_hash_paginas(
        meta,
        pages,
        layer_key="massas_dagua",
        where=where,
        bbox=bbox,
        max_registros=max_registros,
        f=formato,
    )
    return df, meta


@overload
async def massas_dagua(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def massas_dagua(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def massas_dagua(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
async def massas_dagua(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: Literal[True],
    return_meta: Literal[True],
) -> tuple[pl.DataFrame, MetaInfo]: ...


@overload
async def massas_dagua(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def massas_dagua(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    df, meta = await _fetch_massas(uf=uf, bbox=bbox, max_registros=max_registros, geo=False)
    return finalize_result(
        df,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=tuple(
            str(column) for column in df if pd.api.types.is_string_dtype(df[column].dtype)
        ),
    )


@overload
async def massas_dagua_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def massas_dagua_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


@overload
async def massas_dagua_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult: ...


async def massas_dagua_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    gdf, meta = await _fetch_massas(uf=uf, bbox=bbox, max_registros=max_registros, geo=True)
    if return_meta:
        return gdf, meta
    return cast("gpd.GeoDataFrame", gdf)
