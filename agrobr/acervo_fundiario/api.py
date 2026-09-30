from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.models import MetaInfo
from agrobr.normalize import regions
from agrobr.utils.geo import check_pyogrio, validate_bbox
from agrobr.utils.result import (
    DataFrameResult,
    GeoDataFrameResult,
    build_source_meta,
    finalize_result,
)
from agrobr.utils.validation import validate_uf
from agrobr.utils.warnings import warn_once

from . import client, parser

if TYPE_CHECKING:
    import geopandas as gpd

logger = _log.get_logger(__name__)

_NC_WARNING = (
    "Acervo Fundiario/INCRA: vedado o uso comercial — uso comercial requer "
    "autorizacao. Classificacao: nc. Veja docs/licenses.md."
)

_SOURCE_METHOD = "httpx+pyogrio+shapefile_zip"


def _build_meta(
    *,
    tema: str,
    uf: str | None,
    fetch_ms: int,
    parse_ms: int,
    df: Any,
    aquisicao: client.Aquisicao,
) -> MetaInfo:
    meta = build_source_meta(
        "acervo_fundiario",
        client._build_url(tema, uf),
        _SOURCE_METHOD,
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=[f"acervo_fundiario_{tema}"],
        selected_source=f"acervo_fundiario_{tema}",
        raw_content_hash=aquisicao.sha256,
        raw_content_size=aquisicao.size_bytes,
        source_details=aquisicao.source_details,
    )
    meta.from_cache = aquisicao.from_cache
    meta.fetched_at = aquisicao.fetched_at
    meta.fetch_timestamp = aquisicao.fetched_at
    return meta


@overload
async def sigef(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def sigef(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def sigef(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def sigef(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    uf = regions.sigla_uf(uf)
    bbox = validate_bbox(bbox)
    check_pyogrio()
    logger.info("acervo_fundiario_sigef", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    async with client.adquirir("sigef", uf, use_cache=use_cache) as aquisicao:
        fetch_ms = int((time.monotonic() - t0) * 1000)
        t1 = time.monotonic()
        df = parser.parse_sigef(aquisicao.zip_path, bbox=bbox)
        parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(
        tema="sigef", uf=uf, fetch_ms=fetch_ms, parse_ms=parse_ms, df=df, aquisicao=aquisicao
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def sigef_geo(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def sigef_geo(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def sigef_geo(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    uf = regions.sigla_uf(uf)
    bbox = validate_bbox(bbox)
    check_pyogrio()
    logger.info("acervo_fundiario_sigef_geo", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    async with client.adquirir("sigef", uf, use_cache=use_cache) as aquisicao:
        fetch_ms = int((time.monotonic() - t0) * 1000)
        t1 = time.monotonic()
        gdf = parser.parse_sigef_geo(aquisicao.zip_path, bbox=bbox)
        parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = _build_meta(
            tema="sigef", uf=uf, fetch_ms=fetch_ms, parse_ms=parse_ms, df=gdf, aquisicao=aquisicao
        )
        return gdf, meta
    return gdf


@overload
async def snci(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def snci(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def snci(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def snci(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    uf = regions.sigla_uf(uf)
    bbox = validate_bbox(bbox)
    check_pyogrio()
    logger.info("acervo_fundiario_snci", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    async with client.adquirir("snci", uf, use_cache=use_cache) as aquisicao:
        fetch_ms = int((time.monotonic() - t0) * 1000)
        t1 = time.monotonic()
        df = parser.parse_snci(aquisicao.zip_path, bbox=bbox)
        parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(
        tema="snci", uf=uf, fetch_ms=fetch_ms, parse_ms=parse_ms, df=df, aquisicao=aquisicao
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def snci_geo(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def snci_geo(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def snci_geo(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    uf = regions.sigla_uf(uf)
    bbox = validate_bbox(bbox)
    check_pyogrio()
    logger.info("acervo_fundiario_snci_geo", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    async with client.adquirir("snci", uf, use_cache=use_cache) as aquisicao:
        fetch_ms = int((time.monotonic() - t0) * 1000)
        t1 = time.monotonic()
        gdf = parser.parse_snci_geo(aquisicao.zip_path, bbox=bbox)
        parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = _build_meta(
            tema="snci", uf=uf, fetch_ms=fetch_ms, parse_ms=parse_ms, df=gdf, aquisicao=aquisicao
        )
        return gdf, meta
    return gdf


@overload
async def assentamentos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def assentamentos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def assentamentos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def assentamentos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    uf_norm = validate_uf(uf)
    bbox = validate_bbox(bbox)
    check_pyogrio()
    logger.info("acervo_fundiario_assentamentos", uf=uf_norm, bbox=bbox)

    t0 = time.monotonic()
    async with client.adquirir("assentamentos", use_cache=use_cache) as aquisicao:
        fetch_ms = int((time.monotonic() - t0) * 1000)
        t1 = time.monotonic()
        df = parser.parse_assentamentos(aquisicao.zip_path, uf=uf_norm, bbox=bbox)
        parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(
        tema="assentamentos",
        uf=None,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
        df=df,
        aquisicao=aquisicao,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def assentamentos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def assentamentos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def assentamentos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    uf_norm = validate_uf(uf)
    bbox = validate_bbox(bbox)
    check_pyogrio()
    logger.info("acervo_fundiario_assentamentos_geo", uf=uf_norm, bbox=bbox)

    t0 = time.monotonic()
    async with client.adquirir("assentamentos", use_cache=use_cache) as aquisicao:
        fetch_ms = int((time.monotonic() - t0) * 1000)
        t1 = time.monotonic()
        gdf = parser.parse_assentamentos_geo(aquisicao.zip_path, uf=uf_norm, bbox=bbox)
        parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = _build_meta(
            tema="assentamentos",
            uf=None,
            fetch_ms=fetch_ms,
            parse_ms=parse_ms,
            df=gdf,
            aquisicao=aquisicao,
        )
        return gdf, meta
    return gdf
