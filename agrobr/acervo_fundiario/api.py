from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.utils.geo import check_pyogrio, validate_bbox
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_uf as _validate_uf_optional
from agrobr.utils.warnings import warn_once

from . import client, parser
from .models import (
    BASE_URL,
    FILENAME_PATTERNS,
    SIGEF_UFS_DISPONIVEIS,
    SNCI_UFS_DISPONIVEIS,
)

if TYPE_CHECKING:
    import geopandas as gpd

logger = structlog.get_logger()

_NC_WARNING = (
    "Acervo Fundiario/INCRA: vedado o uso comercial — uso comercial requer "
    "autorizacao. Classificacao: nc. Veja docs/licenses.md."
)

_SOURCE_METHOD = "httpx+pyogrio+shapefile_zip"


def _validate_uf(uf: str) -> str:
    result = _validate_uf_optional(uf)
    if result is None:
        raise InvalidParameterError(f"UF invalida: {uf!r}")
    return result


def _validate_uf_for_dataset(uf: str, disponiveis: frozenset[str], dataset: str) -> str:
    uf = _validate_uf(uf)
    if uf not in disponiveis:
        url = BASE_URL + FILENAME_PATTERNS[dataset].format(uf=uf)
        raise SourceUnavailableError(
            source="acervo_fundiario",
            url=url,
            last_error=(
                f"UF {uf!r} nao disponivel em {dataset.upper()}. "
                f"Disponiveis: {', '.join(sorted(disponiveis))}"
            ),
        )
    return uf


def _build_meta(
    *,
    tema: str,
    uf: str | None,
    fetch_ms: int,
    parse_ms: int,
    df: Any,
) -> MetaInfo:
    filename = FILENAME_PATTERNS[tema].format(uf=uf) if uf else FILENAME_PATTERNS[tema]
    source_url = BASE_URL + filename
    return build_source_meta(
        "acervo_fundiario",
        source_url,
        _SOURCE_METHOD,
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=[f"acervo_fundiario_{tema}"],
        selected_source=f"acervo_fundiario_{tema}",
    )


@overload
async def sigef(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def sigef(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def sigef(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    check_pyogrio()
    uf = _validate_uf_for_dataset(uf, SIGEF_UFS_DISPONIVEIS, "sigef")
    bbox = validate_bbox(bbox)
    logger.info("acervo_fundiario_sigef", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    zip_path = await client.download_and_cache("sigef", uf, use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_sigef(zip_path, bbox=bbox)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(tema="sigef", uf=uf, fetch_ms=fetch_ms, parse_ms=parse_ms, df=df)
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
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    check_pyogrio()
    uf = _validate_uf_for_dataset(uf, SIGEF_UFS_DISPONIVEIS, "sigef")
    bbox = validate_bbox(bbox)
    logger.info("acervo_fundiario_sigef_geo", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    zip_path = await client.download_and_cache("sigef", uf, use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_sigef_geo(zip_path, bbox=bbox)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = _build_meta(tema="sigef", uf=uf, fetch_ms=fetch_ms, parse_ms=parse_ms, df=gdf)
        return gdf, meta
    return gdf


@overload
async def snci(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def snci(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def snci(
    uf: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    check_pyogrio()
    uf = _validate_uf_for_dataset(uf, SNCI_UFS_DISPONIVEIS, "snci")
    bbox = validate_bbox(bbox)
    logger.info("acervo_fundiario_snci", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    zip_path = await client.download_and_cache("snci", uf, use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_snci(zip_path, bbox=bbox)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(tema="snci", uf=uf, fetch_ms=fetch_ms, parse_ms=parse_ms, df=df)
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
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    check_pyogrio()
    uf = _validate_uf_for_dataset(uf, SNCI_UFS_DISPONIVEIS, "snci")
    bbox = validate_bbox(bbox)
    logger.info("acervo_fundiario_snci_geo", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    zip_path = await client.download_and_cache("snci", uf, use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_snci_geo(zip_path, bbox=bbox)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = _build_meta(tema="snci", uf=uf, fetch_ms=fetch_ms, parse_ms=parse_ms, df=gdf)
        return gdf, meta
    return gdf


@overload
async def assentamentos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def assentamentos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def assentamentos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    check_pyogrio()
    uf_norm = _validate_uf(uf) if uf is not None else None
    bbox = validate_bbox(bbox)
    logger.info("acervo_fundiario_assentamentos", uf=uf_norm, bbox=bbox)

    t0 = time.monotonic()
    zip_path = await client.download_and_cache("assentamentos", use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_assentamentos(zip_path, uf=uf_norm, bbox=bbox)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _build_meta(tema="assentamentos", uf=None, fetch_ms=fetch_ms, parse_ms=parse_ms, df=df)
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
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    warn_once("acervo_fundiario_license", _NC_WARNING)
    check_pyogrio()
    uf_norm = _validate_uf(uf) if uf is not None else None
    bbox = validate_bbox(bbox)
    logger.info("acervo_fundiario_assentamentos_geo", uf=uf_norm, bbox=bbox)

    t0 = time.monotonic()
    zip_path = await client.download_and_cache("assentamentos", use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_assentamentos_geo(zip_path, uf=uf_norm, bbox=bbox)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = _build_meta(
            tema="assentamentos", uf=None, fetch_ms=fetch_ms, parse_ms=parse_ms, df=gdf
        )
        return gdf, meta
    return gdf
