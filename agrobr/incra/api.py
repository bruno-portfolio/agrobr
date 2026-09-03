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
from .models import FASES_VALIDAS

if TYPE_CHECKING:
    import geopandas as gpd

logger = structlog.get_logger()


def _validate_fase(fase: str | None) -> str | None:
    if fase is None:
        return None
    if fase not in FASES_VALIDAS:
        raise InvalidParameterError(
            f"Fase invalida: {fase!r}. Valores aceitos: {sorted(FASES_VALIDAS)}"
        )
    return fase


@overload
async def quilombolas(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def quilombolas(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def quilombolas(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Territorios quilombolas via WFS do servidor CMR/FUNAI.

    Note:
        Campo `codigo` (cd_quilomb) e nullable em ~63% dos registros — sao
        territorios em pre-cadastro identificados pelo CMR/FUNAI que ainda
        nao receberam codigo INCRA oficial. Use `df["codigo"].notna()` para
        filtrar apenas cadastrados. Filtros `uf` e `fase` sao aplicados
        client-side (servidor ignora `CQL_FILTER`).
    """
    uf = validate_uf(uf)
    fase = _validate_fase(fase)
    bbox = validate_bbox(bbox)
    logger.info("incra_quilombolas", uf=uf, fase=fase, bbox=bbox)

    t0 = time.monotonic()
    csv_bytes, source_url = await client.fetch_quilombolas(bbox=bbox)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_quilombolas_csv(csv_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if uf is not None and not df.empty:
        df = df[df["uf"] == uf].reset_index(drop=True)
    if fase is not None and not df.empty:
        df = df[df["fase"] == fase].reset_index(drop=True)

    meta = build_source_meta(
        "incra",
        source_url,
        "httpx+wfs+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["incra_wfs"],
        selected_source="incra_wfs",
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def quilombolas_geo(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def quilombolas_geo(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def quilombolas_geo(
    *,
    uf: str | None = None,
    fase: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    """Territorios quilombolas como GeoDataFrame via WFS do CMR/FUNAI.

    Note:
        Campo `codigo` (cd_quilomb) e nullable em ~63% dos registros (pre-cadastro
        sem codigo INCRA oficial). Geometrias invalidas sao reparadas via
        `make_valid` (log `incra_quilombolas_geo_repaired`). Filtros `uf` e `fase`
        sao client-side.
    """
    uf = validate_uf(uf)
    fase = _validate_fase(fase)
    bbox = validate_bbox(bbox)
    logger.info("incra_quilombolas_geo", uf=uf, fase=fase, bbox=bbox)

    t0 = time.monotonic()
    geojson_bytes, source_url = await client.fetch_quilombolas_geo(bbox=bbox)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_quilombolas_geojson(geojson_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if uf is not None and not gdf.empty:
        gdf = gdf[gdf["uf"] == uf].reset_index(drop=True)
    if fase is not None and not gdf.empty:
        gdf = gdf[gdf["fase"] == fase].reset_index(drop=True)

    if return_meta:
        meta = build_source_meta(
            "incra",
            source_url,
            "httpx+wfs+geojson",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            attempted_sources=["incra_wfs_geo"],
            selected_source="incra_wfs_geo",
        )
        return gdf, meta

    return gdf
