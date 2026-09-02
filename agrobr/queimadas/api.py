from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo
from agrobr.utils.geo import check_geopandas
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_bioma

from . import client, parser

if TYPE_CHECKING:
    import geopandas as gpd

logger = structlog.get_logger()


@overload
async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    bioma = validate_bioma(bioma)
    logger.info(
        "queimadas_focos",
        ano=ano,
        mes=mes,
        dia=dia,
        uf=uf,
        bioma=bioma,
        satelite=satelite,
    )

    t0 = time.monotonic()

    if dia is not None:
        data_str = f"{ano:04d}{mes:02d}{dia:02d}"
        csv_bytes, source_url = await client.fetch_focos_diario(data_str)
    else:
        csv_bytes, source_url = await client.fetch_focos_mensal(ano, mes)

    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_focos_csv(csv_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if dia is None and "anual" in source_url:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df = df[df["data"].dt.month == mes].reset_index(drop=True)
        df["data"] = df["data"].dt.date

    if uf is not None:
        uf_upper = uf.strip().upper()
        df = df[df["uf"] == uf_upper].reset_index(drop=True)

    if bioma is not None:
        df = df[df["bioma"] == bioma].reset_index(drop=True)

    if satelite is not None:
        df = df[df["satelite"] == satelite].reset_index(drop=True)

    meta = build_source_meta(
        "queimadas",
        source_url,
        "httpx+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def focos_geo(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def focos_geo(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def focos_geo(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> Any:
    gpd_mod = check_geopandas()

    if return_meta:
        df, meta = await focos(
            ano=ano,
            mes=mes,
            dia=dia,
            uf=uf,
            bioma=bioma,
            satelite=satelite,
            return_meta=True,
        )
    else:
        df = await focos(
            ano=ano,
            mes=mes,
            dia=dia,
            uf=uf,
            bioma=bioma,
            satelite=satelite,
        )

    geometry = gpd_mod.points_from_xy(df["lon"], df["lat"])
    gdf = gpd_mod.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    if return_meta:
        return gdf, meta
    return gdf
