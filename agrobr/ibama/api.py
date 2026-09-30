from __future__ import annotations

import hashlib
import time
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.models import MetaInfo
from agrobr.utils.geo import validate_bbox
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.validation import validate_uf

from . import client, models, parser

if TYPE_CHECKING:
    import geopandas as gpd

logger = _log.get_logger(__name__)


def _details(df: pd.DataFrame) -> dict[str, Any]:
    return {"ultima_atualizacao_relatorio": df.attrs.get(models.EDICAO_COLUMN_CSV)}


@overload
async def embargos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def embargos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def embargos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    uf = validate_uf(uf)
    bbox = validate_bbox(bbox)
    logger.info("ibama_embargos", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    csv_bytes, source_url = await client.fetch_embargos_csv()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_embargos_csv(csv_bytes, uf=uf, bbox=bbox)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "ibama",
        source_url,
        "httpx+csv",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["ibama_sifisc"],
        selected_source="ibama_sifisc",
        raw_content_hash=hashlib.sha256(csv_bytes).hexdigest(),
        raw_content_size=len(csv_bytes),
        source_details=_details(df),
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def embargos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def embargos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def embargos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> Any:
    uf = validate_uf(uf)
    bbox = validate_bbox(bbox)
    logger.info("ibama_embargos_geo", uf=uf, bbox=bbox)

    t0 = time.monotonic()
    csv_bytes, source_url = await client.fetch_embargos_csv()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_embargos_geo(csv_bytes, uf=uf, bbox=bbox)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = build_source_meta(
            "ibama",
            source_url,
            "httpx+csv+wkt",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            attempted_sources=["ibama_sifisc"],
            selected_source="ibama_sifisc",
            raw_content_hash=hashlib.sha256(csv_bytes).hexdigest(),
            raw_content_size=len(csv_bytes),
            source_details=_details(gdf),
        )
        return gdf, meta

    return gdf
