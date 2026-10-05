from __future__ import annotations

import asyncio
import hashlib
import time
from typing import TYPE_CHECKING, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.models import MetaInfo
from agrobr.utils.geo import check_geopandas, validate_bbox
from agrobr.utils.result import (
    DataFrameResult,
    GeoDataFrameResult,
    build_source_meta,
    check_polars,
    finalize_result,
)
from agrobr.utils.validation import validate_uf

from . import _cache, models, parser

if TYPE_CHECKING:
    import geopandas as gpd

logger = _log.get_logger(__name__)


def _meta(
    coleta: _cache.Coleta, df: pd.DataFrame, method: str, fetch_ms: int, parse_ms: int
) -> MetaInfo:
    meta = build_source_meta(
        "ibama",
        coleta.url,
        method,
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["ibama_sifisc"],
        selected_source="ibama_sifisc",
        raw_content_hash=hashlib.sha256(coleta.conteudo).hexdigest(),
        raw_content_size=len(coleta.conteudo),
        source_details={"ultima_atualizacao_relatorio": df.attrs.get(models.EDICAO_COLUMN_CSV)},
    )
    meta.from_cache = coleta.from_cache
    meta.fetched_at = meta.fetch_timestamp = coleta.fetched_at
    return meta


@overload
async def embargos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def embargos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def embargos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def embargos(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    """Termos de embargo do IBAMA.

    O CSV da fonte (~208 MB) fica em cache em disco por `models.CACHE_TTL` (1 h) a partir da
    coleta; `use_cache=False` baixa de novo sem ler nem gravar o cache. O `MetaInfo` traz o
    instante da coleta em `fetched_at` e `from_cache=True` quando ela veio do cache.
    """
    uf = validate_uf(uf)
    bbox = validate_bbox(bbox)
    check_polars(as_polars)
    logger.info("ibama_embargos", uf=uf, bbox=bbox, use_cache=use_cache)

    t0 = time.monotonic()
    coleta = await _cache.obter_embargos_csv(use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = await asyncio.to_thread(parser.parse_embargos_csv, coleta.conteudo, uf=uf, bbox=bbox)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = _meta(coleta, df, "httpx+csv", fetch_ms, parse_ms)
    return finalize_result(
        df,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=models.COLUNAS_TEXTO,
    )


@overload
async def embargos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def embargos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def embargos_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    use_cache: bool = True,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    """Termos de embargo do IBAMA com o polígono embargado; cache como em `embargos`."""
    uf = validate_uf(uf)
    bbox = validate_bbox(bbox)
    check_geopandas()
    logger.info("ibama_embargos_geo", uf=uf, bbox=bbox, use_cache=use_cache)

    t0 = time.monotonic()
    coleta = await _cache.obter_embargos_csv(use_cache=use_cache)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = await asyncio.to_thread(parser.parse_embargos_geo, coleta.conteudo, uf=uf, bbox=bbox)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        return gdf, _meta(coleta, gdf, "httpx+csv+wkt", fetch_ms, parse_ms)
    return gdf
