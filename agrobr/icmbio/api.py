from __future__ import annotations

import hashlib
import importlib
import math
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
)
from agrobr.models import MetaInfo
from agrobr.utils.geo import check_geopandas, validate_bbox
from agrobr.utils.result import DataFrameResult, GeoDataFrameResult, build_source_meta
from agrobr.utils.validation import validate_bioma, validate_uf

from . import client, models, parser
from .models import GRUPOS_VALIDOS

if TYPE_CHECKING:
    import geopandas as gpd

logger = _log.get_logger(__name__)


def _validate_grupo(grupo: str | None) -> str | None:
    if grupo is None:
        return None
    grupo_upper = grupo.strip().upper()
    if grupo_upper not in GRUPOS_VALIDOS:
        raise InvalidParameterError(
            f"Grupo invalido: {grupo!r}. Valores validos: {sorted(GRUPOS_VALIDOS)}"
        )
    return grupo_upper


def _validate_output(*, as_polars: bool, return_meta: bool) -> None:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    if as_polars:
        try:
            importlib.import_module("polars")
        except ImportError:
            raise ImportError("Instale agrobr[polars] para usar as_polars=True") from None


def _query(
    uf: str | None,
    grupo: str | None,
    bioma: str | None,
    bbox: tuple[float, float, float, float] | None,
) -> dict[str, Any]:
    for name, value in (("uf", uf), ("grupo", grupo), ("bioma", bioma)):
        if value is not None and not isinstance(value, str):
            raise InvalidParameterError(f"{name} deve ser texto ou None")
    if bbox is not None:
        if not isinstance(bbox, (tuple, list)) or len(bbox) != 4:
            raise InvalidParameterError("BBOX deve conter quatro coordenadas")
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            for value in bbox
        ):
            raise InvalidParameterError("BBOX deve conter coordenadas numéricas finitas")
    try:
        validate_bbox(bbox)
    except ValueError as exc:
        raise InvalidParameterError(str(exc)) from exc
    if bbox is not None and not (
        -180 <= bbox[0] <= 180
        and -180 <= bbox[2] <= 180
        and -90 <= bbox[1] <= 90
        and -90 <= bbox[3] <= 90
    ):
        raise InvalidParameterError("BBOX fora dos limites geográficos de longitude/latitude")
    return {
        "uf": validate_uf(uf),
        "grupo": _validate_grupo(grupo),
        "bioma": validate_bioma(bioma),
        "bbox": bbox,
    }


def _to_polars(frame: pd.DataFrame) -> Any:
    pl = importlib.import_module("polars")
    return pl.DataFrame(
        {
            name: pl.Series(
                name,
                [None if pd.isna(value) else value for value in frame[name]],
                dtype=pl.Float64
                if name == "area_ha"
                else pl.Int64
                if name == "ano_criacao"
                else pl.Utf8,
                strict=True,
            )
            for name in frame.columns
        }
    )


async def _fetch_tabular(query: dict[str, Any]) -> tuple[pd.DataFrame, MetaInfo]:
    t0 = time.monotonic()
    count_body, count_url = await client.fetch_ucs_count(**query)
    expected = parser.parse_feature_count(count_body)
    if expected > models.MAX_FEATURES_TABULAR:
        raise ResourceLimitError(
            "icmbio",
            f"Seleção de {expected} feições excede o limite de {models.MAX_FEATURES_TABULAR}; refine os filtros",
            url=count_url,
        )
    csv_bytes, source_url = await client.fetch_ucs(**query)
    acquired_at = datetime.now(UTC)
    fetch_ms = int((time.monotonic() - t0) * 1000)
    t1 = time.monotonic()
    frame = parser.parse_ucs_csv(csv_bytes)
    if len(frame) != expected:
        raise ParseError(
            source="icmbio",
            parser_version=parser.PARSER_VERSION,
            reason=f"Contagem divergente: {expected} feições anunciadas, {len(frame)} recebidas; coleção alterada ou coleta incompleta",
        )
    parse_ms = int((time.monotonic() - t1) * 1000)
    meta = build_source_meta(
        "icmbio",
        source_url,
        "httpx+wfs+csv",
        fetch_ms,
        parse_ms,
        frame,
        parser.PARSER_VERSION,
        attempted_sources=["icmbio_wfs"],
        selected_source="icmbio_wfs",
        raw_content_hash=hashlib.sha256(csv_bytes).hexdigest(),
        source_details={
            "layer": f"{models.NAMESPACE}:{models.LAYER}",
            "query": query,
            "coverage": {
                "expected": expected,
                "returned": len(frame),
                "status": "count_reconciled",
                "limit": models.MAX_FEATURES_TABULAR,
                "transactional_snapshot": False,
            },
            "count": {
                "url": count_url,
                "sha256": hashlib.sha256(count_body).hexdigest(),
                "bytes": len(count_body),
                "order": "before_feature_acquisition",
            },
            "edition": None,
            "temporal_scope": "current_layer; creation_year_is_an_attribute_not_an_edition",
        },
    )
    meta.fetched_at = acquired_at
    meta.fetch_timestamp = acquired_at
    meta.timestamp = datetime.now(UTC)
    meta.raw_content_size = len(csv_bytes)
    return frame, meta


@overload
async def ucs(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ucs(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def ucs(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def ucs(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> DataFrameResult:
    if kwargs:
        raise TypeError(f"Argumentos desconhecidos em icmbio.ucs: {sorted(kwargs)}")
    _validate_output(as_polars=as_polars, return_meta=return_meta)
    query = _query(uf, grupo, bioma, bbox)
    logger.info("icmbio_ucs", **query)
    frame, meta = await _fetch_tabular(query)
    result = _to_polars(frame) if as_polars else frame
    return (result, meta) if return_meta else result


@overload
async def ucs_geo(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[False] = False,
) -> gpd.GeoDataFrame: ...


@overload
async def ucs_geo(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: Literal[True],
) -> tuple[gpd.GeoDataFrame, MetaInfo]: ...


async def ucs_geo(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> GeoDataFrameResult:
    uf = validate_uf(uf)
    grupo = _validate_grupo(grupo)
    bioma = validate_bioma(bioma)
    validate_bbox(bbox)
    logger.info("icmbio_ucs_geo", uf=uf, grupo=grupo, bioma=bioma, bbox=bbox)
    check_geopandas()

    t0 = time.monotonic()
    count_body, count_url = await client.fetch_ucs_count(bbox=bbox)
    expected = parser.parse_feature_count(count_body)
    if expected > models.MAX_FEATURES_GEO:
        raise ResourceLimitError(
            "icmbio",
            f"Consulta tem {expected} feições; limite geo={models.MAX_FEATURES_GEO}. Restrinja bbox.",
            url=count_url,
        )
    geojson_bytes, source_url = await client.fetch_ucs_geo(bbox=bbox)
    acquired_at = datetime.now(UTC)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    gdf = parser.parse_ucs_geojson(geojson_bytes)
    if len(gdf) != expected:
        raise ParseError(
            source="icmbio",
            parser_version=parser.PARSER_VERSION,
            reason=f"Contagem geo divergente: hits={expected}, recebidas={len(gdf)}",
        )
    parse_ms = int((time.monotonic() - t1) * 1000)

    if uf is not None and not gdf.empty:
        gdf = gdf[gdf["uf"].str.contains(uf, na=False, regex=False)].reset_index(drop=True)
    if grupo is not None and not gdf.empty:
        gdf = gdf[gdf["grupo"] == grupo].reset_index(drop=True)
    if bioma is not None and not gdf.empty:
        gdf = gdf[gdf["bioma"].str.contains(bioma, case=False, regex=False, na=False)].reset_index(
            drop=True
        )

    if return_meta:
        meta = build_source_meta(
            "icmbio",
            source_url,
            "httpx+wfs+geojson",
            fetch_ms,
            parse_ms,
            gdf,
            parser.PARSER_VERSION,
            attempted_sources=["icmbio_wfs_geo"],
            selected_source="icmbio_wfs_geo",
            raw_content_hash=hashlib.sha256(geojson_bytes).hexdigest(),
            raw_content_size=len(geojson_bytes),
            source_details={
                "query": {"uf": uf, "grupo": grupo, "bioma": bioma, "bbox": bbox},
                "filtros_locais": {
                    nome: valor
                    for nome, valor in (("uf", uf), ("grupo", grupo), ("bioma", bioma))
                    if valor is not None
                },
            },
        )
        meta.fetched_at = acquired_at
        meta.fetch_timestamp = acquired_at
        meta.source_details["coverage"] = {
            "status": "count_reconciled",
            "expected_features": expected,
            "received_features": expected,
            "returned_features": len(gdf),
            "count_url": count_url,
        }
        return gdf, meta

    return gdf
