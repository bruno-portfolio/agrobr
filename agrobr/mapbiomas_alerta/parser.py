from __future__ import annotations

from typing import Any

import pandas as pd

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.normalize import dates
from agrobr.utils.geo import check_geopandas, wkt_within_limits

from .models import COLUNAS_SAIDA, COLUNAS_SAIDA_GEO, RENAME_MAP

logger = _log.get_logger(__name__)

PARSER_VERSION = 2

_REQUIRED_FIELDS = {"alertCode", "areaHa", "detectedAt"}
_TIPOS = {
    "alert_code": "Int64",
    "area_ha": "float64",
    "data_deteccao": "datetime64[ns]",
    "data_publicacao": "datetime64[ns]",
    "lat": "float64",
    "lon": "float64",
}


def _vazio() -> pd.DataFrame:
    return pd.DataFrame(
        {
            coluna: pd.Series(dtype=_TIPOS[coluna])
            if coluna in _TIPOS
            else pd.Series([""]).iloc[:0]
            for coluna in COLUNAS_SAIDA
        }
    )


def _flatten_sources(sources: Any) -> str:
    if not sources:
        return ""
    if isinstance(sources, list):
        return ", ".join(str(s) for s in sources if s)
    return str(sources)


def _normalize_records(
    records: list[dict[str, object]],
) -> tuple[pd.DataFrame, list[Any]]:
    if not records:
        return _vazio(), []

    rows: list[dict[str, object]] = []
    geometries: list[Any] = []
    for rec in records:
        row = dict(rec)
        raw_coords = row.pop("coordenates", None)
        coords = raw_coords if isinstance(raw_coords, dict) else {}
        row["lat"] = coords.get("latitude")
        row["lon"] = coords.get("longitude")

        raw_sources = row.pop("sources", None)
        row["fonte"] = _flatten_sources(raw_sources)

        geometries.append(row.pop("geometryWkt", None))
        rows.append(row)

    df = pd.DataFrame(rows)

    missing = _REQUIRED_FIELDS - set(df.columns)
    if missing:
        raise ParseError(
            source="mapbiomas_alerta",
            parser_version=PARSER_VERSION,
            reason=f"Campos obrigatorios ausentes: {missing}",
        )

    df = df.rename(columns=RENAME_MAP)
    dates.converter_coluna(df, "data_deteccao", fonte="mapbiomas_alerta")
    dates.converter_coluna(df, "data_publicacao", fonte="mapbiomas_alerta")
    try:
        df["alert_code"] = df["alert_code"].astype("Int64")
    except (TypeError, ValueError) as exc:
        raise ParseError(
            source="mapbiomas_alerta",
            parser_version=PARSER_VERSION,
            reason="alertCode publicado sem código inteiro",
        ) from exc
    df["area_ha"] = pd.to_numeric(df["area_ha"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    return df, geometries


def parse_alertas(records: list[dict[str, object]]) -> pd.DataFrame:
    df, _ = _normalize_records(records)
    if df.empty:
        return df
    output_cols = [c for c in COLUNAS_SAIDA if c in df.columns]
    df = df[output_cols].reset_index(drop=True)
    logger.info("mapbiomas_alerta_parse_ok", records=len(df))
    return df


def parse_alertas_geo(records: list[dict[str, object]]) -> Any:
    gpd = check_geopandas()
    df, wkt_strings = _normalize_records(records)
    if df.empty:
        return gpd.GeoDataFrame(df, geometry=gpd.GeoSeries([], crs="EPSG:4326"))

    from shapely import wkt
    from shapely.errors import GEOSException

    geoms: list[Any] = []
    for wkt_str in wkt_strings:
        geom = None
        if wkt_str and not wkt_within_limits(wkt_str):
            logger.warning("mapbiomas_alerta_invalid_wkt")
        elif wkt_str:
            try:
                geom = wkt.loads(wkt_str)
            except (GEOSException, ValueError):
                logger.warning("mapbiomas_alerta_invalid_wkt")
        geoms.append(geom)

    gdf = gpd.GeoDataFrame(df, geometry=geoms, crs="EPSG:4326")
    gdf.attrs.update(df.attrs)
    null_geom = gdf.geometry.isna().sum()
    if null_geom:
        logger.warning("mapbiomas_alerta_null_geometry", null_count=int(null_geom), total=len(gdf))

    output_cols = [c for c in COLUNAS_SAIDA_GEO if c in gdf.columns]
    return gdf[output_cols].reset_index(drop=True)
