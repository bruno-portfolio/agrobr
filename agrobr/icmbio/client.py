from __future__ import annotations

from urllib import parse

import structlog

from agrobr.http.settings import get_timeout
from agrobr.utils.geo import build_wfs_url, fetch_wfs

from .models import (
    GEOM_COLUMN,
    LAYER,
    MAX_FEATURES_GEO,
    MAX_FEATURES_TABULAR,
    NAMESPACE,
    PROPERTY_NAMES,
    PROPERTY_NAMES_GEO,
    WFS_BASE,
    WFS_VERSION,
)

logger = structlog.get_logger()

TIMEOUT = get_timeout(read=120.0)


def _escape_filter_value(value: str) -> str:
    return value.replace("'", "''")


def _build_cql_filters(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
) -> str | None:
    filters: list[str] = []
    if uf is not None:
        filters.append(f"uf LIKE '%{_escape_filter_value(uf)}%'")
    if grupo is not None:
        grupo_upper = _escape_filter_value(grupo.strip().upper())
        filters.append(f"grupouc='{grupo_upper}'")
    if bioma is not None:
        filters.append(f"biomas ILIKE '%{_escape_filter_value(bioma)}%'")
    if bbox is not None:
        coordinates = ",".join(str(value) for value in bbox)
        filters.append(f"BBOX({GEOM_COLUMN},{coordinates},'EPSG:4674')")
    return " AND ".join(filters) if filters else None


def _tabular_url(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    result_type: str | None = None,
) -> str:
    url = build_wfs_url(
        WFS_BASE,
        NAMESPACE,
        LAYER,
        WFS_VERSION,
        PROPERTY_NAMES,
        max_features=MAX_FEATURES_TABULAR,
        cql_filter=_build_cql_filters(uf=uf, grupo=grupo, bioma=bioma, bbox=bbox),
        result_type=result_type,
    )
    if result_type == "hits":
        parts = parse.urlsplit(url)
        query = dict(parse.parse_qsl(parts.query))
        for key in ("maxFeatures", "outputFormat", "propertyName"):
            query.pop(key, None)
        url = parse.urlunsplit(parts._replace(query=parse.urlencode(query)))
    return url


async def fetch_ucs_count(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
) -> tuple[bytes, str]:
    url = _tabular_url(uf=uf, grupo=grupo, bioma=bioma, bbox=bbox, result_type="hits")
    content = await fetch_wfs(url, source="icmbio", timeout=TIMEOUT)
    return content, url


async def fetch_ucs(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
) -> tuple[bytes, str]:
    url = _tabular_url(uf=uf, grupo=grupo, bioma=bioma, bbox=bbox)
    content = await fetch_wfs(url, source="icmbio", timeout=TIMEOUT)
    logger.info("icmbio_ucs_csv", source="icmbio", size=len(content))
    return content, url


async def fetch_ucs_geo(
    *,
    bbox: tuple[float, float, float, float] | None = None,
) -> tuple[bytes, str]:
    url = build_wfs_url(
        WFS_BASE,
        NAMESPACE,
        LAYER,
        WFS_VERSION,
        PROPERTY_NAMES_GEO,
        max_features=MAX_FEATURES_GEO,
        output_format="application/json",
        bbox=bbox,
        srs_name="EPSG:4326",
    )
    content = await fetch_wfs(url, source="icmbio", timeout=TIMEOUT)
    logger.info("icmbio_ucs_geojson", source="icmbio", size=len(content))
    return content, url
