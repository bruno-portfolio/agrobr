from __future__ import annotations

import structlog

from agrobr.http.settings import get_timeout
from agrobr.utils.geo import build_wfs_url, fetch_wfs

from .models import (
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
) -> str | None:
    filters: list[str] = []
    if uf is not None:
        filters.append(f"uf LIKE '%{_escape_filter_value(uf)}%'")
    if grupo is not None:
        grupo_upper = _escape_filter_value(grupo.strip().upper())
        filters.append(f"grupouc='{grupo_upper}'")
    if bioma is not None:
        filters.append(f"biomas ILIKE '%{_escape_filter_value(bioma)}%'")
    return " AND ".join(filters) if filters else None


async def fetch_ucs(
    *,
    uf: str | None = None,
    grupo: str | None = None,
    bioma: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
) -> tuple[bytes, str]:
    cql = _build_cql_filters(uf=uf, grupo=grupo, bioma=bioma)
    url = build_wfs_url(
        WFS_BASE,
        NAMESPACE,
        LAYER,
        WFS_VERSION,
        PROPERTY_NAMES,
        max_features=MAX_FEATURES_TABULAR,
        cql_filter=cql,
        bbox=bbox,
    )
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
    )
    content = await fetch_wfs(url, source="icmbio", timeout=TIMEOUT)
    logger.info("icmbio_ucs_geojson", source="icmbio", size=len(content))
    return content, url
