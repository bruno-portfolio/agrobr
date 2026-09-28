"""Utilitários gerais."""

from __future__ import annotations

from agrobr.utils.geo import (
    build_wfs_url,
    check_geopandas,
    fetch_wfs,
    parse_geojson_base,
    parse_wfs_hits,
    validate_bbox,
)
from agrobr.utils.html import parse_links_from_html
from agrobr.utils.io import open_excel_safe, read_csv_safe, read_excel_safe
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.time import utcnow
from agrobr.utils.validation import validate_bioma, validate_uf, validate_year_uf
from agrobr.utils.warnings import warn_once, warn_once_reset

__all__: list[str] = [
    "build_source_meta",
    "build_wfs_url",
    "check_geopandas",
    "fetch_wfs",
    "finalize_result",
    "parse_geojson_base",
    "parse_wfs_hits",
    "validate_bbox",
    "open_excel_safe",
    "parse_links_from_html",
    "read_csv_safe",
    "read_excel_safe",
    "utcnow",
    "validate_bioma",
    "validate_uf",
    "validate_year_uf",
    "warn_once",
    "warn_once_reset",
]
