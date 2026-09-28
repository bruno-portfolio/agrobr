from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pandas as pd

from agrobr.models import MetaInfo
from agrobr.utils import result

from . import acquisition


def build_meta(acquired: acquisition.DesmatamentoAcquisition, frame: pd.DataFrame) -> MetaInfo:
    details = acquired.model_dump(mode="json")
    manifest_fields = ("query", "resources", "pages")
    manifest = json.dumps(
        {name: details[name] for name in manifest_fields},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    details.update(
        raw_content_hash_kind="resource_manifest_sha256",
        manifest_fields=list(manifest_fields),
        manifest_encoding="UTF-8 JSON, sorted keys, compact separators, no ASCII escaping",
        raw_content_size_kind="serialized_manifest_bytes",
        resource_bytes=sum(resource.size_bytes or 0 for resource in acquired.resources),
        revision_snapshot=False,
        area_semantics="published_feature_area_not_official_deforestation_rate",
        identifier_semantics="published_identifiers_are_not_unique_keys",
        output_dtypes={name: str(dtype) for name, dtype in frame.dtypes.items()},
        output_null_counts={name: int(count) for name, count in frame.isna().sum().items()},
    )
    if acquired.query.include_geometry:
        details["geometry"] = {
            "requested_crs": "EPSG:4326",
            "declared_crs_verified": bool(acquired.pages),
            "crs_basis": "verified_page_declarations"
            if acquired.pages
            else "requested_crs_for_empty_frame",
            "coordinate_transformation_by_sdk": False,
            "topology_repaired": False,
            "area_recalculated": False,
            "geodetic_accuracy_verified": False,
        }
    selected = f"terrabrasilis_{acquired.query.product.lower()}"
    if acquired.query.include_geometry:
        selected += "_geo"
    resource = next(
        (resource for resource in acquired.resources if resource.role == "page"),
        acquired.resources[-1],
    )
    meta = result.build_source_meta(
        "desmatamento",
        resource.url,
        "httpx+wfs2+geojson" if acquired.query.include_geometry else "httpx+wfs2+json",
        acquired.fetch_duration_ms,
        acquired.parse_duration_ms,
        frame,
        acquired.parser_version,
        schema_version="2.0",
        attempted_sources=[selected],
        selected_source=selected,
        raw_content_hash=hashlib.sha256(manifest).hexdigest(),
        source_details=details,
    )
    meta.raw_content_size = len(manifest)
    meta.contract_version = "2.0"
    meta.fetched_at = max(resource.fetched_at for resource in acquired.resources)
    meta.fetch_timestamp = meta.fetched_at
    meta.timestamp = datetime.now(UTC)
    meta.validation_warnings = list(acquired.warnings)
    return meta
