from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pandas as pd

from agrobr.models import MetaInfo
from agrobr.utils import result

from . import acquisition


def build_meta(acquired: acquisition.SolosAcquisition, frame: pd.DataFrame) -> MetaInfo:
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
        raw_content_size_kind="serialized_manifest_bytes",
        manifest_fields=list(manifest_fields),
        manifest_encoding="UTF-8 JSON, sorted keys, compact separators, no ASCII escaping",
        resource_bytes=sum(
            resource.size_bytes or 0 for resource in acquired.resources if resource.complete_body
        ),
        resource_bytes_basis="complete_decoded_response_bodies_including_retries",
        revision_snapshot=False,
        identifier_semantics="published_identifiers_are_not_unique_keys",
        text_semantics="literal_published_text_without_laboratory_unit_or_date_inference",
        coordinate_semantics="published_coordinates_may_be_assigned_to_municipality",
        area_semantics="published_attribute_without_recalculation",
        output_dtypes={name: str(dtype) for name, dtype in frame.dtypes.items()},
        output_null_counts={name: int(count) for name, count in frame.isna().sum().items()},
    )
    if acquired.query.fetch_geometry:
        observed = [
            page.crs for page in acquired.pages if page.received_rows and page.crs is not None
        ]
        details["geometry"] = {
            "requested_crs": acquired.query.fetch_crs,
            "acquired_for_bbox_filter": acquired.query.bbox is not None,
            "included_in_output": acquired.query.include_geometry,
            "declared_crs_verified": bool(observed),
            "observed_declarations": observed,
            "crs_basis": "verified_nonempty_page_declarations"
            if observed
            else "requested_crs_for_empty_frame",
            "coordinate_transformation_by_sdk": False,
            "topology_repaired": False,
            "area_recalculated": False,
            "geodetic_accuracy_verified": False,
        }
    if acquired.query.bbox is not None:
        details["spatial_selection"] = {
            "bbox": list(acquired.query.bbox),
            "crs": acquired.query.bbox_crs,
            "remote_population": "bbox_candidates",
            "local_predicate": "Intersects",
            "local_filters": ["Intersects"]
            + (["uf"] if acquired.query.uf is not None else [])
            + (["ordem1"] if acquired.query.ordem is not None else []),
            "null_geometry_policy": "error",
            "server_configuration_inferred": False,
            "geometry_repaired": False,
            "geodetic_accuracy_verified": False,
        }
    selected = "embrapa_geoinfo_geo" if acquired.query.include_geometry else "embrapa_geoinfo"
    resource = next(
        (
            resource
            for resource in acquired.resources
            if resource.role == "page" and resource.complete_body
        ),
        acquired.resources[-1],
    )
    meta = result.build_source_meta(
        "embrapa_solos",
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
    meta.fetched_at = max(resource.fetched_at for resource in acquired.resources).astimezone(UTC)
    meta.fetch_timestamp = meta.fetched_at
    meta.timestamp = datetime.now(UTC)
    meta.validation_warnings = list(acquired.warnings)
    return meta
