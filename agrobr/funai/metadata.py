from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pandas as pd

from agrobr.models import MetaInfo
from agrobr.utils import result

from . import acquisition


def build_meta(acquired: acquisition.FunaiAcquisition, frame: pd.DataFrame) -> MetaInfo:
    details = acquired.model_dump(mode="json")
    manifest_fields = ("query", "resources", "pages", "count_checks")
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
        resource_bytes=sum(resource.size_bytes or 0 for resource in acquired.resources),
        resource_bytes_basis="observed_decoded_response_bytes_including_partial_bodies_and_retries",
        revision_snapshot=False,
        source_revision=None,
        source_revision_basis="no_immutable_edition_identifier_demonstrated",
        identifier_semantics="received_feature_id_is_volatile_published_codes_are_not_primary_keys",
        continuity_basis="all_published_properties_and_acquired_geometry_excluding_volatile_feature_id",
        text_semantics="literal_published_text_without_date_boolean_or_UF_coercion",
        update_date_semantics="published_text_not_http_timestamp_or_immutable_edition",
        area_semantics="published_hectares_without_recalculation",
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
            "published_epsg_attribute_defines_output_crs": False,
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
            + (["fase"] if acquired.query.fase is not None else []),
            "null_geometry_policy": "error",
            "empty_geometry_policy": "disjoint",
            "server_configuration_inferred": False,
            "geometry_repaired": False,
        }
    selected = "funai_geoserver_geo" if acquired.query.include_geometry else "funai_geoserver"
    resource = next(
        (
            resource
            for resource in acquired.resources
            if resource.role == "page" and resource.complete_body
        ),
        acquired.resources[-1],
    )
    meta = result.build_source_meta(
        "funai",
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
