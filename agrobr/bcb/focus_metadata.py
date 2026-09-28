from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pandas as pd

from agrobr.models import MetaInfo
from agrobr.utils import result

from . import focus_acquisition, focus_models, focus_query


def build_meta(
    acquired: focus_acquisition.FocusAcquisition,
    frame: pd.DataFrame,
    *,
    fetch_ms: int,
    parse_ms: int,
) -> MetaInfo:
    details = acquired.model_dump(mode="json", exclude={"records"})
    raw = json.dumps(
        {"query": details["query"], "resources": details["resources"]},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    details.update(
        hash_kind="resource_manifest_sha256",
        manifest_encoding="canonical_json_utf8",
        manifest_fields=["query", "resources"],
        resource_bytes=sum(resource.size_bytes for resource in acquired.resources),
        origin_index_base=0,
        survey_date_semantics="civil_date_of_survey; distinct_from_acquisition_time",
        reference_semantics="published_forecast_horizon; not_an_observed_value",
        revision_snapshot=False,
        parsing={
            "output_rows": len(frame),
            "null_counts": {column: int(frame[column].isna().sum()) for column in frame.columns},
            "dtypes": {column: str(frame[column].dtype) for column in frame.columns},
        },
    )
    meta = result.build_source_meta(
        "bcb_focus",
        focus_query.build_query_url(acquired.query),
        "httpx",
        fetch_ms,
        parse_ms,
        frame,
        focus_models.PARSER_VERSION,
        schema_version="2.0",
        attempted_sources=["bcb_focus"],
        selected_source="bcb_focus",
        raw_content_hash=hashlib.sha256(raw).hexdigest(),
        source_details=details,
    )
    meta.raw_content_size = len(raw)
    meta.contract_version = "2.0"
    meta.fetched_at = max(resource.fetched_at for resource in acquired.resources)
    meta.fetch_timestamp = meta.fetched_at
    meta.timestamp = datetime.now(UTC)
    meta.validation_warnings = list(acquired.warnings)
    return meta
