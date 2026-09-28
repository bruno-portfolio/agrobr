from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from agrobr.contracts import comtrade as source_contracts
from agrobr.models import MetaInfo
from agrobr.utils import result

from . import acquisition, parser


def _manifest(details: dict[str, Any]) -> bytes:
    return json.dumps(
        {"query": details["query"], "resources": details["resources"]},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _build(
    frame: pd.DataFrame,
    details: dict[str, Any],
    *,
    source: str,
    source_url: str,
    attempted_sources: list[str],
    selected_source: str,
    fetched_at: datetime,
    version: str,
    fetch_ms: int,
    parse_ms: int,
) -> MetaInfo:
    manifest = _manifest(details)
    details["hash_kind"] = "resource_manifest_sha256"
    details["manifest_encoding"] = "canonical_json_utf8"
    details["manifest_fields"] = ["query", "resources"]
    details["license"] = {
        "classification": "zona_cinza",
        "terms_url": "https://uncomtrade.org/docs/policy-on-use-and-re-dissemination/",
    }
    details["resource_bytes"] = sum(resource["size_bytes"] for resource in details["resources"])
    meta = result.build_source_meta(
        source,
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        frame,
        parser.PARSER_VERSION,
        schema_version=version,
        attempted_sources=attempted_sources,
        selected_source=selected_source,
        raw_content_hash=hashlib.sha256(manifest).hexdigest(),
        source_details=details,
    )
    meta.raw_content_size = len(manifest)
    meta.contract_version = version
    meta.fetched_at = fetched_at
    meta.fetch_timestamp = meta.fetched_at
    meta.timestamp = datetime.now(UTC)
    meta.validation_warnings = list(details["warnings"])
    return meta


def trade_meta(
    acquired: acquisition.TradeAcquisition,
    frame: pd.DataFrame,
    *,
    fetch_ms: int,
    parse_ms: int,
) -> MetaInfo:
    details = acquired.model_dump(mode="json", exclude={"records"})
    details["query"]["partner_parameter_omitted"] = acquired.query.partner_parameter_omitted
    details["parsing"] = parser.parse_details(acquired.records, frame)
    estimados = frame.loc[frame["peso_liquido_estimado"].fillna(False), ["periodo", "hs_code"]]
    details["peso_liquido_estimado"] = estimados.to_dict("records")
    if len(estimados):
        celulas = ", ".join(
            f"HS {hs}/{periodo}" for periodo, hs in estimados.itertuples(index=False)
        )
        details["warnings"] = [
            *details["warnings"],
            f"Comtrade: peso líquido estimado pela ONU (isNetWgtEstimated) em {celulas}; "
            "peso_liquido_kg e volume_ton trazem o valor estimado, não o declarado.",
        ]
    accepted = [resource for resource in acquired.resources if resource.accepted]
    data = [resource for resource in accepted if resource.role == "data"]
    return _build(
        frame,
        details,
        source="comtrade",
        source_url=data[-1].url,
        attempted_sources=list(acquired.attempted_sources),
        selected_source=acquired.selected_source,
        fetched_at=max(resource.fetched_at for resource in accepted),
        version=source_contracts.COMERCIO_BILATERAL_V2.version,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
    )


def mirror_meta(
    reporter_meta: MetaInfo,
    partner_meta: MetaInfo,
    frame: pd.DataFrame,
    *,
    fetch_ms: int,
    parse_ms: int,
) -> MetaInfo:
    legs = {"reporter": reporter_meta, "partner": partner_meta}
    states = [meta.source_details["coverage"]["state"] for meta in legs.values()]
    state = "partial" if "partial" in states else "unknown" if "unknown" in states else "complete"
    attempted = list(
        dict.fromkeys(source for meta in legs.values() for source in meta.attempted_sources)
    )
    selected = list(dict.fromkeys(meta.selected_source for meta in legs.values()))
    details = {
        "query": {name: meta.source_details["query"] for name, meta in legs.items()},
        "resources": [
            {**resource, "leg": name}
            for name, meta in legs.items()
            for resource in meta.source_details["resources"]
        ],
        "coverage": {
            "state": state,
            "basis": "both_legs_independent_count_and_partition_coverage",
            "received_count": len(frame),
            "legs": {name: meta.source_details["coverage"] for name, meta in legs.items()},
        },
        "legs": {name: meta.to_dict() for name, meta in legs.items()},
        "peso_estimado": {
            name: meta.source_details.get("peso_liquido_estimado", [])
            for name, meta in legs.items()
        },
        "warnings": list(
            dict.fromkeys(warning for meta in legs.values() for warning in meta.validation_warnings)
        ),
        "parsing": {
            "join": "outer_one_to_one_periodo_hs_code",
            "hs_harmonization": False,
            "output_rows": len(frame),
            "null_counts": {column: int(frame[column].isna().sum()) for column in frame.columns},
        },
    }
    return _build(
        frame,
        details,
        source="comtrade_mirror",
        source_url=reporter_meta.source_url,
        attempted_sources=attempted,
        selected_source=selected[0] if len(selected) == 1 else "comtrade_mixed",
        fetched_at=max(meta.fetched_at for meta in legs.values()),
        version=source_contracts.TRADE_MIRROR_V2.version,
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
    )
