from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from agrobr.models import MetaInfo
from agrobr.utils import result

from . import ptax_acquisition, ptax_models, ptax_query


def _parsing(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "output_rows": len(frame),
        "null_counts": {column: int(frame[column].isna().sum()) for column in frame.columns},
        "dtypes": {column: str(frame[column].dtype) for column in frame.columns},
    }


def _build_meta(
    source: str,
    version: str,
    details: dict[str, Any],
    resources: list[ptax_acquisition.PtaxResource],
    frame: pd.DataFrame,
    *,
    source_url: str,
    fetch_ms: int,
    parse_ms: int,
) -> MetaInfo:
    details["resources"] = [resource.model_dump(mode="json") for resource in resources]
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
        resource_bytes=sum(resource.size_bytes for resource in resources),
        origin_index_base=0,
        origin_index_scope="page_index_within_resource_role",
        revision_snapshot=False,
        parsing=_parsing(frame),
    )
    meta = result.build_source_meta(
        source,
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        frame,
        ptax_models.PARSER_VERSION,
        schema_version=version,
        attempted_sources=[source],
        selected_source=source,
        raw_content_hash=hashlib.sha256(raw).hexdigest(),
        source_details=details,
    )
    meta.raw_content_size = len(raw)
    meta.contract_version = version
    meta.fetched_at = max(resource.fetched_at for resource in resources)
    meta.fetch_timestamp = meta.fetched_at
    meta.timestamp = datetime.now(UTC)
    meta.validation_warnings = list(details["warnings"])
    return meta


def build_quote_meta(
    acquired: ptax_acquisition.PtaxAcquisition,
    frame: pd.DataFrame,
    catalog_frame: pd.DataFrame,
    *,
    fetch_ms: int,
    parse_ms: int,
) -> MetaInfo:
    details = acquired.model_dump(
        mode="json", exclude={"records": True, "catalog": {"records", "resources"}}
    )
    currency = next(
        record for record in acquired.catalog.records if record.moeda == acquired.query.moeda
    )
    parity_unit = {
        "A": f"{currency.moeda}/USD",
        "B": f"USD/{currency.moeda}",
    }.get(currency.tipo_moeda)
    details["catalog"].update(
        selected_currency=currency.model_dump(mode="json"),
        schema_version="1.0",
        parsing=_parsing(catalog_frame),
        scope="current_OData_Moedas; not_historical_validity",
    )
    details.update(
        units={
            "cotacao_compra": "domestic_currency_at_reference_date_per_unit_of_selected_currency",
            "cotacao_venda": "domestic_currency_at_reference_date_per_unit_of_selected_currency",
            "paridade_compra": parity_unit,
            "paridade_venda": parity_unit,
            "converted": False,
        },
        published_clock={
            "semantics": "published_naive_timestamp",
            "timezone": None,
            "precision": "nanoseconds",
            "civil_date": "data_hora.normalize()",
            "distinct_from_acquisition_time": True,
        },
        bulletin_selection={
            "requested": acquired.query.boletim,
            "method": "local_filter_after_full_page_validation",
            "published_labels_preserved": True,
            "cross_route_labels_may_differ": True,
        },
    )
    return _build_meta(
        "bcb_ptax",
        "2.0",
        details,
        [*acquired.catalog.resources, *acquired.resources],
        frame,
        source_url=ptax_query.build_query_url(acquired.query),
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
    )


def build_catalog_meta(
    acquired: ptax_acquisition.PtaxCatalogAcquisition,
    frame: pd.DataFrame,
    *,
    fetch_ms: int,
    parse_ms: int,
) -> MetaInfo:
    details = acquired.model_dump(mode="json", exclude={"records"})
    details["catalog_scope"] = "current_OData_Moedas; not_a_historical_currency_inventory"
    return _build_meta(
        "bcb_ptax_moedas",
        "1.0",
        details,
        acquired.resources,
        frame,
        source_url=ptax_query.build_query_url(acquired.query),
        fetch_ms=fetch_ms,
        parse_ms=parse_ms,
    )
