from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.incra import acquisition
from agrobr.models import MetaInfo
from agrobr.utils import result

from . import budget, models


class AdministrativeCoverage(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    status: str
    declared_rows: int = Field(ge=0)
    validated_rows: int = Field(ge=0)
    returned_rows: int = Field(ge=0)
    all_logical_requests_succeeded: bool
    local_filters: list[str]

    @model_validator(mode="after")
    def complete(self) -> AdministrativeCoverage:
        if (
            self.status != "reconciled_publication"
            or not self.all_logical_requests_succeeded
            or self.local_filters
            or self.declared_rows != self.validated_rows
            or self.validated_rows != self.returned_rows
        ):
            raise ValueError("Publicação administrativa incompleta")
        return self


def parent_payload(meta: MetaInfo, frame: pd.DataFrame, *, geographical: bool) -> dict[str, Any]:
    payload = copy.deepcopy(meta.to_dict())
    expected = "incra_geoserver" if geographical else "incra_andamento_pdf"
    version = "2.0" if geographical else "1.0"
    if (
        meta.source != "incra"
        or meta.selected_source != expected
        or not meta.validation_passed
        or meta.records_count != len(frame)
        or meta.columns != frame.columns.tolist()
        or meta.schema_version != version
        or meta.contract_version != version
    ):
        raise ParseError("incra_vinculos", 1, "Identidade/metadados da fonte divergem do quadro")
    try:
        details = payload["source_details"]
        if meta.fetch_timestamp is None:
            raise ValueError("Relógio de aquisição da fonte ausente")
        fields = details["manifest_fields"]
        if type(fields) is not list or not fields or len(set(fields)) != len(fields):
            raise ValueError("Campos de manifesto inválidos")
        manifest = encode({name: details[name] for name in fields})
        if (
            hashlib.sha256(manifest).hexdigest() != meta.raw_content_hash
            or len(manifest) != meta.raw_content_size
        ):
            raise ValueError("Manifesto da fonte diverge")
        if geographical:
            coverage = acquisition.IncraCoverage.model_validate(details["coverage"])
            query = details["query"]
            if (
                coverage.remote.status != "reconciled"
                or coverage.local.returned_rows != len(frame)
                or coverage.remote.accepted_rows != len(frame)
                or coverage.remote.max_records is not None
                or coverage.local.rejected_rows != 0
                or any(query[name] is not None for name in ("uf", "fase", "bbox", "max_records"))
                or query["fetch_geometry"]
                or query["include_geometry"]
            ):
                raise ValueError("Aquisição geográfica não é integral e sem filtros")
        else:
            coverage_admin = AdministrativeCoverage.model_validate(details["coverage"])
            if (
                coverage_admin.returned_rows != len(frame)
                or details["publication"]["internal_edition"]
                != details["query"]["resolved_edition"]
            ):
                raise ValueError("Edição/cobertura administrativa diverge")
    except (KeyError, TypeError, ValueError, ValidationError) as error:
        raise ParseError("incra_vinculos", 1, f"Proveniência da fonte inválida: {error}") from error
    return payload


def encode(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def build_meta(
    parsed: models.RelationResult,
    parents: dict[str, dict[str, Any]],
    requested: dict[str, Any],
    elapsed_ms: int,
    relation_ms: int,
) -> MetaInfo:
    details: dict[str, Any] = {
        "query": requested,
        "sources": copy.deepcopy(parents),
        "relation": {
            "version": "1.0",
            "reference_pattern": constants.INCRA_VINCULOS_REFERENCE_PATTERN,
            "reference_checksum_validated": False,
            "punctuation_repaired": False,
            "matching": "literal_reference_occurrences_cartesian_product",
            "unrecognized_cells_linked": False,
            "deduplicated": False,
            "positions": "one_based_source_occurrences_scoped_to_each_parent_manifest",
            "semantic_primary_key": [],
        },
        "evidence": {
            name: [cell.model_dump(mode="json") for cell in cells]
            for name, cells in parsed.evidence.items()
        },
        "coverage": {
            "status": "reconciled_inputs_and_complete_relation",
            "transactional_snapshot": False,
            **parsed.counts,
        },
    }
    fields = list(details)
    manifest = encode(details)
    estimate = budget.check(
        parsed.retained_bytes_estimate + budget.retained_size(details) + len(manifest)
    )
    details.update(
        manifest_fields=fields,
        manifest_encoding="UTF-8 JSON sorted keys compact separators no ASCII escaping",
        raw_content_hash_kind="composite_parent_manifests_and_relation_evidence_sha256",
        raw_content_size_kind="serialized_composite_manifest_bytes",
        resource_bytes=sum(
            parent["source_details"]["resource_bytes"] for parent in parents.values()
        ),
        resource_bytes_basis="sum_parent_observed_decoded_resource_bytes_including_errors_partials_retries",
        revision_snapshot=False,
        budgets={
            "max_rows": requested["max_vinculos"],
            "max_retained_bytes": constants.INCRA_VINCULOS_MAX_RETAINED_BYTES,
            "retained_bytes_estimate": estimate,
            "basis": "SDK_local_composite_retention_estimate_not_peak_RSS_or_remote_limit",
        },
        output_dtypes={name: str(dtype) for name, dtype in parsed.frame.dtypes.items()},
        output_null_counts={name: int(value) for name, value in parsed.frame.isna().sum().items()},
        composition_duration_ms=elapsed_ms,
        timing_basis="fetch_duration_ms_includes_complete_parent_API_stages_and_parent_contract_checks; parse_duration_ms_is_local_relation_and_contract_stage",
    )
    meta = result.build_source_meta(
        "incra",
        constants.INCRA_ANDAMENTO_PAGE_URL,
        "composed+literal_document_references",
        max(0, elapsed_ms - relation_ms),
        relation_ms,
        parsed.frame,
        1,
        attempted_sources=["incra_geoserver", "incra_andamento_pdf"],
        selected_source="incra_geoserver+incra_andamento_pdf",
        raw_content_hash=hashlib.sha256(manifest).hexdigest(),
        source_details=details,
    )
    meta.raw_content_size = len(manifest)
    meta.contract_version = "1.0"
    meta.fetched_at = max(
        datetime.fromisoformat(parent["fetched_at"]) for parent in parents.values()
    )
    meta.timestamp = datetime.now(UTC)
    meta.fetch_timestamp = meta.fetched_at
    meta.validation_warnings = [
        "Referência documental comum não prova identidade territorial; não correspondência é limitada às duas aquisições, sem snapshot transacional."
    ]
    return meta
