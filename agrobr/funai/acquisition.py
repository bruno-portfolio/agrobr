from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from . import query


class FunaiResource(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    role: Literal["count_before", "page", "count_after"]
    logical_index: int = Field(ge=0)
    attempt_index: int = Field(ge=0)
    requested_url: str
    url: str
    parameters: dict[str, str]
    status: int | None
    started_at: datetime
    fetched_at: datetime
    finished_at: datetime | None = None
    size_bytes: int | None = Field(default=None, ge=0)
    sha256: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    complete_body: bool = False
    error_type: str | None = None
    from_cache: Literal[False] = False

    @field_validator("started_at", "fetched_at", "finished_at")
    @classmethod
    def aware_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.utcoffset() is None:
            raise ValueError("Aquisição exige horário com fuso")
        return value.astimezone(UTC)


class FunaiPage(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    index: int = Field(ge=0)
    resource_index: int = Field(ge=0)
    requested_offset: int = Field(ge=0)
    requested_count: int = Field(ge=0)
    received_rows: int = Field(ge=0)
    validated_rows: int = Field(ge=0)
    overlap_rows: int = Field(ge=0, le=1)
    accepted_start: int = Field(ge=0)
    accepted_rows: int = Field(ge=0)
    local_matched_rows: int = Field(ge=0)
    local_rejected_rows: int = Field(ge=0)
    selected_positions: list[int]
    reported_total: int = Field(ge=0)
    sequence_sha256: str
    layout_fingerprint: dict[str, Any] | None
    next_link: str | None = None
    source_timestamp: str | None = None
    crs: dict[str, Any] | None = None
    bbox: list[Any] | None = None

    @model_validator(mode="after")
    def counts(self) -> FunaiPage:
        if (
            self.validated_rows != self.received_rows
            or self.accepted_rows + self.overlap_rows != self.received_rows
            or self.local_matched_rows + self.local_rejected_rows != self.accepted_rows
            or len(self.selected_positions) != self.local_matched_rows
            or self.selected_positions != sorted(set(self.selected_positions))
            or any(pos < 0 or pos >= self.accepted_rows for pos in self.selected_positions)
        ):
            raise ValueError("Contagens/posições da página inconsistentes")
        return self


class RemoteCoverage(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    expected_before: int = Field(ge=0)
    expected_after: int = Field(ge=0)
    received_rows_with_overlap: int = Field(ge=0)
    validated_rows_with_overlap: int = Field(ge=0)
    overlap_rows: int = Field(ge=0)
    accepted_rows: int = Field(ge=0)
    max_records: int | None = Field(ge=1)
    truncated: bool
    counts_consistent: bool
    status: Literal["reconciled", "partial"]


class LocalCoverage(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    evaluated_rows: int = Field(ge=0)
    matched_rows: int = Field(ge=0)
    rejected_rows: int = Field(ge=0)
    returned_rows: int = Field(ge=0)
    status: Literal["reconciled", "unknown"]
    total_basis: Literal["reconciled_remote_scan", "observed_remote_prefix"]


class FunaiCoverage(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    remote: RemoteCoverage
    local: LocalCoverage
    all_logical_requests_succeeded: Literal[True] = True
    transactional: Literal[False] = False
    semantic_progress_proven: Literal[False] = False
    ambiguity_count: int = Field(default=0, ge=0)
    ambiguities: list[dict[str, Any]] = Field(default_factory=list)
    ambiguity_examples_omitted: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def reconciled_counts(self) -> FunaiCoverage:
        remote, local = self.remote, self.local
        if (
            remote.expected_before != remote.expected_after
            or not remote.counts_consistent
            or remote.accepted_rows > remote.expected_before
            or remote.validated_rows_with_overlap != remote.received_rows_with_overlap
            or remote.accepted_rows + remote.overlap_rows != remote.received_rows_with_overlap
            or remote.truncated != (remote.accepted_rows < remote.expected_before)
            or remote.status != ("partial" if remote.truncated else "reconciled")
            or local.evaluated_rows != remote.accepted_rows
            or local.matched_rows + local.rejected_rows != local.evaluated_rows
            or local.returned_rows != local.matched_rows
            or local.status != ("unknown" if remote.truncated else "reconciled")
            or local.total_basis
            != ("observed_remote_prefix" if remote.truncated else "reconciled_remote_scan")
            or len(self.ambiguities) + self.ambiguity_examples_omitted != self.ambiguity_count
        ):
            raise ValueError("Cobertura remota/local inconsistente")
        return self


class FunaiCountCheck(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    role: Literal["count_before", "count_after"]
    resource_index: int = Field(ge=0)
    reported_total: int = Field(ge=0)
    received_rows: int = Field(ge=0, le=1)
    validated_rows: int = Field(ge=0, le=1)
    source_timestamp: str | None
    layout_fingerprint: dict[str, Any]

    @model_validator(mode="after")
    def counts(self) -> FunaiCountCheck:
        if (
            self.received_rows != min(1, self.reported_total)
            or self.validated_rows != self.received_rows
        ):
            raise ValueError("Probe de contagem exige zero ou uma ocorrência validada")
        return self


class FunaiAcquisition(BaseModel):
    model_config = ConfigDict(strict=True, arbitrary_types_allowed=True, extra="forbid")

    query: query.FunaiQuery
    frame: pd.DataFrame = Field(exclude=True, repr=False)
    geometries: list[dict[str, Any] | None] | None = Field(default=None, exclude=True, repr=False)
    resources: list[FunaiResource]
    pages: list[FunaiPage]
    count_checks: list[FunaiCountCheck]
    coverage: FunaiCoverage
    local_filters: dict[str, Any]
    diagnostics: dict[str, Any]
    details: dict[str, Any]
    warnings: list[str]
    fetch_duration_ms: int = Field(ge=0)
    parse_duration_ms: int = Field(ge=0)
    parser_version: Literal[3] = 3

    @model_validator(mode="after")
    def count_provenance(self) -> FunaiAcquisition:
        if [check.role for check in self.count_checks] != ["count_before", "count_after"]:
            raise ValueError("Aquisição exige probes antes e depois")
        totals = (self.coverage.remote.expected_before, self.coverage.remote.expected_after)
        for check, total in zip(self.count_checks, totals, strict=True):
            if check.reported_total != total or check.resource_index >= len(self.resources):
                raise ValueError("Probe contradiz cobertura ou recurso")
            resource = self.resources[check.resource_index]
            if (
                resource.role != check.role
                or not resource.complete_body
                or resource.error_type is not None
                or resource.status != 200
            ):
                raise ValueError("Probe não referencia resposta concluída com sucesso")
        before, after = (check.resource_index for check in self.count_checks)
        if before >= after or any(
            not before < page.resource_index < after
            or self.resources[page.resource_index].role != "page"
            for page in self.pages
        ):
            raise ValueError("Ordem de recursos/probes/páginas inconsistente")
        return self
