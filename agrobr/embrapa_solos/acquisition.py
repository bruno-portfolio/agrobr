from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any, Literal
from xml.etree import ElementTree as ET

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from agrobr.exceptions import ParseError
from agrobr.utils import json as json_utils

from . import query


class SolosResource(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    role: Literal["hits_before", "page", "hits_after"]
    logical_index: int = Field(ge=0)
    attempt_index: int = Field(ge=0)
    requested_url: str
    url: str
    parameters: dict[str, str]
    status: int | None
    fetched_at: datetime
    size_bytes: int | None = Field(default=None, ge=0)
    sha256: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    complete_body: bool = False
    error_type: str | None = None
    from_cache: Literal[False] = False

    @field_validator("fetched_at")
    @classmethod
    def aware_utc(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("Aquisição exige horário com fuso")
        return value.astimezone(UTC)


class SolosPage(BaseModel):
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
    crs: dict[str, Any] | None = None
    bbox: list[Any] | None = None

    @model_validator(mode="after")
    def counts(self) -> SolosPage:
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


class SolosCoverage(BaseModel):
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
    def reconciled_counts(self) -> SolosCoverage:
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


class SolosAcquisition(BaseModel):
    model_config = ConfigDict(strict=True, arbitrary_types_allowed=True, extra="forbid")

    query: query.SolosQuery
    frame: pd.DataFrame = Field(exclude=True, repr=False)
    geometries: list[dict[str, Any] | None] | None = Field(default=None, exclude=True, repr=False)
    resources: list[SolosResource]
    pages: list[SolosPage]
    coverage: SolosCoverage
    local_filters: dict[str, Any]
    diagnostics: dict[str, Any]
    details: dict[str, Any]
    warnings: list[str]
    fetch_duration_ms: int = Field(ge=0)
    parse_duration_ms: int = Field(ge=0)
    parser_version: Literal[3] = 3


class _Hits(BaseModel):
    model_config = ConfigDict(strict=True, extra="allow")

    type: Literal["FeatureCollection"]
    features: list[Any]
    numberMatched: int = Field(ge=0)
    numberReturned: int = Field(ge=0)


class _NoDTD(ET.TreeBuilder):
    def doctype(self, _name: str, _pubid: str | None, _system: str | None) -> None:
        raise ValueError("DTD não é permitido em hits WFS")


def parse_hits(content: bytes) -> int:
    try:
        if content.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"{"):
            data = json_utils.decode(content, encoding=None)
            if not isinstance(data, dict):
                raise ValueError("Envelope hits JSON inválido")
            for name in ("numberMatched", "numberReturned", "totalFeatures"):
                if name in data:
                    data[name] = json_utils.count(data[name], bits=None)
            parsed = _Hits.model_validate(data)
            if parsed.features or parsed.numberReturned != 0:
                raise ValueError("Hits contém observações")
            total = data.get("totalFeatures", parsed.numberMatched)
            if type(total) is not int or total != parsed.numberMatched:
                raise ValueError("Contagens hits contraditórias/desconhecidas")
            return parsed.numberMatched
        root = ET.fromstring(content, parser=ET.XMLParser(target=_NoDTD()))
        if root.tag != "{http://www.opengis.net/wfs/2.0}FeatureCollection" or len(root):
            raise ValueError("Envelope hits WFS2 inválido")
        matched = root.get("numberMatched", "")
        if re.fullmatch(r"0|[1-9][0-9]*", matched) is None or root.get("numberReturned") != "0":
            raise ValueError("Hits sem contagem inteira válida")
        return int(matched)
    except (ValueError, TypeError, ValidationError, ET.ParseError) as exc:
        raise ParseError(
            source="embrapa_solos", parser_version=3, reason=f"Hits inválido: {exc}"
        ) from exc
