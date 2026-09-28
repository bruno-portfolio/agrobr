from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any, Literal
from xml.etree import ElementTree as ET

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from agrobr.exceptions import ParseError

from .query import DesmatamentoQuery


class DesmatamentoResource(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    role: Literal["hits_before", "page", "hits_after"]
    logical_index: int
    attempt_index: int
    requested_url: str
    url: str
    parameters: dict[str, str]
    status: int | None
    fetched_at: datetime
    size_bytes: int | None
    sha256: str | None
    headers: dict[str, str]
    complete_body: bool = False
    error_type: str | None = None
    from_cache: bool = False

    @field_validator("fetched_at")
    @classmethod
    def aware_utc(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("Aquisição exige UTC aware")
        return value.astimezone(UTC)


class AcceptedPage(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    index: int
    resource_index: int
    requested_offset: int
    requested_count: int
    received_rows: int
    overlap_rows: int
    accepted_start: int
    accepted_rows: int
    reported_total: int
    layout_fingerprint: dict[str, Any] | None
    sequence_sha256: str
    next_link: str | None = None
    crs: dict[str, Any] | None = None
    bbox: list[Any] | None = None


class DesmatamentoCoverage(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    expected_rows: int
    received_rows_with_overlap: int
    validated_rows_with_overlap: int
    overlap_rows: int
    accepted_rows: int
    returned_rows: int
    local_limit: int | None
    truncated: bool
    count_reconciled: bool
    all_logical_requests_succeeded: bool = True
    status: Literal["reconciled", "partial"]
    reconciliation_method: str = "hits_before_after_and_one_occurrence_overlap"
    transactional: bool = False
    semantic_progress_proven: bool = False
    ambiguities: list[dict[str, Any]] = Field(default_factory=list)


class DesmatamentoAcquisition(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    query: DesmatamentoQuery
    frame: pd.DataFrame = Field(exclude=True, repr=False)
    geometries: list[dict[str, Any] | None] | None = Field(default=None, exclude=True, repr=False)
    resources: list[DesmatamentoResource]
    pages: list[AcceptedPage]
    coverage: DesmatamentoCoverage
    details: dict[str, Any]
    warnings: list[str]
    fetch_duration_ms: int
    parse_duration_ms: int
    parser_version: int = 2


class _Hits(BaseModel):
    model_config = ConfigDict(strict=True, extra="allow")

    type: Literal["FeatureCollection"]
    features: list[Any]
    numberMatched: int = Field(ge=0)
    numberReturned: int = Field(ge=0)


class _NoDTD(ET.TreeBuilder):
    def doctype(self, _name: str, _pubid: str | None, _system: str | None) -> None:
        raise ValueError("DTD não é permitido em hits WFS")


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in values:
        if key in result:
            raise ValueError("Chave duplicada no JSON hits")
        result[key] = value
    return result


def _constant(value: str) -> Any:
    raise ValueError(f"Número JSON inválido: {value}")


def parse_hits(content: bytes) -> int:
    try:
        if content.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"{"):
            data = json.loads(content, object_pairs_hook=_pairs, parse_constant=_constant)
            parsed = _Hits.model_validate(data)
            if parsed.features or parsed.numberReturned != 0:
                raise ValueError("Hits contém observações")
            total = data.get("totalFeatures", parsed.numberMatched)
            if total != "unknown" and (type(total) is not int or total != parsed.numberMatched):
                raise ValueError("Contagens hits contraditórias")
            return parsed.numberMatched
        root = ET.fromstring(content, parser=ET.XMLParser(target=_NoDTD()))
        if root.tag != "{http://www.opengis.net/wfs/2.0}FeatureCollection" or len(root):
            raise ValueError("Envelope hits WFS2 inválido")
        text = root.get("numberMatched", "")
        if re.fullmatch(r"[0-9]+", text) is None or root.get("numberReturned") != "0":
            raise ValueError("Hits sem contagem válida")
        return int(text)
    except (ValueError, TypeError, ValidationError, ET.ParseError) as exc:
        raise ParseError(
            source="desmatamento", parser_version=2, reason=f"Hits inválido: {exc}"
        ) from exc
