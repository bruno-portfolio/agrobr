from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any, Literal

import pydantic

from agrobr import constants

from . import models


class TradeQuery(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid")

    reporter: int = pydantic.Field(gt=0)
    partner: int | None = pydantic.Field(ge=0)
    hs_codes: list[str] = pydantic.Field(min_length=1)
    flow: Literal["X", "M"]
    freq: Literal["A", "M"]
    periods: list[str] = pydantic.Field(min_length=1)
    requested_period: str
    type_code: Literal["C"] = "C"
    classification: Literal["HS"] = "HS"
    partner2_code: Literal["0"] = "0"
    mot_code: Literal["0"] = "0"
    customs_code: Literal["C00"] = "C00"

    @property
    def partner_parameter_omitted(self) -> bool:
        return self.partner is None

    @pydantic.model_validator(mode="after")
    def valid_selection(self) -> TradeQuery:
        if len(self.hs_codes) != len(set(self.hs_codes)) or any(
            re.fullmatch(constants.COMTRADE_HS_PATTERN, code) is None for code in self.hs_codes
        ):
            raise ValueError("HS inválido ou duplicado")
        if len(self.periods) != len(set(self.periods)):
            raise ValueError("Períodos duplicados")
        for period in self.periods:
            pattern = r"[0-9]{4}" if self.freq == "A" else r"[0-9]{6}"
            if re.fullmatch(pattern, period) is None:
                raise ValueError("Período incompatível com frequência")
            datetime(int(period[:4]), int(period[4:]) if self.freq == "M" else 1, 1)
        return self


class TradePartition(pydantic.BaseModel):
    partition_id: str
    parent_id: str | None = None
    periods: list[str]
    hs_codes: list[str]


class TradeEnvelope(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    count: int = pydantic.Field(ge=0)
    data: list[models.TradeRecord]
    error: str

    @pydantic.model_validator(mode="after")
    def valid_response(self) -> TradeEnvelope:
        if self.error or self.count != len(self.data):
            raise ValueError("Envelope com erro ou contagem retornada divergente")
        return self


class TradeCountResult(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    count: int = pydantic.Field(ge=0)
    data: dict[str, Any]
    error: str

    @pydantic.field_validator("error")
    @classmethod
    def no_error(cls, value: str) -> str:
        if value:
            raise ValueError("Envelope de contagem declara erro")
        return value


class TradeResource(pydantic.BaseModel):
    partition_id: str
    role: Literal["data", "count"]
    access: Literal["guest", "authenticated"]
    requested_url: str
    url: str
    parameters: dict[str, str]
    fetched_at: datetime
    sha256: str
    size_bytes: int
    status_code: int
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    requested_limit: int
    effective_limit: int | None
    limit_basis: str = "documented_preview_limit"
    received_count: int | None = None
    reported_count: int | None = None
    accepted: bool = False
    replaced_by: list[str] = pydantic.Field(default_factory=list)

    @pydantic.field_validator("fetched_at")
    @classmethod
    def aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("Aquisição sem fuso")
        return value.astimezone(UTC)


class PartitionCoverage(pydantic.BaseModel):
    partition_id: str
    state: Literal["complete", "partial", "unknown"]
    reason: str
    received_count: int
    expected_count: int | None
    limit: int | None
    saturated: bool
    leaf_partition_ids: list[str]


class TradeCoverage(pydantic.BaseModel):
    state: Literal["complete", "partial", "unknown"]
    basis: str
    initial_partitions: int
    completed_partitions: int
    received_count: int
    expected_count: int | None
    partitions: list[PartitionCoverage]


class TradeAcquisition(pydantic.BaseModel):
    query: TradeQuery
    records: list[models.TradeRecord]
    resources: list[TradeResource]
    coverage: TradeCoverage
    attempted_sources: list[str]
    selected_source: str
    warnings: list[str] = pydantic.Field(default_factory=list)
    fallback: dict[str, Any] | None = None
