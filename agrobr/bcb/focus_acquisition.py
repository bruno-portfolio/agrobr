from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Literal

import pydantic

from agrobr import constants

from . import focus_models


class FocusQuery(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid")

    indicador: str = pydantic.Field(min_length=1)
    periodicidade: Literal["anual", "mensal"]
    entity: str
    data_inicial: date | None
    top: int = pydantic.Field(gt=0)
    max_registros: int | None = pydantic.Field(gt=0)
    filter: str
    order_by: str

    @pydantic.model_validator(mode="after")
    def valid_selection(self) -> FocusQuery:
        if not self.indicador.strip():
            raise ValueError("Indicador vazio")
        if self.entity != constants.BCB_FOCUS_ENTITIES[self.periodicidade]:
            raise ValueError("Entidade incompatível com periodicidade")
        if self.order_by != constants.BCB_FOCUS_ORDER_BY[self.periodicidade]:
            raise ValueError("Ordenação incompatível com periodicidade")
        return self


class FocusResource(pydantic.BaseModel):
    page_index: int
    index_base: Literal[0] = 0
    requested_url: str
    url: str
    parameters: dict[str, str]
    offset: int
    top: int
    sha256: str
    size_bytes: int
    fetched_at: datetime
    status_code: int
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    received_count: int
    retained_count: int
    reported_count: int | None
    next_link: str | None
    layout_fingerprint: str | None
    parser_version: int

    @pydantic.field_validator("fetched_at")
    @classmethod
    def utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Aquisição Focus exige instante com fuso")
        return value.astimezone(UTC)


class FocusCoverage(pydantic.BaseModel):
    request_status: Literal["all_requested_pages_succeeded"] = "all_requested_pages_succeeded"
    completeness: Literal["complete", "partial", "unknown"]
    basis: str
    expected_count: int | None
    received_count: int
    returned_count: int
    discarded_by_local_limit: int
    pages_fetched: int
    page_size_requested: int
    local_limit: int | None
    local_limit_reached: bool
    stop_reason: Literal["source_count", "local_limit", "terminal_empty_page"]
    terminal_empty_page: bool
    next_link_remaining: str | None
    revision_snapshot: Literal[False] = False


class FocusAcquisition(pydantic.BaseModel):
    query: FocusQuery
    records: list[focus_models.FocusObservation]
    resources: list[FocusResource]
    coverage: FocusCoverage
    warnings: list[str]
