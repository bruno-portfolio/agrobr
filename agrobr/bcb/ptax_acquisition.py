from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Literal

import pydantic

from . import ptax_models


class PtaxQuery(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid")

    requested_moeda: str
    moeda: str
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"]
    top: int = pydantic.Field(gt=0)
    mode: Literal["dia", "periodo"]
    data: date | None
    data_inicial: date | None
    data_final: date | None
    inicio: date
    fim: date
    reference_date: date
    defaulted_fields: list[str]

    @pydantic.model_validator(mode="after")
    def valid_interval(self) -> PtaxQuery:
        if self.inicio > self.fim:
            raise ValueError("Intervalo PTAX invertido")
        if self.mode == "dia" and self.inicio != self.fim:
            raise ValueError("Consulta diária PTAX exige uma única data")
        return self


class PtaxCatalogQuery(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid")

    top: int = pydantic.Field(gt=0)


class PtaxResource(pydantic.BaseModel):
    role: Literal["catalog", "quotes"]
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
            raise ValueError("Aquisição PTAX exige instante com fuso")
        return value.astimezone(UTC)


class PtaxCoverage(pydantic.BaseModel):
    request_status: Literal["all_requested_pages_succeeded"] = "all_requested_pages_succeeded"
    completeness: Literal["complete", "unknown"]
    basis: str
    expected_count: int | None
    received_count: int
    returned_count: int
    filtered_by_boletim_count: int
    pages_fetched: int
    page_size_requested: int
    stop_reason: Literal["source_count", "terminal_empty_page"]
    terminal_empty_page: bool
    revision_snapshot: Literal[False] = False


class PtaxCatalogAcquisition(pydantic.BaseModel):
    query: PtaxCatalogQuery
    records: list[ptax_models.PtaxCurrency]
    resources: list[PtaxResource]
    coverage: PtaxCoverage
    warnings: list[str]


class PtaxAcquisition(pydantic.BaseModel):
    query: PtaxQuery
    records: list[ptax_models.PtaxObservation]
    catalog: PtaxCatalogAcquisition
    resources: list[PtaxResource]
    coverage: PtaxCoverage
    warnings: list[str]
