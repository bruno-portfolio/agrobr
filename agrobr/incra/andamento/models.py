from __future__ import annotations

from datetime import date
from typing import Annotated, Any, Literal

import pandas as pd
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    model_validator,
)


class AndamentoRow(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    regional: Annotated[str, Field(min_length=1)]
    numero_publicado: Annotated[int, Field(gt=0)]
    processo: str
    comunidade: str
    municipio: str
    area_ha_texto: str
    familias_texto: str
    edital_rtid_1: str
    edital_rtid_2: str
    retificacao_edital_1: str
    retificacao_edital_2: str
    portaria: str
    retificacao_portaria: str
    decreto: str
    titulo: str


class LinkedEdition(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    file_date: date
    href: str
    url: str
    label: str


class Resource(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    role: Literal["publisher", "pdf"]
    url: str
    attempt: Annotated[int, Field(ge=1)]
    requested_at: AwareDatetime
    finished_at: AwareDatetime | None = None
    status: StrictInt | None = None
    request_headers: dict[str, str] = Field(default_factory=dict)
    response_headers: dict[str, str] = Field(default_factory=dict)
    size_bytes: Annotated[int, Field(ge=0)] = 0
    sha256: str | None = None
    complete_body: bool = False
    error_type: str | None = None
    error: str | None = None
    close_error: str | None = None


class Acquisition(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    content: bytes = Field(exclude=True, repr=False)
    linked_edition: LinkedEdition
    page_url: str
    resources: list[Resource]
    duration_ms: int


class OrdinalCell(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    ordinal: Annotated[int, Field(gt=0)]
    page: Annotated[int, Field(gt=0)]
    top: Annotated[float, Field(allow_inf_nan=False)]
    bottom: Annotated[float, Field(allow_inf_nan=False)]

    @model_validator(mode="after")
    def ordered_bounds(self) -> OrdinalCell:
        if self.bottom <= self.top:
            raise ValueError("Caixa ordinal sem altura positiva")
        return self


class RegionalGroup(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    first: int
    last: int
    label: Annotated[StrictStr, Field(min_length=1)]
    pages: list[int]
    label_page: int
    label_text_object: int
    closing_paths: list[dict[str, Any]]


class ParsedPublication(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    frame: pd.DataFrame = Field(exclude=True, repr=False)
    edition: date
    declared_total: int
    page_count: int
    regional_groups: list[RegionalGroup]
    notes: list[dict[str, Any]]
    fingerprint: dict[str, Any]
    diagnostics: dict[str, Any]
    source_sha256: str


def columns() -> tuple[str, ...]:
    return tuple(AndamentoRow.model_fields)
