from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Literal

import pydantic

from . import sgs_models


class SGSQuery(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid")

    codigo: int = pydantic.Field(gt=0, le=2**63 - 1)
    nome_serie: str | None
    mode: Literal["range", "latest", "server_start"]
    requested_start: date | None
    requested_end: date | None
    inicio: date | None
    fim: date | None
    ultimos: int | None = pydantic.Field(gt=0)
    reference_date: date
    defaulted_fields: list[str] = pydantic.Field(default_factory=list)

    @pydantic.model_validator(mode="after")
    def valid_selection(self) -> SGSQuery:
        if self.mode == "range":
            if self.inicio is None or self.fim is None or self.inicio > self.fim:
                raise ValueError("Intervalo SGS inválido")
        elif self.mode == "latest":
            if self.ultimos is None or self.inicio is not None or self.fim is not None:
                raise ValueError("Seleção de últimos SGS inválida")
        elif self.inicio is not None or self.fim is None:
            raise ValueError("Seleção de início implícito SGS inválida")
        return self


class SGSBlock(pydantic.BaseModel):
    id: str
    mode: Literal["range", "latest", "server_start"]
    inicio: date | None
    fim: date | None
    ultimos: int | None = None


class SGSReferenceDiagnostics(pydantic.BaseModel):
    before_count: int = 0
    before_min: date | None = None
    before_max: date | None = None
    after_count: int = 0
    after_min: date | None = None
    after_max: date | None = None


class SGSResource(pydantic.BaseModel):
    requested_url: str
    url: str
    parameters: dict[str, str]
    block: SGSBlock
    sha256: str
    size_bytes: int
    fetched_at: datetime
    status_code: int
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    received_count: int
    layout_fingerprint: str | None
    parser_version: int
    reference_diagnostics: SGSReferenceDiagnostics

    @pydantic.field_validator("fetched_at")
    @classmethod
    def utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Coleta SGS exige instante com fuso")
        return value.astimezone(UTC)


class SGSOrigin(pydantic.BaseModel):
    index_base: Literal[0] = 0
    block_id: str
    resource_index: int
    row_index: int


class SGSReconciliation(pydantic.BaseModel):
    data: date
    valor: float | None
    origins: list[SGSOrigin]


class SGSCoverage(pydantic.BaseModel):
    request_status: Literal["all_blocks_succeeded"] = "all_blocks_succeeded"
    completeness: Literal["unknown"] = "unknown"
    basis: Literal["no_source_total"] = "no_source_total"
    expected_count: None = None
    planned_blocks: int
    completed_blocks: int
    received_count: int
    unique_count: int
    reconciled_count: int
    returned_count: int
    empty_blocks: list[str]
    observed_start: date | None
    observed_end: date | None
    tail_applied: bool


class SGSAcquisition(pydantic.BaseModel):
    query: SGSQuery
    records: list[sgs_models.SGSObservation]
    resources: list[SGSResource]
    coverage: SGSCoverage
    reconciliation: list[SGSReconciliation]
    warnings: list[str]


class SGSNotFoundDetail(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    statusCode: Literal[404]
    detail: Literal["br.gov.bcb.pec.sgs.comum.excecoes.SGSNegocioException: Value(s) not found"]


class SGSNotFoundEnvelope(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    erro: SGSNotFoundDetail


class SGSSelectionError(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    error: str = pydantic.Field(min_length=1)
    message: str | None = None


class SGSLatestLimitDetail(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    statusCode: Literal[400]
    detail: Literal[
        "br.gov.bcb.pec.sgs.comum.excecoes.SGSNegocioException: "
        "A quantidade máxima de valores deve ser 20"
    ]


class SGSLatestLimitEnvelope(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    erro: SGSLatestLimitDetail
