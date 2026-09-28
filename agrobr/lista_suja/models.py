from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

import pydantic

from agrobr.normalize import regions

PARSER_VERSION = 4

COLUNAS_SAIDA = [
    "empregador",
    "cpf_cnpj",
    "estabelecimento",
    "uf",
    "cnae",
    "data_inclusao",
    "trabalhadores_resgatados",
    "ano_acao_fiscal",
    "id_registro",
    "data_decisao",
    "data_atualizacao",
    "data_inclusao_texto",
]

SOURCE_COLUMNS = [
    "ID",
    "Ano da ação fiscal",
    "UF",
    "Empregador",
    "CNPJ/CPF",
    "Estabelecimento",
    "Trabalhadores envolvidos",
    "CNAE",
    "Decisão administrativa de procedência",
    "Inclusão no Cadastro de Empregadores",
]


class HTTPResource(pydantic.BaseModel):
    content: bytes = pydantic.Field(repr=False)
    requested_url: str
    url: str
    fetched_at: datetime
    sha256: str = pydantic.Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = pydantic.Field(ge=0)
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None

    @pydantic.field_validator("fetched_at")
    @classmethod
    def acquisition_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("fetched_at deve conter fuso horário")
        return value.astimezone(UTC)


class PublicationResources(pydantic.BaseModel):
    title: str
    resources: dict[str, str]


class Acquisition(pydantic.BaseModel):
    formato: Literal["csv", "pdf"]
    resource: HTTPResource
    discovery: HTTPResource
    publication: PublicationResources
    companion: HTTPResource | None = None
    attempted_sources: list[str]
    selected_source: str
    warnings: list[str] = pydantic.Field(default_factory=list)
    fallback: dict[str, Any] | None = None


class EmployerRecord(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid")

    empregador: str = pydantic.Field(min_length=1)
    cpf_cnpj: str = pydantic.Field(min_length=1)
    estabelecimento: str | None
    uf: str | None
    cnae: str | None
    data_inclusao: datetime | None
    trabalhadores_resgatados: int | None = pydantic.Field(ge=0, le=2**63 - 1)
    ano_acao_fiscal: int | None = pydantic.Field(ge=1, le=9999)
    id_registro: str = pydantic.Field(pattern=r"^[0-9]+$")
    data_decisao: datetime | None
    data_atualizacao: datetime | None
    data_inclusao_texto: str

    @pydantic.field_validator("uf")
    @classmethod
    def valid_uf(cls, value: str | None) -> str | None:
        if value is not None and value not in regions.UFS_VALIDAS:
            raise ValueError("UF inválida")
        return value
