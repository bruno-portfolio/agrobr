from __future__ import annotations

import copy
import json
import sys
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator

from .constants import Fonte
from .utils.time import utcnow

__all__ = [
    "Fingerprint",
    "Indicador",
    "MetaInfo",
    "Safra",
]


class Indicador(BaseModel):
    fonte: Fonte
    produto: str = Field(..., min_length=2)
    praca: str | None = None
    data: date
    valor: Decimal = Field(..., gt=0)
    unidade: str
    metodologia: str | None = None
    revisao: int = Field(default=0, ge=0)
    meta: dict[str, Any] = Field(default_factory=dict)

    parsed_at: datetime = Field(default_factory=utcnow)
    parser_version: int = Field(default=1)
    anomalies: list[str] = Field(default_factory=list)

    @field_validator("produto")
    @classmethod
    def lowercase_produto(cls, v: str) -> str:
        if isinstance(v, str):
            return v.lower().strip()
        return v


class Safra(BaseModel):
    fonte: Fonte
    produto: str
    safra: str = Field(..., pattern=r"^\d{4}/\d{2}$")
    uf: str | None = Field(None, min_length=2, max_length=2)
    area_plantada: Decimal | None = Field(None, ge=0)
    producao: Decimal | None = Field(None, ge=0)
    produtividade: Decimal | None = Field(None, ge=0)
    unidade_area: str = Field(default="mil_ha")
    unidade_producao: str = Field(default="mil_ton")
    levantamento: int = Field(..., ge=1, le=12)
    data_publicacao: date | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    parsed_at: datetime = Field(default_factory=utcnow)
    parser_version: int = Field(default=1)
    anomalies: list[str] = Field(default_factory=list)


class Fingerprint(BaseModel):
    source: Fonte
    url: str
    collected_at: datetime
    table_classes: list[list[str]]
    key_ids: list[str]
    structure_hash: str
    table_headers: list[list[str]]
    element_counts: dict[str, int]


@dataclass
class MetaInfo:
    source: str
    source_url: str
    source_method: str
    fetched_at: datetime
    timestamp: datetime = dataclass_field(default_factory=utcnow)
    fetch_duration_ms: int = 0
    parse_duration_ms: int = 0
    from_cache: bool = False
    cache_key: str | None = None
    cache_expires_at: datetime | None = None
    raw_content_hash: str | None = None
    raw_content_size: int = 0
    records_count: int = 0
    columns: list[str] = dataclass_field(default_factory=list)
    agrobr_version: str = ""
    schema_version: str = "1.0"
    parser_version: int = 1
    python_version: str = ""
    validation_passed: bool = True
    validation_warnings: list[str] = dataclass_field(default_factory=list)
    dataset: str = ""
    contract_version: str = ""
    snapshot: str | None = None
    attempted_sources: list[str] = dataclass_field(default_factory=list)
    selected_source: str = ""
    data_sources: list[str] = dataclass_field(default_factory=list)
    fetch_timestamp: datetime | None = None
    source_details: dict[str, Any] = dataclass_field(default_factory=dict)

    def __setattr__(self, name: str, value: Any) -> None:
        if (
            name in ("fetched_at", "timestamp", "cache_expires_at", "fetch_timestamp")
            and value is not None
        ):
            value = (
                value.replace(tzinfo=UTC) if value.utcoffset() is None else value.astimezone(UTC)
            )
        super().__setattr__(name, value)

    def __post_init__(self) -> None:
        for name in ("fetched_at", "timestamp", "cache_expires_at", "fetch_timestamp"):
            setattr(self, name, getattr(self, name))

        if not self.agrobr_version:
            from agrobr import __version__

            self.agrobr_version = __version__

        if not self.python_version:
            self.python_version = sys.version.split()[0]

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "source_url": self.source_url,
            "source_method": self.source_method,
            "fetched_at": self.fetched_at.isoformat(),
            "timestamp": self.timestamp.isoformat(),
            "fetch_duration_ms": self.fetch_duration_ms,
            "parse_duration_ms": self.parse_duration_ms,
            "from_cache": self.from_cache,
            "cache_key": self.cache_key,
            "cache_expires_at": (
                self.cache_expires_at.isoformat() if self.cache_expires_at else None
            ),
            "raw_content_hash": self.raw_content_hash,
            "raw_content_size": self.raw_content_size,
            "records_count": self.records_count,
            "columns": self.columns,
            "agrobr_version": self.agrobr_version,
            "schema_version": self.schema_version,
            "parser_version": self.parser_version,
            "python_version": self.python_version,
            "validation_passed": self.validation_passed,
            "validation_warnings": self.validation_warnings,
            "dataset": self.dataset,
            "contract_version": self.contract_version,
            "snapshot": self.snapshot,
            "attempted_sources": self.attempted_sources,
            "selected_source": self.selected_source,
            "data_sources": self.data_sources,
            "fetch_timestamp": (self.fetch_timestamp.isoformat() if self.fetch_timestamp else None),
            "license": self.license,
            **(
                {"source_details": copy.deepcopy(self.source_details)}
                if self.source_details
                else {}
            ),
        }

    @property
    def license(self) -> str | None:
        """Classificação de licença do dado (``docs/licenses.md``).

        A mais restritiva entre as fontes de ``data_sources``; sem elas, a da fonte
        selecionada (ou da ``source``). Nula quando nenhuma está na tabela.
        """
        from agrobr import constants

        classes = [c for nome in self.data_sources if (c := constants.licenca_da_fonte(nome))]
        if classes:
            return max(classes, key=constants.LICENCAS_EM_ORDEM.index)
        for nome in (self.selected_source, self.source.rsplit("/", 1)[-1]):
            if classe := constants.licenca_da_fonte(nome):
                return classe
        return None

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MetaInfo:
        data = copy.deepcopy(data)
        data.pop("license", None)

        for key in ["fetched_at", "timestamp", "cache_expires_at", "fetch_timestamp"]:
            if data.get(key) and isinstance(data[key], str):
                data[key] = datetime.fromisoformat(data[key])

        return cls(**data)
