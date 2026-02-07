"""Modelos Pydantic v2 do agrobr."""

from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field, field_validator

from .constants import Fonte

if TYPE_CHECKING:
    import pandas as pd


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

    parsed_at: datetime = Field(default_factory=datetime.utcnow)
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
    data_publicacao: date
    meta: dict[str, Any] = Field(default_factory=dict)

    parsed_at: datetime = Field(default_factory=datetime.utcnow)
    parser_version: int = Field(default=1)
    anomalies: list[str] = Field(default_factory=list)


class CacheEntry(BaseModel):
    key: str
    data: bytes
    created_at: datetime
    expires_at: datetime
    source: Fonte
    version: int = 1
    stale: bool = False
    hit_count: int = 0


class HistoryEntry(BaseModel):
    key: str
    data: bytes
    source: Fonte
    data_date: date
    collected_at: datetime
    parser_version: int
    fingerprint_hash: str


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
    """Metadados de proveniencia e rastreabilidade para data lineage."""

    source: str
    source_url: str
    source_method: str
    fetched_at: datetime
    timestamp: datetime = dataclass_field(default_factory=datetime.now)
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
    fetch_timestamp: datetime | None = None

    def __post_init__(self) -> None:
        """Preenche versoes automaticamente."""
        if not self.agrobr_version:
            from agrobr import __version__

            self.agrobr_version = __version__

        if not self.python_version:
            self.python_version = sys.version.split()[0]

    def to_dict(self) -> dict[str, Any]:
        """Converte para dicionario serializavel."""
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
            "fetch_timestamp": (self.fetch_timestamp.isoformat() if self.fetch_timestamp else None),
        }

    def to_json(self, indent: int = 2) -> str:
        """Serializa para JSON."""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MetaInfo:
        """Reconstroi a partir de dicionario."""
        data = data.copy()

        for key in ["fetched_at", "timestamp", "cache_expires_at", "fetch_timestamp"]:
            if data.get(key) and isinstance(data[key], str):
                data[key] = datetime.fromisoformat(data[key])

        return cls(**data)

    def compute_dataframe_hash(self, df: pd.DataFrame) -> str:
        """Computa hash do DataFrame para verificacao de integridade."""
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        return f"sha256:{hashlib.sha256(csv_bytes).hexdigest()}"

    def verify_hash(self, df: pd.DataFrame) -> bool:
        """Verifica se DataFrame corresponde ao hash original."""
        if not self.raw_content_hash:
            return True

        current_hash = self.compute_dataframe_hash(df)
        return current_hash == self.raw_content_hash
