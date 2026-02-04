"""Modelos Pydantic v2 do agrobr."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator

from .constants import Fonte


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
