from __future__ import annotations

import re
from collections.abc import Collection
from datetime import datetime
from typing import Self

import pydantic

from agrobr.exceptions import InvalidParameterError
from agrobr.utils import time as time_utils

MIN_YEAR = 2026

CATEGORIES_BY_YEAR: dict[int, str] = {
    2026: "cmke9w2wu8873b4txfa547lki",
}

PERIODO_LAST_WEEK = "last_week"
PERIODO_CURRENT_WEEK = "current_week"
TIPO_EFETIVADO = "efetivado"
TIPO_PROGRAMADO = "programado"

PRODUTO_ALIASES: dict[str, str] = {
    "soja": "soybean",
    "soja grão": "soybean",
    "soja grao": "soybean",
    "soja em grão": "soybean",
    "soja em grao": "soybean",
    "soybean": "soybean",
    "soybeans": "soybean",
    "farelo": "soybean_meal",
    "farelo de soja": "soybean_meal",
    "soybean meal": "soybean_meal",
    "soybean_meal": "soybean_meal",
    "soybeanmeal": "soybean_meal",
    "soymeal": "soybean_meal",
    "meal": "soybean_meal",
    "milho": "maize",
    "maize": "maize",
    "corn": "maize",
    "ddgs": "ddgs",
    "sorgo": "sorghum",
    "sorghum": "sorghum",
    "trigo": "wheat",
    "wheat": "wheat",
}

DESTINOS_PRODUTOS_PUBLICADOS: tuple[str, ...] = (
    "soybean",
    "soybean_meal",
    "maize",
    "wheat",
)
"""Produtos com tabela de importadores nas edições W13 e W34/2026."""


def resolve_produto(nome: str) -> str:
    key = nome.strip().lower()
    if key not in PRODUTO_ALIASES:
        raise ValueError(
            f"produto desconhecido: {nome!r}. Válidos: {sorted(set(PRODUTO_ALIASES.values()))}"
        )
    return PRODUTO_ALIASES[key]


def validate_year(ano: int) -> None:
    current = time_utils.hoje().year
    if isinstance(ano, bool) or not isinstance(ano, int) or not MIN_YEAR <= ano <= current:
        raise InvalidParameterError(
            f"ano deve ser um ano de edição entre {MIN_YEAR} e {current}; recebido {ano!r}"
        )


class ANECCategory(pydantic.BaseModel):
    cuid: str = pydantic.Field(min_length=1, pattern=r"^[a-zA-Z0-9_-]+$")
    nameEN: str = ""
    nameBR: str = ""
    public: bool = True
    children: list[ANECCategory] = pydantic.Field(default_factory=list)


def validate_filters(
    ano: int,
    semana: int | None,
    produto: str | None,
    *,
    allow_total: bool = False,
    produtos_publicados: Collection[str] | None = None,
) -> str | None:
    validate_year(ano)
    if semana is not None and (
        isinstance(semana, bool) or not isinstance(semana, int) or not 1 <= semana <= 53
    ):
        raise InvalidParameterError(f"semana deve ser um inteiro de 1 a 53; recebido {semana!r}")
    if produto is None:
        return None
    if not isinstance(produto, str):
        raise InvalidParameterError(f"produto deve ser uma string; recebido {produto!r}")
    if allow_total and produto.strip().lower() == "total_products":
        return "total_products"
    try:
        canonical = resolve_produto(produto)
    except ValueError as exc:
        raise InvalidParameterError(str(exc)) from exc
    if produtos_publicados is not None and canonical not in produtos_publicados:
        raise InvalidParameterError(
            f"produto {produto!r} não tem tabela de destinos na ANEC. "
            f"Válidos: {list(produtos_publicados)}"
        )
    return canonical


_TITLE_WEEK_RE = re.compile(r"ANEC\s*-\s*(\d{1,2})\.(\d{4})", re.IGNORECASE)


class ANECArticle(pydantic.BaseModel):
    id: int
    cuid: str
    title_en: str
    slug_en: str
    created_at: datetime
    pdf_url: str
    media_updated_at: datetime

    @pydantic.field_validator("pdf_url")
    @classmethod
    def validate_pdf_url(cls, v: str) -> str:
        if not v.startswith("http"):
            raise ValueError(f"pdf_url deve ser absoluta, recebido: {v}")
        if not v.lower().endswith(".pdf"):
            raise ValueError(f"pdf_url deve ter extensão .pdf, recebido: {v}")
        return v

    @property
    def week_year(self) -> tuple[int, int]:
        m = _TITLE_WEEK_RE.search(self.title_en)
        if not m:
            raise ValueError(f"Não foi possível extrair semana/ano de: {self.title_en!r}")
        week = int(m.group(1))
        year = int(m.group(2))
        if not 1 <= week <= 53:
            raise ValueError(f"Semana fora do intervalo 1-53: {week}")
        return week, year


class ANECMonthlyShipment(pydantic.BaseModel):
    ano: int = pydantic.Field(ge=1900, strict=True)
    mes: int = pydantic.Field(ge=1, le=12, strict=True)
    produto: str = pydantic.Field(min_length=1)
    valor_ton: float | None = pydantic.Field(default=None, ge=0, allow_inf_nan=False)
    eh_estimativa: bool = pydantic.Field(strict=True)
    valor_min_ton: float | None = pydantic.Field(default=None, ge=0, allow_inf_nan=False)
    valor_max_ton: float | None = pydantic.Field(default=None, ge=0, allow_inf_nan=False)

    @pydantic.model_validator(mode="after")
    def validate_range(self) -> Self:
        if (self.valor_min_ton is None) != (self.valor_max_ton is None):
            raise ValueError("Faixa mensal exige ambos os limites")
        if self.valor_min_ton is not None and self.valor_max_ton is not None:
            if self.valor_min_ton > self.valor_max_ton:
                raise ValueError("Limite mínimo mensal excede o máximo")
            if self.valor_ton is not None:
                raise ValueError("Faixa mensal exige valor_ton ausente")
        return self


class ANECAnnualComparison(pydantic.BaseModel):
    mes: int = pydantic.Field(ge=1, le=12, strict=True)
    produto: str = pydantic.Field(min_length=1)
    valor_base_ton: float | None = pydantic.Field(default=None, ge=0, allow_inf_nan=False)
    valor_comparacao_ton: float | None = pydantic.Field(default=None, ge=0, allow_inf_nan=False)
    valor_2025: float | None = pydantic.Field(default=None, ge=0, allow_inf_nan=False)
    valor_2026: float | None = pydantic.Field(default=None, ge=0, allow_inf_nan=False)
    ano_base: int = pydantic.Field(ge=1900, strict=True)
    ano_comparacao: int = pydantic.Field(ge=1900, strict=True)
    eh_estimativa: bool = pydantic.Field(strict=True)

    @pydantic.model_validator(mode="after")
    def align_year_values(self) -> Self:
        if self.ano_comparacao != self.ano_base + 1:
            raise ValueError("Comparação anual exige anos consecutivos em ordem crescente")
        for year, field in (
            (self.ano_base, "valor_base_ton"),
            (self.ano_comparacao, "valor_comparacao_ton"),
        ):
            if year in {2025, 2026}:
                setattr(self, f"valor_{year}", getattr(self, field))
        return self


class ANECDestination(pydantic.BaseModel):
    produto: str = pydantic.Field(min_length=1)
    destino: str = pydantic.Field(min_length=1)
    share_pct: float | None = pydantic.Field(default=None, ge=0, le=100, allow_inf_nan=False)
    ano: int | None = pydantic.Field(default=None, ge=1900, strict=True)
    mes_inicio: int | None = pydantic.Field(default=None, ge=1, le=12, strict=True)
    mes_fim: int | None = pydantic.Field(default=None, ge=1, le=12, strict=True)
