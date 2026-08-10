from __future__ import annotations

import re
from datetime import datetime

from pydantic import BaseModel, field_validator

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


def normalize_produto(nome: str) -> str:
    key = nome.strip().lower()
    return PRODUTO_ALIASES.get(key, key)


def resolve_produto(nome: str) -> str:
    key = nome.strip().lower()
    if key not in PRODUTO_ALIASES:
        raise ValueError(
            f"produto desconhecido: {nome!r}. Válidos: {sorted(set(PRODUTO_ALIASES.values()))}"
        )
    return PRODUTO_ALIASES[key]


_TITLE_WEEK_RE = re.compile(r"ANEC\s*-\s*(\d{1,2})\.(\d{4})", re.IGNORECASE)


class ANECArticle(BaseModel):
    id: int
    cuid: str
    title_en: str
    slug_en: str
    created_at: datetime
    pdf_url: str
    media_updated_at: datetime

    @field_validator("pdf_url")
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
