"""Modelos Pydantic para dados ANDA (fertilizantes)."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class EntregaFertilizante(BaseModel):
    """Registro de entrega de fertilizante por UF/mês."""

    ano: int = Field(..., ge=2000, le=2100)
    mes: int = Field(..., ge=1, le=12)
    uf: str = Field(..., min_length=2, max_length=2)
    produto_fertilizante: str = Field(..., min_length=1)
    volume_ton: float = Field(..., ge=0)

    @field_validator("uf")
    @classmethod
    def normalize_uf(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("produto_fertilizante")
    @classmethod
    def normalize_produto(cls, v: str) -> str:
        return v.strip().lower()


# Mapeamento de nomes comuns → nomes canônicos ANDA
FERTILIZANTES_MAP: dict[str, str] = {
    "npk": "npk",
    "ureia": "ureia",
    "uréia": "ureia",
    "map": "map",
    "dap": "dap",
    "kcl": "kcl",
    "cloreto de potássio": "kcl",
    "cloreto de potassio": "kcl",
    "superfosfato simples": "ssp",
    "ssp": "ssp",
    "superfosfato triplo": "tsp",
    "tsp": "tsp",
    "sulfato de amônio": "sulfato de amonio",
    "sulfato de amonio": "sulfato de amonio",
    "nitrato de amônio": "nitrato de amonio",
    "nitrato de amonio": "nitrato de amonio",
    "total": "total",
}

# UFs válidas para dados ANDA
ANDA_UFS: list[str] = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]


def normalize_fertilizante(nome: str) -> str:
    """Normaliza nome de fertilizante para forma canônica.

    Args:
        nome: Nome do fertilizante (qualquer formato).

    Returns:
        Nome canônico em minúsculas.
    """
    key = nome.strip().lower()
    return FERTILIZANTES_MAP.get(key, key)
