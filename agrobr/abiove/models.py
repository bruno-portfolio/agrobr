"""Modelos e constantes para dados ABIOVE (complexo soja)."""

from __future__ import annotations

from pydantic import BaseModel, field_validator

# Produtos do complexo soja na nomenclatura ABIOVE
ABIOVE_PRODUTOS: dict[str, str] = {
    "grao": "grao",
    "grão": "grao",
    "soja em grão": "grao",
    "soja em grao": "grao",
    "soja grão": "grao",
    "soja grao": "grao",
    "grain": "grao",
    "soybeans": "grao",
    "soybean": "grao",
    "farelo": "farelo",
    "farelo de soja": "farelo",
    "soybean meal": "farelo",
    "soymeal": "farelo",
    "meal": "farelo",
    "oleo": "oleo",
    "óleo": "oleo",
    "oleo de soja": "oleo",
    "óleo de soja": "oleo",
    "soybean oil": "oleo",
    "soyoil": "oleo",
    "oil": "oleo",
    "milho": "milho",
    "corn": "milho",
    "maize": "milho",
    "total": "total",
}

# Meses em português para detecção no Excel
MESES_PT: dict[str, int] = {
    "janeiro": 1,
    "fevereiro": 2,
    "março": 3,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
}


def normalize_produto(nome: str) -> str:
    """Normaliza nome de produto ABIOVE para forma canônica.

    Args:
        nome: Nome do produto (qualquer casing/formato).

    Returns:
        Nome canônico: "grao", "farelo", "oleo", "milho", "total".
    """
    key = nome.strip().lower()
    return ABIOVE_PRODUTOS.get(key, key)


class ExportacaoSoja(BaseModel):
    """Registro de exportação do complexo soja."""

    ano: int
    mes: int
    produto: str
    volume_ton: float
    receita_usd_mil: float | None = None

    @field_validator("mes")
    @classmethod
    def validate_mes(cls, v: int) -> int:
        if not 1 <= v <= 12:
            raise ValueError(f"mes deve ser 1-12, recebido {v}")
        return v

    @field_validator("produto", mode="before")
    @classmethod
    def normalize_produto_validator(cls, v: str) -> str:
        return normalize_produto(v)

    @field_validator("volume_ton")
    @classmethod
    def validate_volume(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"volume_ton não pode ser negativo: {v}")
        return v
