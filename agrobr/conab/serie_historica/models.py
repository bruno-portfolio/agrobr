"""Modelos Pydantic para serie historica de safras CONAB."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class SafraHistorica(BaseModel):
    """Registro de serie historica de uma safra CONAB."""

    produto: str = Field(..., min_length=2)
    safra: str = Field(..., min_length=4)
    regiao: str | None = None
    uf: str | None = Field(None, min_length=2, max_length=2)
    area_plantada_mil_ha: float | None = Field(None, ge=0)
    producao_mil_ton: float | None = Field(None, ge=0)
    produtividade_kg_ha: float | None = Field(None, ge=0)

    @field_validator("produto", mode="before")
    @classmethod
    def normalize_produto(cls, v: str) -> str:
        return v.lower().strip()

    @field_validator("uf", mode="before")
    @classmethod
    def normalize_uf(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return v.upper().strip()

    @field_validator("regiao", mode="before")
    @classmethod
    def normalize_regiao(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return v.upper().strip()


SERIE_HISTORICA_PRODUTOS: dict[str, str] = {
    "graos": "graos",
    "grãos": "graos",
    "soja": "soja",
    "milho": "milho",
    "milho total": "milho",
    "milho 1a safra": "milho_1",
    "milho 1ª safra": "milho_1",
    "milho 2a safra": "milho_2",
    "milho 2ª safra": "milho_2",
    "milho 3a safra": "milho_3",
    "milho 3ª safra": "milho_3",
    "arroz": "arroz",
    "arroz total": "arroz",
    "feijao": "feijao",
    "feijão": "feijao",
    "feijão total": "feijao",
    "algodao": "algodao",
    "algodão": "algodao",
    "algodão em pluma": "algodao_pluma",
    "trigo": "trigo",
    "sorgo": "sorgo",
    "aveia": "aveia",
    "cevada": "cevada",
    "canola": "canola",
    "girassol": "girassol",
    "mamona": "mamona",
    "amendoim": "amendoim",
    "amendoim total": "amendoim",
    "centeio": "centeio",
    "triticale": "triticale",
    "gergelim": "gergelim",
    "cafe": "cafe",
    "café": "cafe",
    "cafe total": "cafe",
    "café total": "cafe",
    "cafe arabica": "cafe_arabica",
    "café arábica": "cafe_arabica",
    "cafe conilon": "cafe_conilon",
    "café conilon": "cafe_conilon",
    "cana": "cana",
    "cana de acucar": "cana",
    "cana-de-açúcar": "cana",
    "laranja": "laranja",
}

SERIE_HISTORICA_CATEGORIAS: dict[str, str] = {
    "graos": "graos",
    "cafe": "cafe",
    "cana": "cana",
    "laranja": "laranja",
}

UFS_BRASIL = [
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

REGIOES_BRASIL = ["NORTE", "NORDESTE", "CENTRO-OESTE", "SUDESTE", "SUL"]


def normalize_produto(nome: str) -> str:
    """Normaliza nome de produto para chave padronizada."""
    lower = nome.lower().strip()
    return SERIE_HISTORICA_PRODUTOS.get(lower, lower.replace(" ", "_"))


def detect_safra_format(value: str) -> str:
    """Detecta e normaliza formato de safra.

    Aceita: '2023/24', '2023/2024', '2023', 'Safra 2023/24'.
    Retorna formato curto: '2023/24'.
    """
    import re

    value = value.strip()
    value = re.sub(r"(?i)^safra\s*", "", value)

    match = re.match(r"(\d{4})/(\d{4})$", value)
    if match:
        return f"{match.group(1)}/{match.group(2)[2:]}"

    match = re.match(r"(\d{4})/(\d{2})$", value)
    if match:
        return value

    match = re.match(r"^(\d{4})$", value)
    if match:
        year = int(match.group(1))
        return f"{year}/{str(year + 1)[2:]}"

    return value
