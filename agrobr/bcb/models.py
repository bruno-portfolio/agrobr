"""Modelos Pydantic para crédito rural BCB/SICOR."""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, Field, field_validator


class CreditoRural(BaseModel):
    """Registro de crédito rural agregado por município."""

    safra: str = Field(..., description="Safra no formato YYYY/YYYY ou YYYY/YY")
    ano_emissao: int | None = None
    mes_emissao: int | None = Field(None, ge=1, le=12)
    cd_municipio: str | None = Field(None, description="Código IBGE do município (7 dígitos)")
    municipio: str | None = None
    uf: str | None = Field(None, min_length=2, max_length=2)
    produto: str = Field(...)
    finalidade: str = Field(default="custeio")
    valor: float = Field(..., ge=0, description="Valor total do crédito (R$)")
    area_financiada: float | None = Field(None, ge=0, description="Área financiada (ha)")
    qtd_contratos: int | None = Field(None, ge=0, description="Número de contratos")

    @field_validator("uf", mode="before")
    @classmethod
    def normalize_uf(cls, v: Any) -> str | None:
        if v is None:
            return None
        if isinstance(v, str):
            return v.upper().strip()
        return str(v)

    @field_validator("produto", mode="before")
    @classmethod
    def normalize_produto(cls, v: str) -> str:
        return v.lower().strip()

    @field_validator("finalidade", mode="before")
    @classmethod
    def normalize_finalidade(cls, v: str) -> str:
        return v.lower().strip()


# Mapeamento agrobr → SICOR nome do produto
SICOR_PRODUTOS: dict[str, str] = {
    "soja": "SOJA",
    "milho": "MILHO",
    "arroz": "ARROZ",
    "feijao": "FEIJAO",
    "trigo": "TRIGO",
    "algodao": "ALGODAO HERBACEO",
    "cafe": "CAFE",
    "cafe_arabica": "CAFE ARABICA",
    "cafe_conilon": "CAFE CONILON",
    "cana": "CANA-DE-ACUCAR",
    "mandioca": "MANDIOCA",
    "sorgo": "SORGO",
}

SICOR_FINALIDADES: dict[str, str] = {
    "custeio": "CUSTEIO",
    "investimento": "INVESTIMENTO",
    "comercializacao": "COMERCIALIZACAO",
    "industrializacao": "INDUSTRIALIZACAO",
}

# Mapeamento de UF sigla → código IBGE (2 dígitos)
UF_CODES: dict[str, str] = {
    "RO": "11",
    "AC": "12",
    "AM": "13",
    "RR": "14",
    "PA": "15",
    "AP": "16",
    "TO": "17",
    "MA": "21",
    "PI": "22",
    "CE": "23",
    "RN": "24",
    "PB": "25",
    "PE": "26",
    "AL": "27",
    "SE": "28",
    "BA": "29",
    "MG": "31",
    "ES": "32",
    "RJ": "33",
    "SP": "35",
    "PR": "41",
    "SC": "42",
    "RS": "43",
    "MS": "50",
    "MT": "51",
    "GO": "52",
    "DF": "53",
}


def normalize_safra_sicor(safra: str) -> str:
    """Normaliza safra para formato SICOR (YYYY/YYYY).

    Args:
        safra: Safra em formato "2023/24", "2023/2024", ou "2024".

    Returns:
        Safra no formato "2023/2024".
    """
    safra = safra.strip()

    # Formato "2023/2024" — já OK
    if re.match(r"^\d{4}/\d{4}$", safra):
        return safra

    # Formato "2023/24"
    match = re.match(r"^(\d{4})/(\d{2})$", safra)
    if match:
        ano_inicio = int(match.group(1))
        # ano_fim é sempre ano_inicio + 1 para safras agrícolas
        ano_fim = ano_inicio + 1
        return f"{ano_inicio}/{ano_fim}"

    # Formato "2024" — assume "2023/2024"
    if re.match(r"^\d{4}$", safra):
        ano = int(safra)
        return f"{ano - 1}/{ano}"

    return safra


def resolve_produto_sicor(produto: str) -> str:
    """Resolve nome de produto agrobr para nome SICOR.

    Args:
        produto: Nome do produto no formato agrobr.

    Returns:
        Nome do produto no formato SICOR (maiúsculas).
    """
    lower = produto.lower().strip()
    return SICOR_PRODUTOS.get(lower, produto.upper())
