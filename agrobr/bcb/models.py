from __future__ import annotations

import re
from typing import Any

import structlog
from pydantic import BaseModel, Field, field_validator

logger = structlog.get_logger()

SICOR_PROGRAMAS: dict[str, str] = {
    "0001": "Pronaf",
    "0002": "Proger Rural",
    "0050": "Pronamp",
    "0070": "Funcafe",
    "0100": "Moderfrota",
    "0102": "Moderagro",
    "0104": "Prodecoop",
    "0106": "Moderinfra",
    "0108": "ABC",
    "0110": "Inovagro",
    "0112": "PCA",
    "0114": "Pronamp Investimento",
    "0150": "Procap-Agro",
    "0152": "RenovAgro",
    "0156": "Moderagro/Moderfrota",
    "0200": "Proirriga",
    "0999": "Sem programa especifico",
}

SICOR_FONTES_RECURSO: dict[str, str] = {
    "0201": "Recursos obrigatorios (MCR 6.2)",
    "0300": "Poupanca rural livre",
    "0303": "Poupanca rural controlados",
    "0400": "BNDES equalizavel",
    "0402": "BNDES/Finame equalizavel",
    "0430": "LCA",
    "0501": "FNO",
    "0502": "FNE",
    "0503": "FCO",
    "0505": "Funcafe",
    "0800": "Recursos livres",
}

SICOR_TIPOS_SEGURO: dict[str, str] = {
    "1": "Proagro",
    "2": "Sem seguro",
    "3": "Seguro privado",
    "9": "Nao se aplica",
}

SICOR_MODALIDADES: dict[str, str] = {
    "01": "Individual",
    "02": "Coletiva com garantia individual",
    "03": "Coletiva",
}

SICOR_ATIVIDADES: dict[str, str] = {
    "1": "Agricola",
    "2": "Pecuaria",
}


def _resolve(dicionario: dict[str, str], codigo: str, dominio: str) -> str:
    nome = dicionario.get(codigo)
    if nome is not None:
        return nome
    logger.warning("sicor_codigo_desconhecido", dominio=dominio, codigo=codigo)
    return f"Desconhecido ({codigo})"


def resolve_programa(cd: str) -> str:
    return _resolve(SICOR_PROGRAMAS, cd, "programa")


def resolve_fonte_recurso(cd: str) -> str:
    return _resolve(SICOR_FONTES_RECURSO, cd, "fonte_recurso")


def resolve_tipo_seguro(cd: str) -> str:
    return _resolve(SICOR_TIPOS_SEGURO, cd, "tipo_seguro")


def resolve_modalidade(cd: str) -> str:
    return _resolve(SICOR_MODALIDADES, cd, "modalidade")


def resolve_atividade(cd: str) -> str:
    return _resolve(SICOR_ATIVIDADES, cd, "atividade")


class CreditoRural(BaseModel):
    safra: str = Field(...)
    ano_emissao: int | None = None
    mes_emissao: int | None = Field(None, ge=1, le=12)
    cd_municipio: str | None = Field(None)
    municipio: str | None = None
    uf: str | None = Field(None, min_length=2, max_length=2)
    produto: str = Field(...)
    finalidade: str = Field(default="custeio")
    valor: float = Field(..., ge=0)
    area_financiada: float | None = Field(None, ge=0)
    qtd_contratos: int | None = Field(None, ge=0)
    regiao: str | None = None
    cd_programa: str | None = None
    programa: str | None = None
    cd_sub_programa: str | None = None
    cd_fonte_recurso: str | None = None
    fonte_recurso: str | None = None
    cd_tipo_seguro: str | None = None
    tipo_seguro: str | None = None
    cd_modalidade: str | None = None
    modalidade: str | None = None
    cd_atividade: str | None = None
    atividade: str | None = None

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
    safra = safra.strip()

    if re.match(r"^\d{4}/\d{4}$", safra):
        return safra

    match = re.match(r"^(\d{4})/(\d{2})$", safra)
    if match:
        ano_inicio = int(match.group(1))
        ano_fim = ano_inicio + 1
        return f"{ano_inicio}/{ano_fim}"

    if re.match(r"^\d{4}$", safra):
        ano = int(safra)
        return f"{ano - 1}/{ano}"

    return safra


def resolve_produto_sicor(produto: str) -> str:
    lower = produto.lower().strip()
    return SICOR_PRODUTOS.get(lower, produto.upper())
