from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, FiniteFloat, field_validator

from agrobr.constants import URLS, Fonte

WFS_BASE: str = URLS[Fonte.ICMBIO]["geoserver"]
WFS_VERSION = "1.1.0"
LAYER = "limiteucsfederais_a"
NAMESPACE = "ICMBio"
GEOM_COLUMN = "the_geom"
MAX_FEATURES_GEO = 500
MAX_FEATURES_TABULAR = 500

PROPERTY_NAMES = [
    "cnuc",
    "nomeuc",
    "sigla_cate",
    "grupouc",
    "areahaalb",
    "uf",
    "biomas",
    "criacaoano",
    "criacaoato",
]

PROPERTY_NAMES_GEO = [GEOM_COLUMN] + PROPERTY_NAMES

RENAME_MAP: dict[str, str] = {
    "cnuc": "codigo",
    "nomeuc": "nome",
    "sigla_cate": "categoria",
    "grupouc": "grupo",
    "areahaalb": "area_ha",
    "uf": "uf",
    "biomas": "bioma",
    "criacaoano": "ano_criacao",
    "criacaoato": "ato_criacao",
}

COLUNAS_SAIDA = [
    "codigo",
    "nome",
    "categoria",
    "grupo",
    "uf",
    "bioma",
    "area_ha",
    "ano_criacao",
    "ato_criacao",
]

COLUNAS_SAIDA_GEO = COLUNAS_SAIDA + ["geometry"]

GRUPOS_VALIDOS = frozenset({"PI", "US"})


class UnidadeConservacao(BaseModel):
    cnuc: str
    nomeuc: str
    sigla_cate: str
    grupouc: str
    areahaalb: FiniteFloat | None
    uf: str
    biomas: str
    criacaoano: int | None = Field(ge=-(2**63), le=2**63 - 1)
    criacaoato: str

    @field_validator("areahaalb", "criacaoano", mode="before")
    @classmethod
    def empty_numeric(cls, value: Any) -> Any:
        return None if value == "" else value

    @field_validator("grupouc")
    @classmethod
    def uppercase_group(cls, value: str) -> str:
        return value.upper()


class FeatureCount(BaseModel):
    number_of_features: int = Field(ge=0)

    @field_validator("number_of_features", mode="before")
    @classmethod
    def decimal_count(cls, value: Any) -> int:
        if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
            raise ValueError("numberOfFeatures deve ser um inteiro decimal nao negativo")
        return int(value)
