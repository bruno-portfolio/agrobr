from __future__ import annotations

import math
import re
from typing import Any

import pydantic

from agrobr import constants
from agrobr.normalize.regions import BIOMAS as BIOMAS  # noqa: F401
from agrobr.normalize.regions import BIOMAS_VALIDOS as BIOMAS_VALIDOS  # noqa: F401
from agrobr.normalize.regions import normalizar_bioma as normalizar_bioma  # noqa: F401

ANO_INICIO = 1985
COLECAO_ATUAL = 11
ANOS_FINAIS = {10: 2024, 11: 2025}
ANO_FIM = ANOS_FINAIS[COLECAO_ATUAL]
CLASSES_LEGENDA: dict[int, str] = {
    1: "Floresta",
    3: "Formação Florestal",
    4: "Formação Savânica",
    5: "Mangue",
    6: "Floresta Alagável",
    9: "Silvicultura",
    10: "Vegetação Herbácea e Arbustiva",
    11: "Campo Alagado e Área Pantanosa",
    12: "Formação Campestre",
    14: "Agropecuária",
    15: "Pastagem",
    18: "Agricultura",
    19: "Lavoura Temporária",
    20: "Cana",
    21: "Mosaico de Usos",
    22: "Área não Vegetada",
    23: "Praia, Duna e Areal",
    24: "Área Urbanizada",
    25: "Outras Áreas não Vegetadas",
    26: "Corpo D'água",
    27: "Não observado",
    29: "Afloramento Rochoso",
    30: "Mineração",
    31: "Aquicultura",
    32: "Apicum",
    33: "Rio, Lago e Oceano",
    35: "Dendê",
    36: "Lavoura Perene",
    39: "Soja",
    40: "Arroz",
    41: "Outras Lavouras Temporárias",
    46: "Café",
    47: "Citrus",
    48: "Outras Lavouras Perenes",
    49: "Restinga Arbórea",
    50: "Restinga Herbácea",
    62: "Algodão (beta)",
    75: "Usina Fotovoltaica",
}

CLASSES_LEGENDA_11 = {
    **CLASSES_LEGENDA,
    0: "Não observado",
    7: "Savana Alagada (beta)",
    13: "Mosaico Herbáceo-Arbustivo",
    22: "Área não vegetada",
    50: "Restinga Herbácea ou Arbustiva",
    75: "Usina Fotovoltaica",
    77: "Formação Herbáceo Arbustiva",
    84: "Marisma (beta)",
    91: "Parque eólico (beta)",
}

CLASSES_LEGENDA_10 = {
    **CLASSES_LEGENDA,
    0: "Não observado",
    13: "Outras Formações não Florestais",
}

SHEET_COBERTURA = "COVERAGE_11"
SHEET_TRANSICAO = "TRANSITION_11"

COLUNAS_SAIDA_COBERTURA = [
    "bioma",
    "estado",
    "classe_id",
    "classe",
    "nivel_0",
    "ano",
    "area_ha",
]

COLUNAS_SAIDA_COBERTURA_MUNICIPAL = [
    "bioma",
    "estado",
    "municipio",
    "classe_id",
    "classe",
    "nivel_0",
    "ano",
    "area_ha",
]

COLUNAS_SAIDA_COBERTURA_MUNICIPAL_V2 = [
    *COLUNAS_SAIDA_COBERTURA_MUNICIPAL,
    "geocodigo",
    "id_registro",
]

COLUNAS_SAIDA_TRANSICAO = [
    "bioma",
    "estado",
    "classe_de_id",
    "classe_de",
    "classe_para_id",
    "classe_para",
    "periodo",
    "area_ha",
]

ESTADOS_MAPBIOMAS: dict[str, str] = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO",
}


def estado_para_uf(estado: str) -> str:
    normalized = estado.strip().rstrip("\n")
    return ESTADOS_MAPBIOMAS.get(normalized, normalized)


def legenda_colecao(colecao: int) -> dict[int, str]:
    return CLASSES_LEGENDA_11 if colecao == 11 else CLASSES_LEGENDA_10


def classe_para_nome(classe_id: int, colecao: int = COLECAO_ATUAL) -> str | None:
    return legenda_colecao(colecao).get(classe_id)


def _nonblank_text(value: str) -> str:
    if not value.strip():
        raise ValueError("texto publicado vazio")
    return value


class MunicipalCoverageRecord(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    source_id: int = pydantic.Field(alias="ID", ge=0, le=2**63 - 1)
    country: str
    biome: str
    state: str
    geocode: str
    municipality: str
    municipality_state: str = pydantic.Field(alias="municipality-state")
    class_id: int = pydantic.Field(alias="class", ge=0)
    class_level_0: str
    class_level_1: str
    class_level_2: str
    class_level_3: str
    class_level_4: str
    areas: tuple[float, ...]

    @pydantic.field_validator(
        "country",
        "biome",
        "state",
        "municipality",
        "municipality_state",
        "class_level_0",
        "class_level_1",
        "class_level_2",
        "class_level_3",
        "class_level_4",
    )
    @classmethod
    def nonblank_text(cls, value: str) -> str:
        return _nonblank_text(value)

    @pydantic.field_validator("country")
    @classmethod
    def brazilian_coverage(cls, value: str) -> str:
        if value != "Brasil":
            raise ValueError("país incompatível com a cobertura brasileira")
        return value

    @pydantic.field_validator("biome")
    @classmethod
    def known_biome(cls, value: str) -> str:
        if value not in BIOMAS_VALIDOS:
            raise ValueError("bioma não reconhecido")
        return value

    @pydantic.field_validator("state")
    @classmethod
    def known_state(cls, value: str) -> str:
        if estado_para_uf(value) not in ESTADOS_MAPBIOMAS.values():
            raise ValueError("estado não reconhecido")
        return value

    @pydantic.field_validator("geocode")
    @classmethod
    def published_geocode(cls, value: str) -> str:
        if not re.fullmatch(constants.MAPBIOMAS_GEOCODE_PATTERN, value):
            raise ValueError("geocode deve conter sete dígitos ASCII publicados")
        return value

    @pydantic.field_validator("areas", mode="before")
    @classmethod
    def complete_numeric_vector(cls, value: Any, info: pydantic.ValidationInfo) -> Any:
        if not isinstance(value, tuple):
            raise ValueError("vetor anual deve ser uma tupla")
        years = (info.context or {}).get("years", ())
        if len(value) != len(years):
            raise ValueError("vetor anual incompatível com o cabeçalho")
        for year, area in zip(years, value, strict=True):
            if isinstance(area, bool) or not isinstance(area, (int, float)):
                raise ValueError(f"área de {year} deve ser numérica e não nula")
            try:
                finite = math.isfinite(area)
            except OverflowError as exc:
                raise ValueError(f"área de {year} excede a representação numérica") from exc
            if not finite or area < 0:
                raise ValueError(f"área de {year} deve ser finita e não negativa")
        return value


class MunicipalCoverageRow(MunicipalCoverageRecord):
    region: str

    @pydantic.field_validator("region")
    @classmethod
    def published_region(cls, value: str) -> str:
        return _nonblank_text(value)


class MunicipalCoverageRow10(MunicipalCoverageRecord):
    municipality_state: str = pydantic.Field(alias="municipality - state")
    feature_id: int = pydantic.Field(ge=0)
