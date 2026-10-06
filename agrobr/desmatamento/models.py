from __future__ import annotations

import re
from datetime import date
from typing import Annotated, Any, Literal

import pandas as pd
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, SerializeAsAny, field_validator

from agrobr import constants
from agrobr.desmatamento import json_numbers
from agrobr.normalize.regions import BIOMAS as BIOMAS  # noqa: F401
from agrobr.normalize.regions import BIOMAS_VALIDOS as BIOMAS_VALIDOS  # noqa: F401
from agrobr.normalize.regions import normalizar_bioma as normalizar_bioma  # noqa: F401

UF_ESTADO: dict[str, str] = {
    "ACRE": "AC",
    "ALAGOAS": "AL",
    "AMAPÁ": "AP",
    "AMAZONAS": "AM",
    "BAHIA": "BA",
    "CEARÁ": "CE",
    "DISTRITO FEDERAL": "DF",
    "ESPÍRITO SANTO": "ES",
    "GOIÁS": "GO",
    "MARANHÃO": "MA",
    "MATO GROSSO": "MT",
    "MATO GROSSO DO SUL": "MS",
    "MINAS GERAIS": "MG",
    "PARÁ": "PA",
    "PARAÍBA": "PB",
    "PARANÁ": "PR",
    "PERNAMBUCO": "PE",
    "PIAUÍ": "PI",
    "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS",
    "RONDÔNIA": "RO",
    "RORAIMA": "RR",
    "SANTA CATARINA": "SC",
    "SÃO PAULO": "SP",
    "SERGIPE": "SE",
    "TOCANTINS": "TO",
}

PRODES_WORKSPACES: dict[str, str] = {
    "Amazônia": "prodes-amazon-nb",
    "Cerrado": "prodes-cerrado-nb",
    "Caatinga": "prodes-caatinga-nb",
    "Mata Atlântica": "prodes-mata-atlantica-nb",
    "Pantanal": "prodes-pantanal-nb",
    "Pampa": "prodes-pampa-nb",
}

PRODES_LAYERS: dict[str, str] = {
    "Amazônia": "yearly_deforestation_biome",
    "Cerrado": "yearly_deforestation",
    "Caatinga": "yearly_deforestation",
    "Mata Atlântica": "yearly_deforestation",
    "Pantanal": "yearly_deforestation",
    "Pampa": "yearly_deforestation",
}

DETER_WORKSPACES: dict[str, str] = {
    "Amazônia": "deter-amz",
    "Cerrado": "deter-cerrado-nb",
}

DETER_LAYERS: dict[str, str] = {
    "Amazônia": "deter_amz",
    "Cerrado": "deter_cerrado",
}


def estado_para_uf(estado: str) -> str:
    return UF_ESTADO.get(estado.strip().upper(), estado.strip())


def civil_date(value: Any) -> date:
    if type(value) is not str or re.fullmatch(constants.DESMATAMENTO_DATE_PATTERN, value) is None:
        raise ValueError("Expected a civil ISO date string")
    parsed = date.fromisoformat(value)
    pd.Timestamp(parsed).as_unit("ns")
    return parsed


def published_date(value: Any) -> date:
    if type(value) is str and re.fullmatch("[0-9]{8}", value):
        value = f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return civil_date(value)


def published_text(value: Any) -> str:
    if type(value) is not str:
        raise ValueError("Expected a JSON string")
    return value


Number = Annotated[float, BeforeValidator(json_numbers.as_float)]
Integer = Annotated[int, BeforeValidator(json_numbers.as_int)]
Int32 = Annotated[Integer, Field(ge=-(2**31), le=2**31 - 1)]
Area = Annotated[Number, Field(ge=0)]
CivilDate = Annotated[date, BeforeValidator(civil_date)]
PublishedDate = Annotated[date, BeforeValidator(published_date)]
SceneIdentifier = Annotated[str, BeforeValidator(json_numbers.numeric_identifier)]
Text = Annotated[str, BeforeValidator(published_text)]


class Properties(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class ProdesProperties(Properties):
    fid: Int32 | None
    state: Text | None
    path_row: Text | None
    main_class: Text | None
    class_name: Text | None
    def_cloud: Number | None
    julian_day: Number | None
    image_date: CivilDate | None
    year: Number | None
    area_km: Area | None
    scene_id: SceneIdentifier | None
    publish_year: CivilDate | None
    source: Text | None
    satellite: Text | None
    sensor: Text | None
    uuid: Text | None


class ProdesAmazonProperties(ProdesProperties):
    fid: Int32
    uuid: Text
    image_date: CivilDate
    year: Int32 | None


class ProdesCerradoProperties(ProdesProperties):
    fid: Int32
    uuid: Text
    year: Int32 | None
    pub_date: PublishedDate | None


class ProdesCaatingaProperties(ProdesProperties):
    pass


class ProdesMataAtlanticaProperties(ProdesProperties):
    pass


class ProdesPantanalProperties(ProdesProperties):
    def_cloud: Int32 | None


class ProdesPampaProperties(ProdesCerradoProperties):
    pass


class DeterProperties(Properties):
    gid: Text | None
    classname: Text | None
    quadrant: Text | None
    path_row: Text | None
    view_date: CivilDate | None
    sensor: Text | None
    satellite: Text | None
    areauckm: Area | None
    uc: Text | None
    areamunkm: Area | None
    municipality: Text | None
    uf: Text | None
    publish_month: CivilDate | None


class DeterAmazonProperties(DeterProperties):
    mun_geocod: Text | None


class DeterCerradoProperties(DeterProperties):
    created_date: CivilDate | None
    areatotalkm: Area | None


class Feature(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    type: Literal["Feature"]
    id: Text
    properties: SerializeAsAny[Properties]

    @field_validator("id")
    @classmethod
    def nonempty_identifier(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Feature.id must be a nonblank string")
        return value


class ParsedPage(BaseModel):
    model_config = ConfigDict(strict=True)

    records: list[Feature]
    source_rows: int
    reported_count: int | None
    returned_count: int | None
    signatures: list[str]
    geometries: list[dict[str, Any] | None] | None
    layout_fingerprint: dict[str, Any]
    warnings: list[str]
    details: dict[str, Any]
    crs: dict[str, Any] | None
    bbox: list[float] | None
    next_link: str | None


class Geometry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    type: Literal["MultiPolygon"]
    coordinates: list[list[list[list[Number]]]]

    @field_validator("coordinates")
    @classmethod
    def positions(cls, value: list[list[list[list[float]]]]) -> list[list[list[list[float]]]]:
        for polygon in value:
            for ring in polygon:
                if len(ring) < 4 or ring[0] != ring[-1]:
                    raise ValueError("Invalid linear ring")
                if any(len(position) not in (2, 3) for position in ring):
                    raise ValueError("Invalid coordinate dimension")
        return value


class CRSProperties(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: Text


class CRS(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    type: Literal["name"]
    properties: CRSProperties


class ResponseFeature(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    type: Literal["Feature"]
    id: Text
    properties: dict[str, Any]
    geometry: Geometry | None
    geometry_name: Text | None = None
    bbox: list[Number] | None = None


class Link(BaseModel):
    model_config = ConfigDict(strict=True, extra="allow")

    rel: Text
    href: Text


class PageEnvelope(BaseModel):
    model_config = ConfigDict(strict=True, extra="allow")

    type: Literal["FeatureCollection"]
    features: list[dict[str, Any]]
    numberMatched: Integer | Literal["unknown"] | None = None
    totalFeatures: Integer | Literal["unknown"] | None = None
    numberReturned: Integer | None = None
    crs: CRS | None = None
    bbox: list[Number] | None = None
    next: Text | None = None
    timeStamp: Text | None = None
    links: list[Link] = Field(default_factory=list)

    @field_validator("numberMatched", "totalFeatures", "numberReturned")
    @classmethod
    def nonnegative_count(cls, value: int | str | None) -> int | str | None:
        if type(value) is int and value < 0:
            raise ValueError("Negative declared count")
        return value


PROPERTY_MODELS: dict[tuple[str, str], type[Properties]] = {
    ("PRODES", "Amazônia"): ProdesAmazonProperties,
    ("PRODES", "Cerrado"): ProdesCerradoProperties,
    ("PRODES", "Caatinga"): ProdesCaatingaProperties,
    ("PRODES", "Mata Atlântica"): ProdesMataAtlanticaProperties,
    ("PRODES", "Pantanal"): ProdesPantanalProperties,
    ("PRODES", "Pampa"): ProdesPampaProperties,
    ("DETER", "Amazônia"): DeterAmazonProperties,
    ("DETER", "Cerrado"): DeterCerradoProperties,
}


def layout_properties(product: str, biome: str) -> list[str]:
    return list(PROPERTY_MODELS[(product, biome)].model_fields)


def layout_geometry_column(product: str, biome: str) -> str:
    return "st_multi" if (product, biome) == ("DETER", "Cerrado") else "geom"
