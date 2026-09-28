from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from agrobr import constants

from . import _json

WFS_BASE = constants.URLS[constants.Fonte.FUNAI]["geoserver"]
WFS_VERSION = constants.FUNAI_WFS_VERSION
LAYER = constants.FUNAI_LAYER
NAMESPACE = constants.FUNAI_NAMESPACE
GEOM_COLUMN = constants.FUNAI_GEOM_COLUMN
MAX_FEATURES_GEO = constants.FUNAI_GEO_DEFAULT_MAX_RECORDS
MAX_FEATURES_TABULAR = constants.FUNAI_DEFAULT_MAX_RECORDS
PROPERTY_NAMES = list(constants.FUNAI_PROPERTIES)
PROPERTY_NAMES_GEO = [GEOM_COLUMN, *PROPERTY_NAMES]
RENAME_MAP = constants.FUNAI_RENAME_MAP
COLUNAS_SAIDA = list(constants.FUNAI_COLUMNS)
COLUNAS_SAIDA_GEO = [*COLUNAS_SAIDA, "geometry"]
FASES_VALIDAS = constants.FUNAI_FASES_VALIDAS


def layout_properties() -> tuple[str, ...]:
    return constants.FUNAI_PROPERTIES


def layout_geometry_column() -> str:
    return constants.FUNAI_GEOM_COLUMN


class Properties(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    gid: int | None
    terrai_codigo: int | None
    terrai_nome: str | None
    etnia_nome: str | None
    municipio_nome: str | None
    uf_sigla: str | None
    superficie_perimetro_ha: float | None
    fase_ti: str | None
    modalidade_ti: str | None
    reestudo_ti: str | None
    cr: str | None
    faixa_fronteira: str | None
    undadm_codigo: int | None
    undadm_nome: str | None
    undadm_sigla: str | None
    dominio_uniao: str | None
    data_atualizacao: str | None
    epsg: int | None

    @field_validator("*", mode="before")
    @classmethod
    def published_value(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        if info.field_name in constants.FUNAI_INTEGER_BITS:
            return _json.integer(value, constants.FUNAI_INTEGER_BITS[info.field_name])
        if info.field_name == "superficie_perimetro_ha":
            return _json.floating(value)
        return value


class Geometry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["Polygon", "MultiPolygon"]
    coordinates: list[Any]
    bbox: list[float] | None = None


class Feature(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["Feature"]
    id: str
    properties: Properties
    geometry: Geometry | None
    geometry_name: str | None = None
    bbox: list[float] | None = None

    @field_validator("id")
    @classmethod
    def identifier(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Feature.id vazio")
        return value

    @field_validator("geometry_name")
    @classmethod
    def geometry_column(cls, value: str | None) -> str | None:
        if value is not None and value != constants.FUNAI_GEOM_COLUMN:
            raise ValueError("Nome geométrico não corresponde à camada")
        return value


class CRSProperties(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    name: str


class CRS(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["name"]
    properties: CRSProperties


class Link(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    rel: str
    href: str
    title: str | None = None
    type: str | None = None


class PageEnvelope(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["FeatureCollection"]
    features: list[dict[str, Any]]
    numberMatched: int | None = None
    numberReturned: int | None = None
    totalFeatures: int | None = None
    crs: CRS | None = None
    bbox: list[Any] | None = None
    timeStamp: str | None = None
    links: list[Link] = Field(default_factory=list)
    next: str | None = None

    @field_validator("numberMatched", "numberReturned", "totalFeatures", mode="before")
    @classmethod
    def count(cls, value: Any) -> int:
        return _json.count(value)


class ParsedPage(BaseModel):
    model_config = ConfigDict(strict=True, arbitrary_types_allowed=True)
    records: list[Feature]
    source_rows: int
    reported_count: int | None
    returned_count: int | None
    signatures: list[str]
    identifier_signatures: list[str]
    geometries: list[dict[str, Any] | None] | None
    layout_fingerprint: dict[str, Any]
    diagnostics: dict[str, Any]
    statistics: dict[str, Any]
    warnings: list[str]
    crs: dict[str, Any] | None
    bbox: list[float] | None
    next_link: str | None
    source_timestamp: str | None = None
    parser_version: int = 3
