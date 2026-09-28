from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from agrobr import constants

from . import _json, _temporal

WFS_BASE = constants.URLS[constants.Fonte.INCRA]["geoserver"]
WFS_VERSION = constants.INCRA_WFS_VERSION
LAYER = constants.INCRA_LAYER
NAMESPACE = constants.INCRA_NAMESPACE
GEOM_COLUMN = constants.INCRA_GEOM_COLUMN
MAX_FEATURES_GEO = constants.INCRA_GEO_DEFAULT_MAX_RECORDS
MAX_FEATURES_TABULAR = constants.INCRA_DEFAULT_MAX_RECORDS
PROPERTY_NAMES = list(constants.INCRA_PROPERTIES)
PROPERTY_NAMES_GEO = [GEOM_COLUMN, *PROPERTY_NAMES]
RENAME_MAP = constants.INCRA_RENAME_MAP
COLUNAS_SAIDA = list(constants.INCRA_COLUMNS)
COLUNAS_SAIDA_GEO = [*COLUNAS_SAIDA, "geometry"]
FASES_VALIDAS = constants.INCRA_FASES_VALIDAS


def layout_properties() -> tuple[str, ...]:
    return constants.INCRA_PROPERTIES


def layout_geometry_column() -> str:
    return constants.INCRA_GEOM_COLUMN


class Properties(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    co_sr: str | None
    nu_processo: str | None
    no_comunidade: str | None
    no_municipio: str | None
    sg_uf: str | None
    dt_publica: str | None
    dt_public1: str | None
    nu_familia: int | None
    dt_titulo: str | None
    nu_area_ha: float | None
    no_responsavel: str | None
    no_esfera: str | None
    dt_cadastro: str
    cd_quilomb: int | None
    cd_sipra: str | None
    ds_descricao: str | None
    st_titulad: str | None
    dt_decreto: str | None
    tp_levanta: str | None
    nr_escalao: str | None
    ds_fase: str | None

    @field_validator("*", mode="before")
    @classmethod
    def published_value(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        if info.field_name in constants.INCRA_INTEGER_BITS:
            return _json.integer(value, constants.INCRA_INTEGER_BITS[info.field_name])
        if info.field_name == "nu_area_ha":
            return _json.floating(value)
        if info.field_name in constants.INCRA_DATE_PROPERTIES:
            return _temporal.validate_date(value)
        if info.field_name in constants.INCRA_DATETIME_PROPERTIES:
            return _temporal.validate_datetime(value)
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
        if value is not None and value != constants.INCRA_GEOM_COLUMN:
            raise ValueError("Nome geométrico não corresponde à camada")
        return value


class CRSProperties(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    name: str

    @field_validator("name")
    @classmethod
    def nonempty_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Declaração CRS vazia")
        return value


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
    numberMatched: int
    numberReturned: int
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
    reported_count: int
    returned_count: int
    signatures: list[str]
    identifier_signatures: list[str]
    sort_keys: list[tuple[int | None, str | None, str | None]]
    geometries: list[dict[str, Any] | None] | None
    layout_fingerprint: dict[str, Any]
    diagnostics: dict[str, Any]
    statistics: dict[str, Any]
    warnings: list[str]
    crs: dict[str, Any] | None
    bbox: list[float] | None
    next_link: str | None
    source_timestamp: str | None = None
    parser_version: int = 2
