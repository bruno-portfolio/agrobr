from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializeAsAny,
    ValidationInfo,
    create_model,
    field_validator,
)

from agrobr import constants

from . import _json

Product = Literal["perfis", "mapa"]

ORDENS_SOLO: tuple[str, ...] = (
    "AFLORAMENTOS DE ROCHAS",
    "ARGISSOLOS",
    "CAMBISSOLOS",
    "CHERNOSSOLOS",
    "DUNAS",
    "ESPODOSSOLOS",
    "GLEISSOLOS",
    "LATOSSOLOS",
    "LUVISSOLOS",
    "NEOSSOLOS",
    "NITOSSOLOS",
    "ORGANOSSOLOS",
    "PLANOSSOLOS",
    "PLINTOSSOLOS",
    "VERTISSOLOS",
)

ORDENS_ALIASES: dict[str, str] = {
    **{ordem: ordem for ordem in ORDENS_SOLO},
    **{ordem.removesuffix("S"): ordem for ordem in ORDENS_SOLO if " " not in ordem},
    "AFLORAMENTO DE ROCHA": "AFLORAMENTOS DE ROCHAS",
    "AFLORAMENTO DE ROCHAS": "AFLORAMENTOS DE ROCHAS",
    "AFLORAMENTOS DE ROCHA": "AFLORAMENTOS DE ROCHAS",
}
WFS_BASE = constants.URLS[constants.Fonte.EMBRAPA_SOLOS]["geoserver"]
WFS_VERSION = constants.EMBRAPA_SOLOS_WFS_VERSION
PERFIS_NAMESPACE = MAPA_NAMESPACE = constants.EMBRAPA_SOLOS_NAMESPACE
PERFIS_LAYER = constants.EMBRAPA_SOLOS_LAYERS["perfis"]
MAPA_LAYER = constants.EMBRAPA_SOLOS_LAYERS["mapa"]
PERFIS_PAGE_SIZE = constants.EMBRAPA_SOLOS_DEFAULT_PAGE_SIZES["perfis"]
MAPA_PAGE_SIZE = constants.EMBRAPA_SOLOS_DEFAULT_PAGE_SIZES["mapa"]
PERFIS_MAX_FEATURES_GEO = constants.EMBRAPA_SOLOS_GEO_DEFAULT_MAX_RECORDS["perfis"]
MAPA_MAX_FEATURES_GEO = constants.EMBRAPA_SOLOS_GEO_DEFAULT_MAX_RECORDS["mapa"]
PERFIS_PROPERTY_NAMES = list(constants.EMBRAPA_SOLOS_PERFIS_PROPERTIES)
MAPA_PROPERTY_NAMES = list(constants.EMBRAPA_SOLOS_MAPA_PROPERTIES)
PERFIS_GEOM_COLUMN = constants.EMBRAPA_SOLOS_GEOMETRY_COLUMNS["perfis"]
MAPA_GEOM_COLUMN = constants.EMBRAPA_SOLOS_GEOMETRY_COLUMNS["mapa"]
PERFIS_PROPERTY_NAMES_GEO = [*PERFIS_PROPERTY_NAMES, PERFIS_GEOM_COLUMN]
MAPA_PROPERTY_NAMES_GEO = [*MAPA_PROPERTY_NAMES, MAPA_GEOM_COLUMN]
PERFIS_RENAME_MAP = constants.EMBRAPA_SOLOS_PERFIS_RENAME_MAP
MAPA_RENAME_MAP = constants.EMBRAPA_SOLOS_MAPA_RENAME_MAP
PERFIS_COLUNAS_SAIDA = list(constants.EMBRAPA_SOLOS_PERFIS_COLUMNS)
MAPA_COLUNAS_SAIDA = list(constants.EMBRAPA_SOLOS_MAPA_COLUMNS)
PERFIS_COLUNAS_SAIDA_GEO = [*PERFIS_COLUNAS_SAIDA, "geometry"]
MAPA_COLUNAS_SAIDA_GEO = [*MAPA_COLUNAS_SAIDA, "geometry"]
PERFIS_NUMERIC_COLS = frozenset({"latitude", "longitude"})


def layout_properties(product: Product) -> tuple[str, ...]:
    if product not in ("perfis", "mapa"):
        raise ValueError("Produto Embrapa Solos inválido")
    return (
        constants.EMBRAPA_SOLOS_PERFIS_PROPERTIES
        if product == "perfis"
        else constants.EMBRAPA_SOLOS_MAPA_PROPERTIES
    )


def layout_geometry_column(product: Product) -> str:
    layout_properties(product)
    return constants.EMBRAPA_SOLOS_GEOMETRY_COLUMNS[product]


class Properties(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    @field_validator("*", mode="before")
    @classmethod
    def source_value(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        if info.field_name in constants.EMBRAPA_SOLOS_INTEGER_BITS:
            return _json.integer(value, constants.EMBRAPA_SOLOS_INTEGER_BITS[info.field_name])
        if info.field_name in constants.EMBRAPA_SOLOS_FLOAT_PROPERTIES:
            return _json.floating(value)
        return value


def property_model(product: Product) -> type[Properties]:
    definitions: dict[str, Any] = {}
    for name in layout_properties(product):
        value_type: Any = (
            int
            if name in constants.EMBRAPA_SOLOS_INTEGER_BITS
            else float
            if name in constants.EMBRAPA_SOLOS_FLOAT_PROPERTIES
            else str
        )
        definitions[name] = (value_type if name in ("fid", "ogc_fid") else value_type | None, ...)
    return create_model(
        "PerfisProperties" if product == "perfis" else "MapaProperties",
        __base__=Properties,
        **definitions,
    )


PerfisProperties = property_model("perfis")
MapaProperties = property_model("mapa")


class Geometry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["Point", "MultiPolygon"]
    coordinates: list[Any]
    bbox: list[float] | None = None


class Feature(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    type: Literal["Feature"]
    id: Annotated[str, Field(min_length=1)]
    properties: SerializeAsAny[Properties]
    geometry: Geometry | None
    bbox: list[float] | None = None

    @field_validator("id")
    @classmethod
    def nonblank_identifier(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Feature.id textual não branco obrigatório")
        return value


class CRSProperties(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    name: str


class CRS(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["name"]
    properties: CRSProperties


class Link(BaseModel):
    model_config = ConfigDict(strict=True, extra="allow")
    rel: str
    href: str


class PageEnvelope(BaseModel):
    model_config = ConfigDict(strict=True, extra="allow")
    type: Literal["FeatureCollection"]
    features: list[dict[str, Any]]
    numberMatched: int | None = None
    numberReturned: int | None = None
    totalFeatures: int | None = None
    crs: CRS | None = None
    bbox: list[Any] | None = None
    next: str | None = None
    links: list[Link] = Field(default_factory=list)
    timeStamp: str | None = None

    @field_validator("numberMatched", "numberReturned", "totalFeatures", mode="before")
    @classmethod
    def counts(cls, value: Any) -> int:
        if isinstance(value, _json.Number) and value.lexeme == "-0":
            raise ValueError("Contagem exige inteiro canônico não negativo")
        result = _json.integer(value, 64)
        if result < 0:
            raise ValueError("Contagem negativa")
        return result


class ParsedPage(BaseModel):
    model_config = ConfigDict(strict=True, arbitrary_types_allowed=True)
    records: list[Feature]
    source_rows: int
    reported_count: int | None
    returned_count: int | None
    signatures: list[str]
    geometries: list[dict[str, Any] | None] | None
    layout_fingerprint: dict[str, Any]
    diagnostics: dict[str, Any]
    statistics: dict[str, Any]
    warnings: list[str]
    crs: dict[str, Any] | None
    bbox: list[float] | None
    next_link: str | None
    parser_version: int = 3
