from __future__ import annotations

import math
import re
from datetime import UTC, date, datetime
from typing import Any, Literal, Self

import pydantic

from agrobr.constants import SICAR_STATUS_VALIDOS, SICAR_TIPO_VALIDOS, URLS, Fonte
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.normalize import municipalities, regions
from agrobr.normalize.regions import UFS_VALIDAS as UFS_VALIDAS
from agrobr.utils.validation import validate_uf

WFS_BASE = URLS[Fonte.SICAR]["geoserver"]
WFS_VERSION = "2.0.0"

PAGE_SIZE = 10_000
MAX_FEATURES_WARNING = 100_000


def layer_name(uf: str) -> str:
    return f"sicar_imoveis_{uf.lower()}"


PROPERTY_NAMES = [
    "cod_imovel",
    "status_imovel",
    "dat_criacao",
    "area",
    "condicao",
    "uf",
    "municipio",
    "cod_municipio_ibge",
    "m_fiscal",
    "tipo_imovel",
]

RENAME_MAP = {
    "status_imovel": "status",
    "dat_criacao": "data_criacao",
    "area": "area_ha",
    "m_fiscal": "modulos_fiscais",
    "tipo_imovel": "tipo",
}

COLUNAS_IMOVEIS = [
    "cod_imovel",
    "status",
    "data_criacao",
    "data_atualizacao",
    "area_ha",
    "condicao",
    "uf",
    "municipio",
    "cod_municipio_ibge",
    "modulos_fiscais",
    "tipo",
    "cod_municipio",
]

STATUS_VALIDOS = SICAR_STATUS_VALIDOS

TIPO_VALIDOS = SICAR_TIPO_VALIDOS

# dat_criacao tem cobertura nacional, mas o campo data_atualizacao nao existe
# nestes layers estaduais (causa "400 Bad Request" / ServiceException no WFS).
UFS_SEM_DATA_ATUALIZACAO = frozenset(
    {"PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"}
)

MAX_FEATURES_GEO = 5_000

SICAR_GEOM_COLUMN = "geo_area_imovel"

SICAR_CRS = "EPSG:4326"
SICAR_CRS_NAMES = frozenset({"EPSG:4326", "urn:ogc:def:crs:EPSG::4326"})

PROPERTY_NAMES_GEO = [SICAR_GEOM_COLUMN] + PROPERTY_NAMES

COLUNAS_IMOVEIS_GEO = COLUNAS_IMOVEIS + ["geometry"]

PARSER_VERSION = 2


def property_names(uf: str, *, geo: bool = False) -> list[str]:
    names = list(PROPERTY_NAMES_GEO if geo else PROPERTY_NAMES)
    if uf.strip().upper() not in UFS_SEM_DATA_ATUALIZACAO:
        names.append("data_atualizacao")
    return names


def _validate_date(value: str | None, name: str, *, allow_time: bool = False) -> None:
    if value is None:
        return
    pattern = r"\d{4}-\d{2}-\d{2}"
    if allow_time:
        pattern += r"(T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.\d+)?(Z|[+-]\d{2}:\d{2})?)?"
    if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
        raise InvalidParameterError(f"{name} invalido: {value!r}")
    fraction = re.search(r"T\d{2}:\d{2}:\d{2}\.(\d+)", value) if allow_time else None
    if fraction and any(digit != "0" for digit in fraction[1][3:]):
        raise InvalidParameterError(f"{name} suporta apenas precisao de milissegundos")
    try:
        datetime.fromisoformat(value) if allow_time else date.fromisoformat(value)
    except ValueError as exc:
        raise InvalidParameterError(f"{name} invalido: {value!r}") from exc


def normalize_updated_after(value: str) -> str:
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    parsed = parsed.astimezone(UTC)
    precision = "milliseconds" if parsed.microsecond else "seconds"
    return parsed.isoformat(timespec=precision).replace("+00:00", "Z")


def validate_cql_filters(
    *,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    atualizado_apos: str | None = None,
) -> None:
    for name, value, allowed in (("Status", status, STATUS_VALIDOS), ("Tipo", tipo, TIPO_VALIDOS)):
        if value is not None and (not isinstance(value, str) or value.upper() not in allowed):
            raise InvalidParameterError(f"{name} '{value}' invalido. Opcoes: {sorted(allowed)}")
    for name, bound in (("area_min", area_min), ("area_max", area_max)):
        if bound is not None and (
            isinstance(bound, bool) or not isinstance(bound, (int, float)) or bound < 0
        ):
            raise InvalidParameterError(f"{name} deve ser numero finito nao negativo")
        if bound is not None:
            try:
                finite = math.isfinite(bound)
            except OverflowError as exc:
                raise InvalidParameterError(
                    f"{name} excede o intervalo numerico suportado"
                ) from exc
            if not finite:
                raise InvalidParameterError(f"{name} deve ser numero finito nao negativo")
    if area_min is not None and area_max is not None and area_min > area_max:
        raise InvalidParameterError("area_min deve ser menor ou igual a area_max")
    _validate_date(criado_apos, "criado_apos")
    _validate_date(atualizado_apos, "atualizado_apos", allow_time=True)


def validate_filters(
    uf: str,
    *,
    municipio: int | str | None = None,
    status: str | None = None,
    tipo: str | None = None,
) -> tuple[str, int | None]:
    """Devolve a UF e o código IBGE do município, que é o filtro enviado à camada."""
    uf_upper = regions.sigla_uf(uf)
    validate_cql_filters(status=status, tipo=tipo)
    if municipio is None:
        return uf_upper, None
    return uf_upper, municipalities.resolver_municipio(municipio, uf_upper)["codigo_ibge"]


def validate_max_registros(max_registros: int | None) -> None:
    if max_registros is not None and (
        isinstance(max_registros, bool) or not isinstance(max_registros, int) or max_registros <= 0
    ):
        raise InvalidParameterError("max_registros deve ser inteiro positivo ou None")


class SicarImovel(pydantic.BaseModel):
    cod_imovel: str = ""
    status_imovel: str = ""
    dat_criacao: datetime | None = None
    data_atualizacao: datetime | None = None
    area: float | None = pydantic.Field(default=None, ge=0, allow_inf_nan=False)
    condicao: str | None = None
    uf: str = ""
    municipio: str = ""
    cod_municipio_ibge: int | None = pydantic.Field(default=None, ge=1_000_000, le=9_999_999)
    m_fiscal: float | None = pydantic.Field(default=None, ge=0, allow_inf_nan=False)
    tipo_imovel: str = ""

    @pydantic.field_validator(
        "cod_imovel", "status_imovel", "uf", "municipio", "tipo_imovel", mode="before"
    )
    @classmethod
    def strip_text(cls, value: Any) -> Any:
        return value.strip() if isinstance(value, str) else "" if value is None else value

    @pydantic.field_validator("condicao", mode="before")
    @classmethod
    def strip_optional_text(cls, value: Any) -> Any:
        return value.strip() if isinstance(value, str) else value

    @pydantic.field_validator("status_imovel", "uf", "tipo_imovel")
    @classmethod
    def uppercase(cls, value: str) -> str:
        return value.upper()

    @pydantic.field_validator("area", "m_fiscal", "cod_municipio_ibge", mode="before")
    @classmethod
    def normalize_numeric(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("Valor numerico nao pode ser booleano")
        if isinstance(value, str):
            return value.replace(",", ".") if value.strip() else None
        return value

    @pydantic.field_validator("dat_criacao", "data_atualizacao", mode="before")
    @classmethod
    def validate_datetime_input(cls, value: Any) -> Any:
        if value == "" or value is None:
            return None
        if not isinstance(value, (str, datetime)):
            raise ValueError("Data deve ser texto ISO ou datetime")
        return datetime.fromisoformat(value) if isinstance(value, str) else value

    @pydantic.model_validator(mode="after")
    def validate_dimensions(self) -> Self:
        if self.uf:
            self.uf = validate_uf(self.uf)
        if self.status_imovel and self.status_imovel not in STATUS_VALIDOS:
            raise ValueError("Status invalido")
        if self.tipo_imovel and self.tipo_imovel not in TIPO_VALIDOS:
            raise ValueError("Tipo invalido")
        if (
            self.cod_municipio_ibge is not None
            and self.uf
            and self.cod_municipio_ibge // 100_000 != regions.uf_para_ibge(self.uf)
        ):
            raise ValueError("Codigo municipal incompativel com UF")
        return self


class SicarFeature(pydantic.BaseModel):
    id: str = pydantic.Field(pattern=r"^sicar_imoveis_[a-z]{2}\.[0-9]+$", strict=True)
    type: Literal["Feature"]
    properties: dict[str, Any]

    @pydantic.field_validator("properties")
    @classmethod
    def validate_properties(cls, value: dict[str, Any]) -> dict[str, Any]:
        required = {"cod_imovel", "status_imovel", "dat_criacao", "area", "uf"}
        if missing := required - value.keys():
            raise ValueError(f"Propriedades obrigatorias ausentes: {sorted(missing)}")
        identifier = value["cod_imovel"]
        if not isinstance(identifier, str) or not identifier.strip():
            raise ValueError("cod_imovel nao pode ser vazio")
        record = SicarImovel.model_validate(value)
        for name in ("status_imovel", "tipo_imovel", "uf", "municipio"):
            if not getattr(record, name):
                raise ValueError(f"{name} nao pode ser vazio")
        if record.cod_municipio_ibge is None:
            raise ValueError("cod_municipio_ibge nao pode ser ausente")
        return value


class SicarCRS(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    type: Literal["name"]
    properties: dict[str, str]


class SicarFeatureCollection(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    type: Literal["FeatureCollection"]
    features: list[SicarFeature]
    numberReturned: int = pydantic.Field(ge=0, strict=True)
    numberMatched: int | Literal["unknown"] | None = None
    crs: SicarCRS | None = None

    @pydantic.model_validator(mode="after")
    def validate_count(self) -> Self:
        if self.numberReturned != len(self.features):
            raise ValueError("numberReturned diverge da quantidade de features")
        return self


def validate_feature_ids(features: list[SicarFeature], seen: set[str]) -> None:
    for feature in features:
        if feature.id in seen:
            raise ParseError(
                source="sicar",
                parser_version=PARSER_VERSION,
                reason=(
                    f"Varredura inconsistente: id de feature repetido {feature.id}; "
                    "a fonte pode ter sido atualizada durante a consulta, repita a consulta"
                ),
            )
        seen.add(feature.id)
