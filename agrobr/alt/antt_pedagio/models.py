from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from typing import Any, Literal

import pandas as pd
import pydantic
from pydantic_core import PydanticCustomError

from agrobr import constants
from agrobr.constants import URLS, Fonte
from agrobr.normalize.regions import UFS_VALIDAS as UFS_VALIDAS
from agrobr.utils.time import hoje

CKAN_BASE = URLS[Fonte.ANTT_PEDAGIO]["base"]
CKAN_API = f"{CKAN_BASE}/api/3/action"

DATASET_TRAFEGO_SLUG = "volume-trafego-praca-pedagio"
DATASET_PRACAS_SLUG = "praca-de-pedagio"

ANO_INICIO = 2010

_EXPLICIT_AXLES = re.compile(constants.ANTT_EXPLICIT_AXLES_PATTERN)
_EXPLICIT_TYPE = re.compile(
    r"ve[ií]culo\s+(comercial|passeio)\s+(?:acima\s+de\s+)?[0-9]+\s+eixos?", re.IGNORECASE
)
_VOLUME = re.compile(r"[+]?[0-9]+(?:[,.][0-9]+)?")
_NEGATIVE = re.compile(r"-[0-9]+(?:[,.][0-9]+)?")
UNCOUNTABLE_VOLUME = "volume_incontavel"
_DATE = re.compile(r"(?:[0-9]{2}/)?[0-9]{2}/[0-9]{4}")


def parse_reference(value: str, frequencia: Literal["mensal", "diaria"]) -> date:
    if not _DATE.fullmatch(value):
        raise ValueError("data deve ser MM/AAAA ou DD/MM/AAAA literal")
    pieces = value.split("/")
    if len(pieces) == 2:
        if frequencia == "diaria":
            raise ValueError("data diária exige dia civil")
        month, year = map(int, pieces)
        return date(year, month, 1)
    day, month, year = map(int, pieces)
    parsed = date(year, month, day)
    return parsed.replace(day=1) if frequencia == "mensal" else parsed


def explicit_axles(value: str | None) -> int | None:
    match = _EXPLICIT_AXLES.fullmatch(value.strip()) if value is not None else None
    if match is None:
        return None
    number = int(match[1])
    if not 1 <= number <= constants.ANTT_INT64_MAX:
        raise ValueError("contagem explícita de eixos fora de Int64 positivo")
    return number


def explicit_vehicle_type(value: str | None) -> str | None:
    if value is None:
        return None
    if value.strip().casefold() == "moto":
        return "Moto"
    match = _EXPLICIT_TYPE.fullmatch(value.strip())
    return match[1].capitalize() if match else None


class TrafegoRecord(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, str_max_length=constants.ANTT_MAX_FIELD_CHARS
    )

    source_record: pydantic.StrictInt = pydantic.Field(ge=1)
    frequencia: Literal["mensal", "diaria"]
    data: date
    concessionaria: pydantic.StrictStr
    praca: pydantic.StrictStr
    sentido: pydantic.StrictStr | None = None
    categoria_eixo: pydantic.StrictStr | None = None
    tipo_veiculo: pydantic.StrictStr | None = None
    tipo_cobranca: pydantic.StrictStr | None = None
    volume: pydantic.StrictInt = pydantic.Field(ge=0, le=constants.ANTT_INT64_MAX)
    n_eixos: pydantic.StrictInt | None = pydantic.Field(
        default=None, ge=1, le=constants.ANTT_INT64_MAX
    )

    @pydantic.field_validator("data", mode="before")
    @classmethod
    def reference(cls, value: Any, info: pydantic.ValidationInfo) -> date:
        frequency = info.data.get("frequencia")
        if frequency not in ("mensal", "diaria"):
            raise ValueError("frequência inválida")
        if not isinstance(value, str):
            raise ValueError("data deve ser texto civil")
        parsed = parse_reference(value, frequency)
        expected_year = (info.context or {}).get("ano")
        if expected_year is not None and parsed.year != expected_year:
            raise ValueError(f"ano {parsed.year} difere do recurso {expected_year}")
        if not date(1678, 1, 1) <= parsed <= date(2261, 12, 31):
            raise ValueError("data fora do intervalo datetime64[ns] civil suportado")
        return parsed

    @pydantic.field_validator("volume", mode="before")
    @classmethod
    def exact_volume(cls, value: Any) -> int:
        if type(value) is int:
            return value
        if isinstance(value, str) and _NEGATIVE.fullmatch(value):
            raise PydanticCustomError(UNCOUNTABLE_VOLUME, "volume negativo não é contagem")
        if (
            not isinstance(value, str)
            or len(value) > constants.ANTT_MAX_FIELD_CHARS
            or not _VOLUME.fullmatch(value)
        ):
            raise ValueError("volume exige contagem decimal não negativa; vazio não é zero")
        number = Decimal(value.replace(",", "."))
        if number != number.to_integral_value():
            raise PydanticCustomError(UNCOUNTABLE_VOLUME, "volume fracionário não é contagem")
        return int(number)

    @pydantic.field_validator(
        "concessionaria", "praca", "sentido", "categoria_eixo", "tipo_veiculo", "tipo_cobranca"
    )
    @classmethod
    def present_text(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("campo textual presente vazio; ausência estrutural deve ser explícita")
        return value

    @pydantic.model_validator(mode="after")
    def derivations(self) -> TrafegoRecord:
        object.__setattr__(self, "n_eixos", explicit_axles(self.categoria_eixo))
        if self.tipo_veiculo is None:
            object.__setattr__(self, "tipo_veiculo", explicit_vehicle_type(self.categoria_eixo))
        return self


class ParsedTraffic(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    frame: pd.DataFrame = pydantic.Field(exclude=True)
    diagnostics: dict[str, Any]


class PracaRecord(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", str_max_length=constants.ANTT_MAX_FIELD_CHARS
    )

    concessionaria: pydantic.StrictStr
    praca_de_pedagio: pydantic.StrictStr
    ano_do_pnv_snv: pydantic.StrictStr | None = None
    rodovia: pydantic.StrictStr | None = None
    uf: pydantic.StrictStr | None = None
    km_m: pydantic.StrictStr | None = None
    municipal: pydantic.StrictStr | None = None
    municipio: pydantic.StrictStr | None = None
    tipo_de_pista: pydantic.StrictStr | None = None
    sentido: pydantic.StrictStr | None = None
    situacao: pydantic.StrictStr | None = None
    data_da_inativacao: pydantic.StrictStr | None = None
    lat: float | None = pydantic.Field(default=None, ge=-35, le=6, allow_inf_nan=False)
    lon: float | None = pydantic.Field(default=None, ge=-74, le=-30, allow_inf_nan=False)

    @pydantic.field_validator("uf")
    @classmethod
    def state(cls, value: str | None) -> str | None:
        if value is None or not value.strip():
            return None
        normalized = value.strip().upper()
        if normalized not in UFS_VALIDAS:
            raise ValueError("UF inválida")
        return normalized

    @pydantic.field_validator("lat", "lon", mode="before")
    @classmethod
    def coordinate(cls, value: Any) -> float | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not re.fullmatch(r"[-+]?[0-9]+(?:[.,][0-9]+)?", value):
            raise ValueError("coordenada decimal inválida")
        number = Decimal(value.replace(",", "."))
        return float(number)


def _resolve_anos(
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
) -> list[int]:
    if ano is not None:
        return [ano]

    if ano_inicio is not None or ano_fim is not None:
        start = ano_inicio or ANO_INICIO
        end = ano_fim or hoje().year
        return list(range(start, end + 1))

    current = hoje().year
    return [current - 1, current]


def build_ckan_package_url(slug: str) -> str:
    return f"{CKAN_API}/package_show?id={slug}"
