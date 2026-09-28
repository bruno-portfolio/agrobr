from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

import pydantic

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.utils import time as time_utils
from agrobr.utils import validation


def validate_codigo(codigo: str) -> str:
    if not isinstance(codigo, str) or not re.fullmatch(r"[A-Za-z0-9]{4,8}", codigo.strip()):
        raise InvalidParameterError("codigo deve identificar uma estacao INMET")
    return codigo.strip().upper()


def validate_agregacao(agregacao: str) -> None:
    if agregacao not in ("horario", "diario"):
        raise InvalidParameterError("agregacao deve ser horario ou diario")


def validate_ano(ano: int) -> int:
    if (
        not isinstance(ano, int)
        or isinstance(ano, bool)
        or not constants.INMET_HISTORICO_MIN_ANO <= ano <= time_utils.hoje().year
    ):
        raise InvalidParameterError(
            f"ano deve estar entre {constants.INMET_HISTORICO_MIN_ANO} e {time_utils.hoje().year}"
        )
    return ano


def validate_periodo(inicio: str | date, fim: str | date) -> tuple[date, date]:
    values: list[date] = []
    for value in (inicio, fim):
        if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            try:
                value = date.fromisoformat(value)
            except ValueError as exc:
                raise InvalidParameterError(
                    "inicio e fim devem usar datas reais YYYY-MM-DD"
                ) from exc
        if not isinstance(value, date) or isinstance(value, datetime):
            raise InvalidParameterError("inicio e fim devem usar datas YYYY-MM-DD")
        values.append(value)
    if values[0] > values[1]:
        raise InvalidParameterError("inicio deve ser anterior ou igual a fim")
    return values[0], values[1]


def validate_uf(uf: str) -> str:
    if not isinstance(uf, str) or not uf.strip():
        raise InvalidParameterError("UF deve ser uma sigla brasileira")
    return validation.validate_uf(uf) or ""


class HistoricoEstacao(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(allow_inf_nan=False)

    codigo: str
    uf: str
    nome: str = pydantic.Field(min_length=1)
    regiao: str | None = None
    latitude: float | None = pydantic.Field(default=None, ge=-90, le=90)
    longitude: float | None = pydantic.Field(default=None, ge=-180, le=180)
    altitude: float | None = None
    fundacao: str | None = None

    @pydantic.field_validator("codigo")
    @classmethod
    def codigo_valido(cls, value: str) -> str:
        return validate_codigo(value)

    @pydantic.field_validator("uf")
    @classmethod
    def uf_valida(cls, value: str) -> str:
        return validate_uf(value)

    @pydantic.field_validator("latitude", "longitude", "altitude", mode="before")
    @classmethod
    def numero_metadata(cls, value: Any) -> Any:
        if value is None or value == "":
            return None
        return str(value).replace(",", ".")


def normalize_hora_utc(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("hora UTC inválida")
    normalized = value.strip().removesuffix(" UTC").replace(":", "")
    if not re.fullmatch(r"(?:[01][0-9]|2[0-3])00", normalized):
        raise ValueError("hora UTC inválida")
    return normalized


class HistoricoObservacao(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(allow_inf_nan=False)

    data: date
    hora_utc: str
    temperatura: float | None = None
    temperatura_max: float | None = None
    temperatura_min: float | None = None
    umidade: float | None = None
    umidade_max: float | None = None
    umidade_min: float | None = None
    precipitacao_mm: float | None = None
    pressao_hpa: float | None = None
    vento_ms: float | None = None
    vento_dir: float | None = None
    vento_rajada_ms: float | None = None
    radiacao_kj_m2: float | None = None
    ponto_orvalho: float | None = None

    @pydantic.field_validator("data", mode="before")
    @classmethod
    def data_valida(cls, value: Any) -> Any:
        return value.replace("/", "-") if isinstance(value, str) else value

    @pydantic.field_validator("hora_utc")
    @classmethod
    def hora_valida(cls, value: str) -> str:
        return normalize_hora_utc(value)

    @pydantic.field_validator(
        "temperatura",
        "temperatura_max",
        "temperatura_min",
        "umidade",
        "umidade_max",
        "umidade_min",
        "precipitacao_mm",
        "pressao_hpa",
        "vento_ms",
        "vento_dir",
        "vento_rajada_ms",
        "radiacao_kj_m2",
        "ponto_orvalho",
        mode="before",
    )
    @classmethod
    def numero_observacao(cls, value: Any) -> float | None:
        if value is None or isinstance(value, str) and not value.strip():
            return None
        number = float(str(value).strip().replace(",", "."))
        return None if number == -9999.0 else number


class APIObservationIdentity(pydantic.BaseModel):
    codigo: str = pydantic.Field(alias="CD_ESTACAO")
    data: date = pydantic.Field(alias="DT_MEDICAO")
    uf: str | None = pydantic.Field(default=None, alias="UF")

    @pydantic.field_validator("codigo")
    @classmethod
    def codigo_valido(cls, value: str) -> str:
        return validate_codigo(value)

    @pydantic.field_validator("data", mode="before")
    @classmethod
    def data_valida(cls, value: Any) -> Any:
        if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError("data de observacao invalida")
        return value

    @pydantic.field_validator("uf")
    @classmethod
    def uf_valida(cls, value: str | None) -> str | None:
        return validate_uf(value) if value is not None else None
