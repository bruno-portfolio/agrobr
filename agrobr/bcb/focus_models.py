from __future__ import annotations

import re
from datetime import date
from typing import Literal

import pydantic

from agrobr import constants

PARSER_VERSION = 2
COLUNAS_SAIDA = [
    "indicador",
    "data",
    "data_referencia",
    "media",
    "mediana",
    "desvio_padrao",
    "minimo",
    "maximo",
    "numero_respondentes",
    "base_calculo",
    "periodicidade",
    "indicador_detalhe",
]


class FocusObservation(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid", allow_inf_nan=False)

    indicador: str
    data: date
    data_referencia: str
    media: float | None
    mediana: float | None
    desvio_padrao: float | None
    minimo: float | None
    maximo: float | None
    numero_respondentes: int | None = pydantic.Field(ge=0, le=2**31 - 1)
    base_calculo: int | None = pydantic.Field(ge=0, le=2**31 - 1)
    periodicidade: Literal["anual", "mensal"]
    indicador_detalhe: str | None

    @pydantic.field_validator("indicador")
    @classmethod
    def nonempty_indicator(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("indicador deve conter texto")
        return value

    @pydantic.model_validator(mode="after")
    def valid_reference(self) -> FocusObservation:
        pattern = (
            constants.BCB_FOCUS_ANNUAL_REFERENCE_PATTERN
            if self.periodicidade == "anual"
            else constants.BCB_FOCUS_MONTHLY_REFERENCE_PATTERN
        )
        if re.fullmatch(pattern, self.data_referencia) is None:
            raise ValueError("Formato de referência incompatível com a periodicidade")
        if self.periodicidade == "anual":
            date(int(self.data_referencia), 1, 1)
        else:
            month, year = self.data_referencia.split("/")
            date(int(year), int(month), 1)
        return self


class FocusParsedPage(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid")

    records: list[FocusObservation]
    source_rows: int = pydantic.Field(ge=0)
    reported_count: int | None = pydantic.Field(default=None, ge=0)
    next_link: str | None = None
    layout_fingerprint: str | None
    parser_version: int = PARSER_VERSION
    warnings: list[str] = pydantic.Field(default_factory=list)


def identity(
    record: FocusObservation,
) -> tuple[str, str, str | None, date, str, int | None]:
    return (
        record.periodicidade,
        record.indicador,
        record.indicador_detalhe,
        record.data,
        record.data_referencia,
        record.base_calculo,
    )
