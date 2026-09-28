from __future__ import annotations

import re
from datetime import date, datetime

import pandas as pd
import pydantic

from agrobr import constants

PARSER_VERSION = 2
COLUNAS_SAIDA = [
    "cotacao_compra",
    "cotacao_venda",
    "data_hora",
    "data",
    "moeda",
    "paridade_compra",
    "paridade_venda",
    "tipo_boletim",
]
COLUNAS_MOEDAS = ["moeda", "nome", "tipo_moeda"]


def timestamp(value: str) -> pd.Timestamp:
    if re.fullmatch(constants.BCB_PTAX_TIMESTAMP_PATTERN, value) is None:
        raise ValueError("Horário PTAX deve ser civil sem fuso, com até nove casas decimais")
    datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")
    try:
        result = pd.Timestamp(value).as_unit("ns")
        result.normalize().as_unit("ns")
    except (ValueError, OverflowError) as exc:
        raise ValueError("Horário ou data civil PTAX fora do domínio ns") from exc
    return result


class PtaxObservation(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid", allow_inf_nan=False)

    cotacao_compra: float | None
    cotacao_venda: float | None
    data_hora: str
    data: date
    moeda: str
    paridade_compra: float | None
    paridade_venda: float | None
    tipo_boletim: str | None

    @pydantic.field_validator("moeda")
    @classmethod
    def valid_currency(cls, value: str) -> str:
        if re.fullmatch(constants.BCB_PTAX_CURRENCY_PATTERN, value) is None:
            raise ValueError("Símbolo PTAX deve conter três letras ASCII maiúsculas")
        return value

    @pydantic.model_validator(mode="after")
    def consistent_date(self) -> PtaxObservation:
        if timestamp(self.data_hora).date() != self.data:
            raise ValueError("Data civil PTAX diverge do horário publicado")
        return self

    @property
    def timestamp(self) -> pd.Timestamp:
        return timestamp(self.data_hora)


class PtaxCurrency(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid")

    moeda: str
    nome: str
    tipo_moeda: str

    @pydantic.field_validator("moeda")
    @classmethod
    def valid_currency(cls, value: str) -> str:
        if re.fullmatch(constants.BCB_PTAX_CURRENCY_PATTERN, value) is None:
            raise ValueError("Símbolo PTAX deve conter três letras ASCII maiúsculas")
        return value

    @pydantic.field_validator("nome", "tipo_moeda")
    @classmethod
    def nonempty_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Nome e tipo de moeda PTAX devem conter texto")
        return value


class _PtaxPage(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid")

    source_rows: int = pydantic.Field(ge=0)
    reported_count: int | None = pydantic.Field(default=None, ge=0)
    next_link: str | None = None
    layout_fingerprint: str | None
    parser_version: int = PARSER_VERSION
    warnings: list[str] = pydantic.Field(default_factory=list)


class PtaxQuotesPage(_PtaxPage):
    records: list[PtaxObservation]


class PtaxCurrenciesPage(_PtaxPage):
    records: list[PtaxCurrency]


def identity(record: PtaxObservation) -> tuple[str, pd.Timestamp, str | None]:
    return record.moeda, record.timestamp, record.tipo_boletim
