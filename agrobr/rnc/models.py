from __future__ import annotations

import re
from datetime import date

import pandas as pd
import pydantic

from agrobr import constants

REGISTRADAS_RENAME: dict[str, str] = {
    "CULTIVAR": "cultivar",
    "NOME COMUM": "nome_comum",
    "NOME CIENTÍFICO": "nome_cientifico",
    "GRUPO DA ESPÉCIE": "grupo",
    "SITUAÇÃO": "situacao",
    "Nº FORMULÁRIO": "nr_formulario",
    "Nº REGISTRO": "nr_registro",
    "DATA DO REGISTRO": "data_registro",
    "DATA DE VALIDADE DO REGISTRO": "data_validade",
    "MANTENEDOR (REQUERENTE) (NOME)": "mantenedor",
}

PROTEGIDAS_RENAME: dict[str, str] = {
    "CULTIVAR": "cultivar",
    "NOME CIENTÍFICO": "nome_cientifico",
    "NOME COMUM": "nome_comum",
    "Nº PROCESSO": "nr_processo",
    "SITUAÇÃO": "situacao",
    "Nº CERTIFICADO": "nr_certificado",
    "INÍCIO DA PROTEÇÃO": "inicio_protecao",
    "TÉRMINO DA PROTEÇÃO": "termino_protecao",
    "TITULAR (NOME)": "titular",
    "REPRESENANTE LEGAL (NOME) ": "representante_legal",
    "MELHORISTAS": "melhoristas",
}

REGISTRADAS_COLS: list[str] = [
    "cultivar",
    "nome_comum",
    "nome_cientifico",
    "grupo",
    "situacao",
    "nr_formulario",
    "nr_registro",
    "data_registro",
    "data_validade",
    "mantenedor",
]

PROTEGIDAS_COLS: list[str] = [
    "cultivar",
    "nome_cientifico",
    "nome_comum",
    "nr_processo",
    "situacao",
    "nr_certificado",
    "inicio_protecao",
    "termino_protecao",
    "titular",
    "representante_legal",
    "melhoristas",
    "termino_protecao_texto",
]

DATE_COLS_REG: list[str] = ["data_registro", "data_validade"]
DATE_COLS_PROT: list[str] = ["inicio_protecao", "termino_protecao"]


def civil_date(value: object, *, conditional_end: bool = False) -> date | None:
    if not isinstance(value, str):
        raise ValueError("data publicada deve ser texto")
    text = value.strip()
    if not text or (conditional_end and text == constants.SNPC_CONDITIONAL_END):
        return None
    if not re.fullmatch(constants.RNC_DATE_PATTERN, text):
        raise ValueError("data publicada deve usar DD/MM/YYYY")
    day, month, year = (int(part) for part in text.split("/"))
    result = date(year, month, day)
    try:
        pd.Timestamp(result).as_unit("ns")
    except (OverflowError, ValueError) as exc:
        raise ValueError("data fora do domínio datetime64[ns]") from exc
    return result


class RncRecord(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid")

    @pydantic.field_validator("*", mode="before")
    @classmethod
    def strip_text(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


class RncRegistrada(RncRecord):
    cultivar: str
    nome_comum: str = pydantic.Field(min_length=1)
    nome_cientifico: str = pydantic.Field(min_length=1)
    grupo: str = pydantic.Field(min_length=1)
    situacao: str = pydantic.Field(min_length=1)
    nr_formulario: str
    nr_registro: str = pydantic.Field(min_length=1)
    data_registro: date | None
    data_validade: date | None
    mantenedor: str

    @pydantic.field_validator("data_registro", "data_validade", mode="before")
    @classmethod
    def parse_date(cls, value: object) -> date | None:
        return civil_date(value)


class SnpcProtegida(RncRecord):
    cultivar: str = pydantic.Field(min_length=1)
    nome_cientifico: str = pydantic.Field(min_length=1)
    nome_comum: str = pydantic.Field(min_length=1)
    nr_processo: str = pydantic.Field(min_length=1)
    situacao: str = pydantic.Field(min_length=1)
    nr_certificado: str = pydantic.Field(min_length=1)
    inicio_protecao: date | None
    termino_protecao: date | None
    titular: str = pydantic.Field(min_length=1)
    representante_legal: str = pydantic.Field(min_length=1)
    melhoristas: str
    termino_protecao_texto: str

    @pydantic.field_validator("inicio_protecao", mode="before")
    @classmethod
    def parse_start(cls, value: object) -> date | None:
        return civil_date(value)

    @pydantic.field_validator("termino_protecao", mode="before")
    @classmethod
    def parse_end(cls, value: object) -> date | None:
        return civil_date(value, conditional_end=True)

    @pydantic.model_validator(mode="after")
    def consistent_end(self) -> SnpcProtegida:
        expected = civil_date(self.termino_protecao_texto, conditional_end=True)
        if self.termino_protecao != expected:
            raise ValueError("data de término incompatível com o texto publicado")
        return self
