from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from agrobr import constants
from agrobr.comexstat import _numbers
from agrobr.exceptions import InvalidParameterError


class ExternalRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    @model_validator(mode="before")
    @classmethod
    def literal_size(cls, value: object) -> object:
        if not isinstance(value, dict) or any(
            not isinstance(token, str) or len(token) > constants.COMEXSTAT_MAX_FIELD_CHARS
            for token in value.values()
        ):
            raise ValueError("campo externo deve ser texto dentro do limite lexical")
        return value


class ExportRecord(ExternalRecord):
    ano: int = Field(alias="CO_ANO", ge=1997, le=9999)
    mes: int = Field(alias="CO_MES", ge=1, le=12)
    ncm: str = Field(alias="CO_NCM")
    cod_unidade: str = Field(alias="CO_UNID")
    cod_pais: str = Field(alias="CO_PAIS")
    uf: str = Field(alias="SG_UF_NCM", pattern=r"^[A-Z]{2}$")
    cod_via: str = Field(alias="CO_VIA")
    cod_urf: str = Field(alias="CO_URF")
    qtd_estatistica: int | None = Field(alias="QT_ESTAT")
    kg_liquido: int | None = Field(alias="KG_LIQUIDO")
    valor_fob_usd: Decimal | None = Field(alias="VL_FOB")

    @field_validator("ano", "mes", "qtd_estatistica", "kg_liquido", mode="before")
    @classmethod
    def integer(cls, value: object, info: ValidationInfo) -> int | None:
        return _numbers.integer_token(
            value, nullable=info.field_name in ("qtd_estatistica", "kg_liquido")
        )

    @field_validator(
        "valor_fob_usd", "valor_frete_usd", "valor_seguro_usd", mode="before", check_fields=False
    )
    @classmethod
    def money(cls, value: object) -> Decimal | None:
        return _numbers.money_token(value)

    @field_validator("ncm", "cod_unidade", "cod_pais", "cod_via", "cod_urf")
    @classmethod
    def code(cls, value: str, info: ValidationInfo) -> str:
        alias = cls.model_fields[info.field_name or ""].alias
        width = constants.COMEXSTAT_CODE_WIDTHS[str(alias)]
        if not re.fullmatch(rf"[0-9]{{{width}}}", value):
            raise ValueError(f"código publicado exige {width} dígitos ASCII, sem reparo")
        return value


class ImportRecord(ExportRecord):
    valor_frete_usd: Decimal | None = Field(alias="VL_FRETE")
    valor_seguro_usd: Decimal | None = Field(alias="VL_SEGURO")


class DictionaryRecord(ExternalRecord):
    @field_validator("cod_unidade", "cod_pais", "cod_via", "cod_urf", check_fields=False)
    @classmethod
    def code(cls, value: str, info: ValidationInfo) -> str:
        alias = cls.model_fields[info.field_name or ""].alias
        width = constants.COMEXSTAT_CODE_WIDTHS[str(alias)]
        if not re.fullmatch(rf"[0-9]{{{width}}}", value):
            raise ValueError(f"código publicado exige {width} dígitos ASCII")
        return value


class UnitRecord(DictionaryRecord):
    cod_unidade: str = Field(alias="CO_UNID")
    unidade: str = Field(alias="NO_UNID")
    sigla_unidade: str = Field(alias="SG_UNID")


class CountryRecord(DictionaryRecord):
    cod_pais: str = Field(alias="CO_PAIS")
    cod_pais_iso_numerico: str = Field(alias="CO_PAIS_ISON3")
    cod_pais_iso_alfa3: str = Field(alias="CO_PAIS_ISOA3")
    pais: str = Field(alias="NO_PAIS")
    pais_ingles: str = Field(alias="NO_PAIS_ING")
    pais_espanhol: str = Field(alias="NO_PAIS_ESP")


class ViaRecord(DictionaryRecord):
    cod_via: str = Field(alias="CO_VIA")
    via: str = Field(alias="NO_VIA")


class CustomsRecord(DictionaryRecord):
    cod_urf: str = Field(alias="CO_URF")
    urf: str = Field(alias="NO_URF")


DICTIONARY_MODELS: dict[str, type[DictionaryRecord]] = {
    "unidades": UnitRecord,
    "paises": CountryRecord,
    "vias": ViaRecord,
    "urfs": CustomsRecord,
}


@dataclass(frozen=True)
class ParsedResource:
    frame: pd.DataFrame
    details: dict[str, Any]


@dataclass(frozen=True)
class SelecaoNcm:
    prefixos: tuple[str, ...]
    excluidos: tuple[str, ...] = ()
    primeiro_ano: int = 1997
    motivo_primeiro_ano: str = ""

    def seleciona(self, ncm: str) -> bool:
        return ncm.startswith(self.prefixos) and not ncm.startswith(self.excluidos)

    @property
    def codigo_unico(self) -> bool:
        return len(self.prefixos) == 1 and len(self.prefixos[0]) == 8


_SUPERFOSFATOS_ANTES_DE_2017 = (
    "a NCM separa superfosfatos pelo teor de 35 % de P2O5 só desde 2017 (31031100 e 31031900); "
    "até 2016 os códigos 31031010, 31031020 e 31031030 cortavam em 22 % e 45 %, sem equivalente "
    "exato. Para anos anteriores, informe o prefixo NCM '310310'"
)

_ESPECIES_DE_CAFE = ("cafe_arabica", "cafe_conilon")

_DOMISSANITARIOS = (
    "38081010",
    "38082010",
    "38083010",
    "38083031",
    "38083040",
    "38084010",
    "38085010",
    "38085910",
    "38089010",
    "38089110",
    "38089111",
    "38089119",
    "38089210",
    "38089211",
    "38089219",
    "38089310",
    "38089311",
    "38089319",
    "38089332",
    "38089340",
    "38089341",
    "38089349",
    "38089411",
    "38089419",
    "38089910",
    "38089911",
    "38089919",
)

NCM_PRODUTOS: dict[str, SelecaoNcm] = {
    "soja": SelecaoNcm(("12019000", "12010090")),
    "soja_grao": SelecaoNcm(("12019000", "12010090")),
    "soja_semeadura": SelecaoNcm(("12011000", "12010010")),
    "oleo_soja": SelecaoNcm(("1507",)),
    "oleo_soja_bruto": SelecaoNcm(("15071000",)),
    "farelo_soja": SelecaoNcm(("2304",)),
    "milho": SelecaoNcm(("1005",)),
    "arroz": SelecaoNcm(("1006",)),
    "trigo": SelecaoNcm(("1001",)),
    "algodao": SelecaoNcm(("5201", "5203")),
    "algodao_cardado": SelecaoNcm(("520300",)),
    "cafe": SelecaoNcm(("09011", "09012")),
    "acucar": SelecaoNcm(("1701",)),
    "etanol": SelecaoNcm(("2207",)),
    "carne_bovina": SelecaoNcm(("0201", "0202")),
    "carne_frango": SelecaoNcm(("02071",)),
    "carne_suina": SelecaoNcm(("0203",)),
    "fertilizantes": SelecaoNcm(("31",)),
    "ureia": SelecaoNcm(("310210",)),
    "sulfato_amonio": SelecaoNcm(("31022100",)),
    "nitrato_amonio": SelecaoNcm(("31023000",)),
    "ssp": SelecaoNcm(
        ("31031900",), primeiro_ano=2017, motivo_primeiro_ano=_SUPERFOSFATOS_ANTES_DE_2017
    ),
    "tsp": SelecaoNcm(
        ("31031100",), primeiro_ano=2017, motivo_primeiro_ano=_SUPERFOSFATOS_ANTES_DE_2017
    ),
    "kcl": SelecaoNcm(("310420",)),
    "map": SelecaoNcm(("31054000",)),
    "dap": SelecaoNcm(("310530",)),
    "npk": SelecaoNcm(("31052000",)),
    "defensivos": SelecaoNcm(("3808",), excluidos=_DOMISSANITARIOS),
    "agrotoxicos": SelecaoNcm(("3808",), excluidos=_DOMISSANITARIOS),
}


def resolve_ncm(produto: str) -> SelecaoNcm:
    if not isinstance(produto, str):
        raise InvalidParameterError("produto deve ser uma string")
    lower = produto.lower().strip()
    if lower in _ESPECIES_DE_CAFE:
        raise InvalidParameterError(
            f"Produto '{produto}' removido: a NCM não separa espécie de café (09011110, café em "
            "grão, reúne arábica e conilon). Use produto='cafe' ou informe o código NCM da "
            "apresentação (ex.: '09011110')"
        )
    if re.fullmatch(r"[0-9]{2,8}", lower):
        return SelecaoNcm((lower,))
    ncm = NCM_PRODUTOS.get(lower)
    if ncm is None:
        raise InvalidParameterError(
            f"Produto '{produto}' sem mapeamento NCM. Informe um alias ou um prefixo NCM de 2 "
            f"a 8 dígitos. Aliases disponíveis: {list(NCM_PRODUTOS.keys())}"
        )
    return ncm
