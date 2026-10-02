from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date, datetime, time
from typing import Any, Literal, Self

import pandas as pd
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    ValidationInfo,
    field_validator,
    model_validator,
)

from agrobr import constants
from agrobr.constants import URLS, Fonte
from agrobr.normalize.dates import month_to_number
from agrobr.normalize.numeric import parse_numeric_br
from agrobr.normalize.regions import REGIOES, normalizar_uf, remover_acentos
from agrobr.normalize.regions import UFS_VALIDAS as UFS_VALIDAS

COLUNAS_PRECOS = constants.ANP_DIESEL_PRECOS_COLUMNS
PRECOS_DTYPES = {
    nome: pd.Series([""]).dtype if dtype == "str" else dtype
    for nome, dtype in constants.ANP_DIESEL_PRECOS_DTYPES.items()
}

SHLP_BASE = URLS[Fonte.ANP_DIESEL]["shlp"]
VENDAS_DIESEL_CSV_URL = URLS[Fonte.ANP_DIESEL]["vendas_diesel_csv"]

PRECOS_MUNICIPIOS_URLS: dict[str, str] = {
    "2022-2023": f"{SHLP_BASE}/semanal/semanal-municipios-2022_a_2023.xlsx",
    "2024-2025": f"{SHLP_BASE}/semanal/semanal-municipio-2024-2025.xlsx",
    "2026": f"{SHLP_BASE}/semanal/semanal-municipios-2026.xlsx",
}

PRECOS_ESTADOS_URL = f"{SHLP_BASE}/semanal/semanal-estados-desde-2013.xlsx"

PRECOS_BRASIL_URL = f"{SHLP_BASE}/semanal/semanal-brasil-desde-2013.xlsx"

PRODUTOS_DIESEL = frozenset(
    {
        "DIESEL",
        "DIESEL S10",
    }
)


def normalize_produto(produto: str) -> str:
    return " ".join(remover_acentos(produto).upper().split()).removeprefix("OLEO ")


NIVEL_MUNICIPIO = "municipio"
NIVEL_UF = "uf"
NIVEL_BRASIL = "brasil"
NIVEIS_VALIDOS = frozenset({NIVEL_MUNICIPIO, NIVEL_UF, NIVEL_BRASIL})

AGREGACAO_SEMANAL = "semanal"
AGREGACAO_MENSAL = "mensal"
AGREGACOES_VALIDAS = frozenset({AGREGACAO_SEMANAL, AGREGACAO_MENSAL})


def _resolve_periodo_municipio(
    ano: int,
    catalog: Mapping[str, str] | None = None,
) -> str | None:
    for periodo in PRECOS_MUNICIPIOS_URLS if catalog is None else catalog:
        partes = periodo.split("-")
        if len(partes) == 2:
            inicio, fim = int(partes[0]), int(partes[1])
            if inicio <= ano <= fim:
                return periodo
        elif len(partes) == 1:
            if ano == int(partes[0]):
                return periodo
    return None


def normalize_municipio(value: str) -> str:
    return " ".join(remover_acentos(value).upper().split())


class PrecoSemanal(BaseModel):
    periodo_inicio: date
    periodo_fim: date
    uf: str
    municipio: str
    produto: Literal["DIESEL", "DIESEL S10"]
    preco_venda: FiniteFloat = Field(ge=0)
    preco_compra: FiniteFloat | None = Field(ge=0)
    n_postos: int = Field(ge=0, le=2**63 - 1)
    nivel: Literal["brasil", "uf", "municipio"]
    unidade: Literal["BRL/litro"]

    @field_validator("periodo_inicio", "periodo_fim", mode="before")
    @classmethod
    def parse_date(cls, value: Any) -> date:
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                value = datetime.strptime(value, "%d/%m/%Y")
        if isinstance(value, datetime):
            if value.tzinfo is not None or value.time() != time.min:
                raise ValueError("Data semanal deve ser civil, sem horario")
            value = value.date()
        if not isinstance(value, date):
            raise ValueError("Data semanal invalida")
        if not date(1678, 1, 1) <= value <= date(2261, 12, 31):
            raise ValueError("Data fora do intervalo datetime64[ns]")
        return value

    @field_validator("preco_venda", "preco_compra", "n_postos", mode="before")
    @classmethod
    def parse_number(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("Booleano nao e numero de pesquisa")
        if isinstance(value, str):
            return value.replace(",", ".")
        return value

    @field_validator("preco_compra", mode="before")
    @classmethod
    def missing_distribution(cls, value: Any) -> Any:
        return None if value == "-" else value

    @field_validator("produto", mode="before")
    @classmethod
    def canonical_product(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("Produto deve ser texto")
        return normalize_produto(value)

    @field_validator("unidade", mode="before")
    @classmethod
    def published_unit(cls, value: Any) -> str:
        if value != "R$/l":
            raise ValueError("Unidade publicada deve ser R$/l")
        return "BRL/litro"

    @field_validator("uf")
    @classmethod
    def canonical_state(cls, value: str) -> str:
        if value == "":
            return value
        normalized = normalizar_uf(value)
        if normalized not in UFS_VALIDAS:
            raise ValueError("UF publicada invalida")
        return normalized

    @field_validator("municipio")
    @classmethod
    def canonical_city(cls, value: str) -> str:
        return normalize_municipio(value)

    @model_validator(mode="after")
    def consistent_period_and_location(self) -> Self:
        if self.periodo_fim < self.periodo_inicio:
            raise ValueError("DATA FINAL anterior a DATA INICIAL")
        if self.nivel == "brasil" and (self.uf or self.municipio):
            raise ValueError("Brasil nao admite UF ou municipio")
        if self.nivel in {"uf", "municipio"} and not self.uf:
            raise ValueError("Nivel estadual/municipal exige UF")
        if self.nivel == "uf" and self.municipio:
            raise ValueError("Nivel estadual nao admite municipio")
        if self.nivel == "municipio" and not self.municipio:
            raise ValueError("Nivel municipal exige municipio")
        return self


class VendaMensal(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    ano: int = Field(ge=1678, le=2261)
    mes: int = Field(ge=1, le=12)
    uf: str = ""
    regiao: str = ""
    produto: str = ""
    volume_m3: FiniteFloat | None

    @field_validator("ano", "mes", mode="before")
    @classmethod
    def parse_calendar_integer(cls, value: Any, info: ValidationInfo) -> Any:
        if isinstance(value, bool):
            raise ValueError("Ano e mês devem ser inteiros")
        if info.field_name == "mes" and isinstance(value, str):
            return month_to_number(value) or value
        return value

    @field_validator("volume_m3", mode="before")
    @classmethod
    def parse_volume(cls, value: Any) -> float | None:
        if value is None or (isinstance(value, str) and value.strip() in {"", "-"}):
            return None
        if isinstance(value, bool):
            raise ValueError("Booleano não é volume de vendas")
        parsed = parse_numeric_br(value)
        if parsed is None:
            raise ValueError("Volume de vendas inválido")
        return parsed

    @field_validator("uf")
    @classmethod
    def canonical_state(cls, value: str) -> str:
        if not value:
            return value
        normalized = normalizar_uf(value)
        if normalized not in UFS_VALIDAS:
            raise ValueError("UF publicada inválida")
        return normalized

    @field_validator("regiao")
    @classmethod
    def canonical_region(cls, value: str) -> str:
        if not value:
            return value
        regioes = {remover_acentos(nome).upper(): nome for nome in REGIOES}
        chave = remover_acentos(value).upper().removeprefix("REGIAO ").strip()
        if chave not in regioes:
            raise ValueError("Região publicada inválida")
        return regioes[chave]

    @field_validator("produto")
    @classmethod
    def canonical_product(cls, value: str) -> str:
        produto = re.sub(r"^[OÓ]LEO\s+", "", value.upper())
        return "DIESEL S10" if produto == "DIESEL S-10" else produto


class MunicipalWorkbook(BaseModel):
    inicio: int = Field(ge=2022, le=2261)
    fim: int = Field(ge=2022, le=2261)
    url: str

    @model_validator(mode="after")
    def validate_publication(self) -> Self:
        if self.inicio > self.fim:
            raise ValueError("Período municipal invertido")
        if not self.url.startswith(f"{SHLP_BASE}/semanal/"):
            raise ValueError("Planilha fora do catálogo semanal oficial")
        return self

    @property
    def periodo(self) -> str:
        return str(self.inicio) if self.inicio == self.fim else f"{self.inicio}-{self.fim}"
