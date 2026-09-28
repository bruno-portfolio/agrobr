from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from agrobr import constants
from agrobr.normalize import regions

SOCIOBIODIVERSIDADE_PRODUTOS = constants.CONAB_SOCIOBIODIVERSIDADE_PRODUTOS

CULTURAS_MAP: dict[str, str] = {
    "soja": "soja",
    "milho": "milho",
    "milho verao": "milho_verao",
    "milho verão": "milho_verao",
    "milho safrinha": "milho_safrinha",
    "milho 2a safra": "milho_safrinha",
    "arroz": "arroz",
    "arroz irrigado": "arroz_irrigado",
    "arroz sequeiro": "arroz_sequeiro",
    "feijao": "feijao",
    "feijão": "feijao",
    "algodao": "algodao",
    "algodão": "algodao",
    "trigo": "trigo",
    "cafe": "cafe",
    "café": "cafe",
    "cafe arabica": "cafe_arabica",
    "café arábica": "cafe_arabica",
    "cafe conilon": "cafe_conilon",
    "café conilon": "cafe_conilon",
    "mandioca": "mandioca",
    "cana": "cana",
    "cana de acucar": "cana",
    "cana-de-açúcar": "cana",
    "sorgo": "sorgo",
}

CATEGORIAS_MAP: dict[str, str] = {
    "sementes": "insumos",
    "fertilizantes": "insumos",
    "adubação de base": "insumos",
    "adubação de cobertura": "insumos",
    "corretivos": "insumos",
    "defensivos": "insumos",
    "herbicidas": "insumos",
    "inseticidas": "insumos",
    "fungicidas": "insumos",
    "adjuvantes": "insumos",
    "tratamento de sementes": "insumos",
    "inoculante": "insumos",
    "operações com máquinas": "operacoes",
    "operações mecânicas": "operacoes",
    "preparo do solo": "operacoes",
    "plantio": "operacoes",
    "semeadura": "operacoes",
    "pulverização": "operacoes",
    "pulverizações": "operacoes",
    "colheita": "operacoes",
    "colheita mecânica": "operacoes",
    "transporte interno": "operacoes",
    "mão de obra": "mao_de_obra",
    "mao de obra": "mao_de_obra",
    "mão de obra temporária": "mao_de_obra",
    "empreita": "mao_de_obra",
    "depreciação": "custos_fixos",
    "depreciação de máquinas": "custos_fixos",
    "depreciação de benfeitorias": "custos_fixos",
    "manutenção periódica": "custos_fixos",
    "manutenção": "custos_fixos",
    "seguros": "custos_fixos",
    "juros sobre capital fixo": "custos_fixos",
    "assistência técnica": "outros",
    "arrendamento": "outros",
    "terra": "outros",
    "cessr": "outros",
    "funrural": "outros",
    "transporte externo": "outros",
    "armazenagem": "outros",
    "juros sobre capital de giro": "outros",
    "agrotóxicos": "insumos",
    "mudas": "insumos",
    "royalties": "insumos",
    "máquinas próprias": "operacoes",
}

CATEGORIAS_POR_SECAO = (
    ("DEPRECIAC", "custos_fixos"),
    ("OUTROS CUSTOS FIXOS", "custos_fixos"),
    ("OUTRAS DESPESAS", "outros"),
    ("POS-COLHEITA", "outros"),
    ("FINANCEIRAS", "outros"),
    ("RENDA DE FATORES", "outros"),
)

PLURAIS = (
    ("oes", "ao"),
    ("aes", "ao"),
    ("ais", "al"),
    ("eis", "el"),
    ("ns", "m"),
    ("res", "r"),
    ("zes", "z"),
)


def _singular(palavra: str) -> str:
    if len(palavra) <= 3 or not palavra.endswith("s"):
        return palavra
    for fim, troca in PLURAIS:
        if palavra.endswith(fim):
            return palavra[: -len(fim)] + troca
    return palavra[:-1]


def _termo(texto: str) -> str:
    return " ".join(
        _singular(palavra)
        for palavra in regions.remover_acentos(texto).lower().replace("-", " ").split()
    )


CATEGORIAS_TERMOS = {_termo(nome): categoria for nome, categoria in CATEGORIAS_MAP.items()}


def classify_categoria(item_name: str, secao: str | None = None) -> str:
    if secao is not None:
        publicada = regions.remover_acentos(secao).upper()
        for trecho, categoria in CATEGORIAS_POR_SECAO:
            if trecho in publicada:
                return categoria
    termo = _termo(item_name)
    for nome, categoria in CATEGORIAS_TERMOS.items():
        if nome in termo:
            return categoria
    return "outros"


def normalize_cultura(nome: str) -> str:
    lower = nome.lower().strip()
    return CULTURAS_MAP.get(lower, lower.replace(" ", "_"))


def normalize_produto_sociobio(nome: str) -> str:
    value = regions.remover_acentos(nome).lower().strip().replace("'", "").replace("’", "")
    value = re.sub(r"\s*\([^)]*\)", "", value)
    value = re.sub(r"[\s-]+", "_", value)
    return "pinhao" if value == "pinhao_de_araucaria" else value


class RecursoCusto(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    planilha: str
    cultura: str
    titulo: str
    pagina_url: str


class RecursoSociobio(RecursoCusto):
    ativo: bool


class ContextoCusto(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    cultura: str
    planilha: str
    aba: str
    indice_aba: int
    local: str
    uf: str
    ano_referencia: int
    referencia: str
    data_referencia: date | None
    safra: str | None
    sistema: str
    celulas_contexto: dict[str, str]


class ObservacaoCusto(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    cultura: str
    uf: str
    safra: str | None
    tecnologia: str | None
    categoria: str
    item: str
    unidade: str
    quantidade_ha: float | None = None
    preco_unitario: float | None = None
    valor_ha: float | None
    participacao_pct: float | None = None
    local: str
    ano_referencia: int
    referencia: str
    data_referencia: date | None
    planilha: str
    aba: str
    sistema: str
    linha: int
    tipo_linha: Literal["item", "subtotal", "total"]
    secao: str | None
    unidade_produto: str | None
    valor_unidade_produto: float | None
    participacao_cv_pct: float | None
    participacao_ct_pct: float | None

    @field_validator(
        "quantidade_ha",
        "preco_unitario",
        "valor_ha",
        "participacao_pct",
        "valor_unidade_produto",
        "participacao_cv_pct",
        "participacao_ct_pct",
    )
    @classmethod
    def finite_measure(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("Medida não finita")
        return value


class ConsultaCusto(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    cultura: str | None = None
    uf: str | None = None
    safra: str | None = None
    local: str | None = None
    ano: int | None = Field(None, ge=1900, le=2100)
    planilha: str | None = None
    aba: str | None = None

    @field_validator("cultura", "uf", "safra", "local", "planilha", "aba")
    @classmethod
    def nonempty_text(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("Seletor vazio")
        return value


class ResultadoCusto(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    contexto: ContextoCusto
    observacoes: list[ObservacaoCusto]
    detalhes: dict[str, Any]


class DadosContextoSociobio(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    produto: str
    local: str
    uf: str
    ano: int
    safra_publicada: str | None
    sistema: str
    tipo_relatorio: str | None
    mes_ano_referencia: str | None
    data_precos: datetime | None
    produtividade: float | None
    unidade_produtividade: str | None
    planilha: str
    aba: str


class ContextoSociobio(DadosContextoSociobio):
    indice_aba: int
    layout: Literal["antigo", "novo"]
    celulas_contexto: dict[str, str]


class ObservacaoSociobio(DadosContextoSociobio):
    secao: str | None
    item: str
    tipo_linha: Literal["item", "total", "secao"]
    linha: int
    valor: float | None
    unidade_valor: str
    valor_unidade_produto: float | None
    unidade_produto: str | None
    participacao_pct: float | None
    participacao_ct_pct: float | None

    @field_validator(
        "produtividade", "valor", "valor_unidade_produto", "participacao_pct", "participacao_ct_pct"
    )
    @classmethod
    def finite_measure(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("Medida não finita")
        return value


class ConsultaSociobio(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    produto: str | None = None
    uf: str | None = None
    ano: int | None = Field(None, ge=1900, le=2100)
    local: str | None = None
    planilha: str | None = None
    aba: str | None = None

    @field_validator("produto", "uf", "local", "planilha", "aba")
    @classmethod
    def nonempty_text(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("Seletor vazio")
        return value


class ResultadoSociobio(BaseModel):
    contexto: ContextoSociobio
    observacoes: list[ObservacaoSociobio]
    detalhes: dict[str, Any]
