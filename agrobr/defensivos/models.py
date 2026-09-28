from __future__ import annotations

import re
from typing import Literal

import pydantic

from agrobr import constants

FORMULADOS_RENAME: dict[str, str] = {
    "NR_REGISTRO": "nr_registro",
    "MARCA_COMERCIAL": "marca_comercial",
    "INGREDIENTE_ATIVO": "ingrediente_ativo",
    "TITULAR_DE_REGISTRO": "titular",
    "CLASSE": "classe",
    "FORMULACAO": "formulacao",
    "CLASSE_TOXICOLOGICA": "classe_toxicologica",
    "CLASSIFICACAO_AMBIENTAL": "classe_ambiental",
    "CLASSE_AMBIENTAL": "classe_ambiental",
    "ORGANICOS": "organicos",
    "MODO_DE_ACAO": "modo_de_acao",
    "CULTURA": "cultura",
    "PRAGA_NOME_CIENTIFICO": "praga",
    "PRAGA_NOME_COMUM": "praga_nome_comum",
    "MODALIDADE_DE_EMPREGO": "modalidade_de_emprego",
    "SITUACAO": "situacao",
}

TECNICOS_RENAME: dict[str, str] = {
    "NR_REGISTRO": "nr_registro",
    "NUMERO_REGISTRO": "nr_registro",
    "MARCA_COMERCIAL": "marca_comercial",
    "PRODUTO_TECNICO_MARCA_COMERCIAL": "marca_comercial",
    "INGREDIENTE_ATIVO": "ingrediente_ativo",
    "TITULAR_DE_REGISTRO": "titular",
    "TITULAR_REGISTRO": "titular",
    "CLASSE": "classe",
    "GRUPO_QUIMICI": "grupo_quimico",
    "NOME_CIENTIFICO": "nome_cientifico",
    "CLASSIFICACAO_TOXICOLOGICA": "classe_toxicologica",
    "CLASSIFICACAO_AMBIENTAL": "classe_ambiental",
}

FORMULADOS_PRODUCT_COLS: list[str] = [
    "nr_registro",
    "marca_comercial",
    "ingrediente_ativo",
    "titular",
    "classe",
    "formulacao",
    "classe_toxicologica",
    "classe_ambiental",
    "organicos",
    "modo_de_acao",
    "situacao",
    "composicao_texto",
]

AUTORIZACOES_COLS: list[str] = [
    "nr_registro",
    "marca_comercial",
    "ingrediente_ativo",
    "titular",
    "classe",
    "cultura",
    "praga",
    "praga_nome_comum",
    "modalidade_de_emprego",
    "situacao",
]

TECNICOS_COLS: list[str] = [
    "nr_registro",
    "marca_comercial",
    "ingrediente_ativo",
    "titular",
    "classe",
    "grupo_quimico",
    "nome_cientifico",
    "classe_toxicologica",
    "classe_ambiental",
    "composicao_texto",
]

COMPOSICAO_COLS: list[str] = [
    "tipo",
    "nr_registro",
    "ordem_componente",
    "ingrediente_ativo",
    "grupo_quimico",
    "componente_texto",
    "concentracao_texto",
    "concentracao_valor",
    "concentracao_unidade",
]


class AgrofitRegistro(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    nr_registro: str

    @pydantic.field_validator("nr_registro")
    @classmethod
    def registro_valido(cls, value: str) -> str:
        normalized = value.strip()
        if not re.fullmatch(constants.DEFENSIVOS_REGISTRO_PATTERN, normalized):
            raise ValueError("numero de registro invalido")
        return normalized


class AgrofitProduto(AgrofitRegistro):
    marca_comercial: str | None = None
    ingrediente_ativo: str | None = None
    titular: str | None = None
    classe: str | None = None
    classe_toxicologica: str | None = None
    classe_ambiental: str | None = None
    composicao_texto: str | None = None


class AgrofitFormulado(AgrofitProduto):
    formulacao: str | None = None
    organicos: str | None = None
    modo_de_acao: str | None = None
    situacao: str | None = None


class AgrofitTecnico(AgrofitProduto):
    grupo_quimico: str | None = None
    nome_cientifico: str | None = None


class AgrofitAutorizacao(AgrofitRegistro):
    marca_comercial: str | None = None
    ingrediente_ativo: str | None = None
    titular: str | None = None
    classe: str | None = None
    cultura: str | None = None
    praga: str | None = None
    praga_nome_comum: str | None = None
    modalidade_de_emprego: str | None = None
    situacao: str | None = None


class AgrofitComponente(AgrofitRegistro):
    tipo: Literal["formulados", "tecnicos"]
    ordem_componente: int = pydantic.Field(ge=1)
    ingrediente_ativo: str | None = None
    grupo_quimico: str | None = None
    componente_texto: str = pydantic.Field(min_length=1)
    concentracao_texto: str | None = None
    concentracao_valor: float | None = pydantic.Field(default=None, ge=0, allow_inf_nan=False)
    concentracao_unidade: str | None = None

    @pydantic.field_validator("componente_texto")
    @classmethod
    def componente_nao_vazio(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("componente textual vazio")
        return value
