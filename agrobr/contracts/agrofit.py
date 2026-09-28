from __future__ import annotations

import re
from typing import Any

import pandas as pd

from agrobr import constants, contracts


class AgrofitContract(contracts.Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        if "nr_registro" in df and not all(
            isinstance(value, str) and re.fullmatch(constants.DEFENSIVOS_REGISTRO_PATTERN, value)
            for value in df["nr_registro"]
        ):
            errors.append("Column 'nr_registro' must contain valid textual registration numbers")
        if "tipo" in df and not df["tipo"].isin(("formulados", "tecnicos")).all():
            errors.append("Column 'tipo' must contain formulados or tecnicos")
        if "componente_texto" in df and not all(
            isinstance(value, str) and value.strip() for value in df["componente_texto"]
        ):
            errors.append("Column 'componente_texto' must contain non-empty strings")
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"]["nr_registro_pattern"] = constants.DEFENSIVOS_REGISTRO_PATTERN
        if "tipo" in self.list_columns():
            schema["constraints"]["tipo_allowed"] = ["formulados", "tecnicos"]
            schema["constraints"]["non_empty_strings"] = ["componente_texto"]
        return schema


def _text_columns(names: tuple[str, ...]) -> list[contracts.Column]:
    return [
        contracts.Column(
            name=name, type=contracts.ColumnType.STRING, nullable=name != "nr_registro"
        )
        for name in names
    ]


AGROFIT_FORMULADOS_V1_1 = AgrofitContract(
    name="defensivos.formulados",
    version="1.1",
    effective_from="2.0.0",
    primary_key=["nr_registro"],
    columns=_text_columns(
        (
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
        )
    )
    + [
        contracts.Column(
            name="situacao",
            type=contracts.ColumnType.STRING,
            nullable=True,
            stable=False,
            description="Texto publicado, sem inferência de vigência cadastral",
        ),
        contracts.Column(
            name="composicao_texto",
            type=contracts.ColumnType.STRING,
            nullable=True,
            stable=False,
            description="Célula original de ingrediente/composição",
        ),
    ],
    guarantees=[
        "Uma linha por número de registro textual, preservando zeros iniciais",
        "Atributos de produto divergentes entre linhas do mesmo registro geram erro",
        "Situação preservada como texto da exportação, sem conversão para autorização de uso",
        "Componentes e concentrações ficam na tabela agrofit_composicao, por posição",
    ],
)

AGROFIT_TECNICOS_V1_1 = AgrofitContract(
    name="defensivos.tecnicos",
    version="1.1",
    effective_from="2.0.0",
    primary_key=["nr_registro"],
    columns=_text_columns(
        (
            "nr_registro",
            "marca_comercial",
            "ingrediente_ativo",
            "titular",
            "classe",
            "grupo_quimico",
            "nome_cientifico",
            "classe_toxicologica",
            "classe_ambiental",
        )
    )
    + [
        contracts.Column(
            name="composicao_texto",
            type=contracts.ColumnType.STRING,
            nullable=True,
            stable=False,
            description="Célula original de ingrediente/composição",
        ),
    ],
    guarantees=[
        "Número de registro textual, preservando zeros iniciais e códigos alfanuméricos",
        "Múltiplos ingredientes e grupos mantêm sua ordem; componentes detalhados ficam em agrofit_composicao",
        "Campo de situação não é inventado para a exportação técnica que não o publica",
    ],
)

AGROFIT_AUTORIZACOES_V1_1 = AgrofitContract(
    name="defensivos.autorizacoes",
    version="1.1",
    effective_from="2.0.0",
    primary_key=[],
    columns=_text_columns(
        (
            "nr_registro",
            "marca_comercial",
            "ingrediente_ativo",
            "titular",
            "classe",
            "cultura",
            "praga",
            "praga_nome_comum",
            "modalidade_de_emprego",
        )
    )
    + [
        contracts.Column(
            name="situacao", type=contracts.ColumnType.STRING, nullable=True, stable=False
        ),
    ],
    guarantees=[
        "Cada linha publicada é preservada, inclusive multiplicidade após projeção de colunas",
        "Não se declara chave oficial única para a relação de autorizações",
        "A exportação descreve relações cadastrais, sem recomendar uma aplicação agronômica",
    ],
)

AGROFIT_COMPOSICAO_V1 = AgrofitContract(
    name="defensivos.composicao",
    version="1.0",
    effective_from="2.0.0",
    primary_key=["tipo", "nr_registro", "ordem_componente"],
    columns=[
        contracts.Column(name="tipo", type=contracts.ColumnType.STRING, nullable=False),
        contracts.Column(name="nr_registro", type=contracts.ColumnType.STRING, nullable=False),
        contracts.Column(
            name="ordem_componente", type=contracts.ColumnType.INTEGER, nullable=False, min_value=1
        ),
        contracts.Column(name="ingrediente_ativo", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="grupo_quimico", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="componente_texto", type=contracts.ColumnType.STRING, nullable=False),
        contracts.Column(
            name="concentracao_texto", type=contracts.ColumnType.STRING, nullable=True
        ),
        contracts.Column(
            name="concentracao_valor", type=contracts.ColumnType.FLOAT, nullable=True, min_value=0
        ),
        contracts.Column(
            name="concentracao_unidade", type=contracts.ColumnType.STRING, nullable=True
        ),
    ],
    guarantees=[
        "Uma linha por posição do componente no registro e na família de produto",
        "Ingredientes repetidos em posições distintas não são deduplicados",
        "Concentração e unidade originais são preservadas, sem conversão dimensional implícita",
        "Valor não interpretável permanece nulo; ausência não equivale a zero",
        "Composição não é multiplicada pelo número de autorizações de uso",
    ],
)

contracts.register_contract("agrofit_formulados", AGROFIT_FORMULADOS_V1_1)
contracts.register_contract("agrofit_tecnicos", AGROFIT_TECNICOS_V1_1)
contracts.register_contract("agrofit_autorizacoes", AGROFIT_AUTORIZACOES_V1_1)
contracts.register_contract("agrofit_composicao", AGROFIT_COMPOSICAO_V1)
