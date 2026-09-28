from __future__ import annotations

import re
from typing import Any

import pandas as pd

from agrobr import constants, contracts
from agrobr.normalize import regions


class ListaSujaContract(contracts.Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        for column in self.columns:
            if column.name not in df:
                continue
            series = df[column.name]
            if column.type == contracts.ColumnType.INTEGER and str(series.dtype) != "Int64":
                errors.append(f"Column '{column.name}' must use nullable Int64 dtype")
            if column.type == contracts.ColumnType.DATE and str(series.dtype) != "datetime64[ns]":
                errors.append(f"Column '{column.name}' must use civil datetime64[ns] dtype")
            if column.type == contracts.ColumnType.STRING and not all(
                isinstance(value, str) and value.strip() for value in series.dropna()
            ):
                errors.append(f"Column '{column.name}' must contain non-empty strings or nulls")
        if "id_registro" in df and not all(
            isinstance(value, str) and re.fullmatch(r"[0-9]+", value) for value in df["id_registro"]
        ):
            errors.append("Column 'id_registro' must contain textual registration IDs")
        if "uf" in df and not df["uf"].dropna().isin(regions.UFS_VALIDAS).all():
            errors.append("Column 'uf' contains an invalid UF")
        if {"data_inclusao_texto", "data_inclusao"}.issubset(df.columns):
            for text, scalar in zip(df["data_inclusao_texto"], df["data_inclusao"], strict=True):
                if (
                    isinstance(text, str)
                    and len(re.findall(constants.LISTA_SUJA_DATE_PATTERN, text)) > 1
                    and not pd.isna(scalar)
                ):
                    errors.append("Compound inclusion text requires a null data_inclusao")
                    break
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            id_registro_pattern=r"^[0-9]+$",
            primary_key_scope="one_resource_content_hash",
            uf_allowed=sorted(regions.UFS_VALIDAS),
            integer_dtype="Int64",
            date_dtype="datetime64[ns]",
            date_semantics="civil_date_without_timezone",
            blank_strings="null_for_nullable_fields",
            compound_inclusion_scalar="null",
        )
        return schema


LISTA_SUJA_EMPREGADORES_V2 = ListaSujaContract(
    name="lista_suja.empregadores",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["id_registro"],
    columns=[
        contracts.Column(name="empregador", type=contracts.ColumnType.STRING),
        contracts.Column(name="cpf_cnpj", type=contracts.ColumnType.STRING),
        contracts.Column(name="estabelecimento", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="uf", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="cnae", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="data_inclusao", type=contracts.ColumnType.DATE, nullable=True),
        contracts.Column(
            name="trabalhadores_resgatados",
            type=contracts.ColumnType.INTEGER,
            nullable=True,
            min_value=0,
            description="Campo oficial Trabalhadores envolvidos, com nome legado da API",
        ),
        contracts.Column(
            name="ano_acao_fiscal",
            type=contracts.ColumnType.INTEGER,
            nullable=True,
            min_value=1,
            max_value=9999,
        ),
        contracts.Column(
            name="id_registro",
            type=contracts.ColumnType.STRING,
            description="ID textual local à exportação identificada pelo hash do recurso",
        ),
        contracts.Column(name="data_decisao", type=contracts.ColumnType.DATE, nullable=True),
        contracts.Column(name="data_atualizacao", type=contracts.ColumnType.DATE, nullable=True),
        contracts.Column(
            name="data_inclusao_texto",
            type=contracts.ColumnType.STRING,
            description="Texto original de inclusão, preservando intervalos e múltiplas datas",
        ),
    ],
    guarantees=[
        "Uma linha por ID da exportação; documentos repetidos não são deduplicados",
        "CPF/CNPJ e CNAE preservam pontuação e zeros iniciais como texto",
        "Ausências permanecem nulas; números anuláveis usam Int64 e datas civis datetime64[ns]",
        "Inclusão composta permanece no texto original, sem escolher uma data escalar",
        "Atualização do cadastro exige evidência no corpo do PDF ou TXT conferido com o CSV",
        "O hash identifica a exportação corrente; não se declara histórico de revisões",
    ],
)

contracts.register_contract("lista_suja_empregadores", LISTA_SUJA_EMPREGADORES_V2)
