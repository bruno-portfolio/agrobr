from __future__ import annotations

import re
from datetime import datetime
from typing import Any

import pandas as pd

from agrobr import constants, contracts


class CultivaresContract(contracts.Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        for column in self.columns:
            if column.name not in df:
                continue
            series = df[column.name]
            if column.type == contracts.ColumnType.DATE:
                if str(series.dtype) != "datetime64[ns]":
                    errors.append(f"Column '{column.name}' must use civil datetime64[ns] dtype")
                elif not series.dropna().eq(series.dropna().dt.normalize()).all():
                    errors.append(f"Column '{column.name}' must contain dates without a time")
        for name in self.primary_key:
            if name in df and not all(
                isinstance(value, str) and value.strip() for value in df[name]
            ):
                errors.append(f"Column '{name}' must contain non-empty textual identifiers")
        if {"termino_protecao_texto", "termino_protecao"}.issubset(df.columns) and str(
            df["termino_protecao"].dtype
        ) == "datetime64[ns]":
            for text, scalar in zip(
                df["termino_protecao_texto"], df["termino_protecao"], strict=True
            ):
                if not _valid_protection_end(text, scalar):
                    errors.append("Protection end date must agree with its published text")
                    break
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            primary_key_scope="one_family_and_resource",
            non_empty_strings=self.primary_key,
            date_dtype="datetime64[ns]",
            date_semantics="civil_date_without_timezone_or_time",
            blank_strings="preserved",
        )
        if "termino_protecao_texto" in self.list_columns():
            schema["constraints"].update(
                protection_end_text_matches_date=True,
                protection_end_null_texts=["", constants.SNPC_CONDITIONAL_END],
                protection_end_date_pattern=constants.RNC_DATE_PATTERN,
            )
        return schema


def _valid_protection_end(text: Any, scalar: Any) -> bool:
    if not isinstance(text, str):
        return False
    if text in ("", constants.SNPC_CONDITIONAL_END):
        return bool(pd.isna(scalar))
    if not re.fullmatch(constants.RNC_DATE_PATTERN, text):
        return False
    try:
        expected = datetime.strptime(text, "%d/%m/%Y").date()
    except ValueError:
        return False
    return isinstance(scalar, pd.Timestamp) and not pd.isna(scalar) and scalar.date() == expected


RNC_REGISTRADAS_V1 = CultivaresContract(
    name="rnc.registradas",
    version="1.0",
    effective_from="2.0.0",
    primary_key=["nr_registro"],
    columns=[
        contracts.Column(name="cultivar", type=contracts.ColumnType.STRING),
        contracts.Column(name="nome_comum", type=contracts.ColumnType.STRING),
        contracts.Column(name="nome_cientifico", type=contracts.ColumnType.STRING),
        contracts.Column(name="grupo", type=contracts.ColumnType.STRING),
        contracts.Column(name="situacao", type=contracts.ColumnType.STRING),
        contracts.Column(name="nr_formulario", type=contracts.ColumnType.STRING),
        contracts.Column(name="nr_registro", type=contracts.ColumnType.STRING),
        contracts.Column(name="data_registro", type=contracts.ColumnType.DATE, nullable=True),
        contracts.Column(name="data_validade", type=contracts.ColumnType.DATE, nullable=True),
        contracts.Column(name="mantenedor", type=contracts.ColumnType.STRING),
    ],
    guarantees=[
        "Uma linha por número de registro textual na exportação RNC processada",
        "Formulários repetidos e vazios publicados são preservados; não constituem chave",
        "Textos vazios permanecem strings vazias e identificadores não são convertidos em números",
        "Datas civis usam datetime64[ns], sem timezone ou horário; ausência não é imputada",
        "Situação e mantenedores permanecem textos publicados, sem inferência jurídica ou divisão",
    ],
)

RNC_PROTEGIDAS_V1 = CultivaresContract(
    name="rnc.protegidas",
    version="1.0",
    effective_from="2.0.0",
    primary_key=["nr_processo"],
    columns=[
        contracts.Column(name="cultivar", type=contracts.ColumnType.STRING),
        contracts.Column(name="nome_cientifico", type=contracts.ColumnType.STRING),
        contracts.Column(name="nome_comum", type=contracts.ColumnType.STRING),
        contracts.Column(name="nr_processo", type=contracts.ColumnType.STRING),
        contracts.Column(name="situacao", type=contracts.ColumnType.STRING),
        contracts.Column(name="nr_certificado", type=contracts.ColumnType.STRING),
        contracts.Column(name="inicio_protecao", type=contracts.ColumnType.DATE, nullable=True),
        contracts.Column(name="termino_protecao", type=contracts.ColumnType.DATE, nullable=True),
        contracts.Column(name="titular", type=contracts.ColumnType.STRING),
        contracts.Column(name="representante_legal", type=contracts.ColumnType.STRING),
        contracts.Column(name="melhoristas", type=contracts.ColumnType.STRING),
        contracts.Column(
            name="termino_protecao_texto",
            type=contracts.ColumnType.STRING,
            description="Célula publicada de término com espaços externos removidos",
        ),
    ],
    guarantees=[
        "Uma linha por número de processo textual na exportação SNPC processada",
        "Certificados repetidos entre processos são preservados; não constituem chave",
        "Texto original de término é preservado, inclusive datas, vazio e condição publicada",
        "Término é nulo somente para texto vazio ou a condição oficial sem data determinada",
        "Datas civis usam datetime64[ns], sem timezone ou horário; ausência não é imputada",
        "Campos compostos e situação não são divididos, deduplicados ou interpretados juridicamente",
    ],
)
