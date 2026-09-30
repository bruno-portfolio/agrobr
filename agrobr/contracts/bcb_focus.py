from __future__ import annotations

import re
from typing import Any

import pandas as pd

from agrobr import constants, contracts


class FocusContract(contracts.Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        for column in self.columns:
            expected = None
            if column.type == contracts.ColumnType.FLOAT:
                expected = "float64"
            elif column.type == contracts.ColumnType.INTEGER:
                expected = "Int64"
            elif column.name == "data":
                expected = "datetime64[ns]"
            if (
                expected is not None
                and column.name in df
                and str(df[column.name].dtype) != expected
            ):
                errors.append(f"Column '{column.name}' must use {expected} dtype")
        if errors:
            return False, errors
        if not df["data"].eq(df["data"].dt.normalize()).all():
            errors.append("data must be a civil survey date without time of day")
        if not df["data"].is_monotonic_decreasing:
            errors.append("Survey dates must be ordered from most recent to oldest")
        if not df["periodicidade"].isin(constants.BCB_FOCUS_ENTITIES).all():
            errors.append("periodicidade must be anual or mensal")
        if not all(value.strip() for value in df["indicador"]):
            errors.append("indicador must contain non-empty text")
        for periodicidade, reference in zip(
            df["periodicidade"], df["data_referencia"], strict=True
        ):
            if not _valid_reference(periodicidade, reference):
                errors.append("data_referencia must match its annual or monthly horizon")
                break
        if df.loc[df["periodicidade"].eq("mensal"), "indicador_detalhe"].notna().any():
            errors.append("Monthly expectations must have null indicador_detalhe")
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            date_dtype="datetime64[ns]",
            date_semantics="civil_survey_date_without_timezone",
            integer_dtype="Int64",
            float_dtype="float64",
            date_order="descending",
            reference_formats={"anual": "YYYY", "mensal": "MM/YYYY"},
            null_key_semantics="missing_dimension; empty_detail_text_is_distinct",
            periodicidade_semantics="forecast_horizon_granularity",
            statistical_consistency="source_values_preserved_with_diagnostics",
        )
        return schema


def _valid_reference(periodicidade: str, value: str) -> bool:
    if periodicidade == "anual":
        return (
            re.fullmatch(constants.BCB_FOCUS_ANNUAL_REFERENCE_PATTERN, value) is not None
            and int(value) > 0
        )
    if (
        periodicidade != "mensal"
        or re.fullmatch(constants.BCB_FOCUS_MONTHLY_REFERENCE_PATTERN, value) is None
    ):
        return False
    return 1 <= int(value[:2]) <= 12 and int(value[3:]) > 0


BCB_FOCUS_V2 = FocusContract(
    name="bcb.focus",
    version="2.0",
    effective_from="2.0.0",
    primary_key=[
        "periodicidade",
        "indicador",
        "indicador_detalhe",
        "data",
        "data_referencia",
        "base_calculo",
    ],
    columns=[
        contracts.Column(name="indicador", type=contracts.ColumnType.STRING),
        contracts.Column(name="data", type=contracts.ColumnType.DATE),
        contracts.Column(name="data_referencia", type=contracts.ColumnType.STRING),
        *[
            contracts.Column(name=name, type=contracts.ColumnType.FLOAT, nullable=True)
            for name in ("media", "mediana", "desvio_padrao", "minimo", "maximo")
        ],
        *[
            contracts.Column(
                name=name,
                type=contracts.ColumnType.INTEGER,
                nullable=True,
                min_value=0,
                max_value=2**31 - 1,
            )
            for name in ("numero_respondentes", "base_calculo")
        ],
        contracts.Column(name="periodicidade", type=contracts.ColumnType.STRING),
        contracts.Column(name="indicador_detalhe", type=contracts.ColumnType.STRING, nullable=True),
    ],
    guarantees=[
        "Doze colunas estáveis, incluindo resultados vazios e ausências explícitas",
        "Data da pesquisa e horizonte textual da previsão são dimensões distintas",
        "Detalhe anual e base de cálculo preservam a identidade das expectativas",
        "Valores negativos finitos são preservados sem imputação ou arredondamento",
        "Duplicatas ou falhas de página interrompem a aquisição, sem retorno parcial",
        "Limite local e ausência de contagem independente são explícitos nos metadados",
        "Estatísticas internas inconsistentes permanecem publicadas com diagnóstico, sem certificação científica",
    ],
)

contracts.register_contract("bcb_focus", BCB_FOCUS_V2)
