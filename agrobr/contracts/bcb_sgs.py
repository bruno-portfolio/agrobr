from __future__ import annotations

import copy
import dataclasses
from typing import Any

import pandas as pd

from agrobr import contracts


@dataclasses.dataclass
class SGSContract(contracts.Contract):
    integer_dtype: str = "int64"

    def empty_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "data": pd.Series(dtype="datetime64[ns]"),
                "valor": pd.Series(dtype="float64"),
                "codigo": pd.Series(dtype=self.integer_dtype),
                "nome_serie": pd.Series([""]).iloc[:0],
                "data_fim": pd.Series(dtype="datetime64[ns]"),
            }
        )

    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        for name, dtype in {
            "data": "datetime64[ns]",
            "valor": "float64",
            "codigo": self.integer_dtype,
            "data_fim": "datetime64[ns]",
        }.items():
            if name in df and str(df[name].dtype) != dtype:
                errors.append(f"Column '{name}' must use {dtype} dtype")
        if errors:
            return False, errors
        if not df["data"].eq(df["data"].dt.normalize()).all():
            errors.append("data must be a civil reference date without time of day")
        keys = df[["codigo", "data"]].reset_index(drop=True)
        if not keys.sort_values(["codigo", "data"]).reset_index(drop=True).equals(keys):
            errors.append("Rows must be sorted by codigo and data")
        if not all(isinstance(value, str) and value.strip() for value in df["nome_serie"].dropna()):
            errors.append("nome_serie must contain non-empty text or null")
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            date_dtype="datetime64[ns]",
            date_semantics="civil_reference_date_without_timezone",
            integer_dtype=self.integer_dtype,
            float_dtype="float64",
            sorted_by=["codigo", "data"],
            frequency="not_inferred_from_observation_dates",
        )
        return schema


BCB_SGS_V2 = SGSContract(
    name="bcb.sgs",
    version="2.1",
    effective_from="2.0.0",
    primary_key=["codigo", "data"],
    columns=[
        contracts.Column(name="data", type=contracts.ColumnType.DATE),
        contracts.Column(name="valor", type=contracts.ColumnType.FLOAT, nullable=True),
        contracts.Column(
            name="codigo", type=contracts.ColumnType.INTEGER, min_value=1, max_value=2**63 - 1
        ),
        contracts.Column(name="nome_serie", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(
            name="data_fim", type=contracts.ColumnType.DATE, nullable=True, stable=False
        ),
    ],
    guarantees=[
        "Quatro colunas estáveis, inclusive em consultas sem valores declarados pela fonte",
        "data_fim opcional, presente quando a fonte publica dataFim (fim do período, ex.: TR)",
        "Valores finitos, incluindo negativos legítimos; ausências explícitas permanecem nulas",
        "Uma observação por código e referência, ordenada antes de aplicar ultimos",
        "Referências repetidas entre blocos só são reconciliadas quando o valor coincide, com origem preservada",
        "Duplicata no mesmo corpo ou conflito entre blocos interrompe a aquisição",
        "Datas publicadas de referência não são cortadas por uma regra diária inferida",
        "Metadados distinguem blocos obtidos de completude da série, que não tem contagem global comprovada",
    ],
)

BCB_SGS_V3 = dataclasses.replace(copy.deepcopy(BCB_SGS_V2), version="3.0", integer_dtype="Int64")

contracts.register_contract("bcb_sgs", BCB_SGS_V3)
