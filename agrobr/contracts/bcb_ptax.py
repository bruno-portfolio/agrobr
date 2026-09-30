from __future__ import annotations

import re
from typing import Any

import pandas as pd

from agrobr import constants, contracts


class PtaxContract(contracts.Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        for column in self.columns:
            expected = (
                "datetime64[ns]"
                if column.type in (contracts.ColumnType.DATE, contracts.ColumnType.DATETIME)
                else "float64"
                if column.type == contracts.ColumnType.FLOAT
                else None
            )
            if (
                expected is not None
                and column.name in df
                and str(df[column.name].dtype) != expected
            ):
                errors.append(f"Column '{column.name}' must use {expected} dtype")
        if errors:
            return False, errors
        if not all(
            re.fullmatch(constants.BCB_PTAX_CURRENCY_PATTERN, value) for value in df["moeda"]
        ):
            errors.append("moeda must be an uppercase three-letter ASCII symbol")
        if any(column.name == "data_hora" for column in self.columns):
            if not df["data"].eq(df["data_hora"].dt.normalize()).all():
                errors.append("data must be the civil date of data_hora")
            if not df["data_hora"].is_monotonic_increasing:
                errors.append("Published quote timestamps must be ordered ascending")
        else:
            if not df["moeda"].is_monotonic_increasing:
                errors.append("Currency symbols must be ordered ascending")
            for column_name in ("nome", "tipo_moeda"):
                if not all(value.strip() for value in df[column_name]):
                    errors.append(f"{column_name} must contain non-empty text")
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            currency_pattern=constants.BCB_PTAX_CURRENCY_PATTERN,
            currency_catalog="current_OData_Moedas; not_a_historical_currency_inventory",
        )
        if any(column.name == "data_hora" for column in self.columns):
            schema["constraints"].update(
                date_dtype="datetime64[ns]",
                timestamp_semantics="published_naive_timestamp; timezone_not_in_payload",
                civil_date="data_hora.normalize()",
                timestamp_order="ascending",
                timestamp_precision="up_to_nanoseconds_without_truncation",
                float_dtype="float64",
                quote_unit="domestic_currency_at_reference_date_per_unit_of_selected_currency",
                parity_unit={"A": "selected_currency/USD", "B": "USD/selected_currency"},
                bulletin_text="published_label_preserved; day_and_period_routes_may_differ",
                blank_bulletin="published_unknown_label; distinct_from_null",
                key_scope="selected_route_acquisition",
                statistical_consistency="finite_source_values_preserved_with_diagnostics",
            )
        else:
            schema["constraints"].update(currency_order="ascending", currency_type="published_text")
        return schema


BCB_PTAX_V2 = PtaxContract(
    name="bcb.ptax",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["moeda", "data_hora", "tipo_boletim"],
    columns=[
        contracts.Column(name="cotacao_compra", type=contracts.ColumnType.FLOAT, nullable=True),
        contracts.Column(name="cotacao_venda", type=contracts.ColumnType.FLOAT, nullable=True),
        contracts.Column(name="data_hora", type=contracts.ColumnType.DATETIME),
        contracts.Column(name="data", type=contracts.ColumnType.DATE),
        contracts.Column(name="moeda", type=contracts.ColumnType.STRING),
        contracts.Column(name="paridade_compra", type=contracts.ColumnType.FLOAT, nullable=True),
        contracts.Column(name="paridade_venda", type=contracts.ColumnType.FLOAT, nullable=True),
        contracts.Column(name="tipo_boletim", type=contracts.ColumnType.STRING, nullable=True),
    ],
    guarantees=[
        "Oito colunas estáveis, inclusive em resultado vazio",
        "Moeda selecionada consta do catálogo OData obtido na aquisição",
        "Boletins conservam o texto publicado e a precisão do horário sem atribuir fuso",
        "Data civil e instante UTC de aquisição são dimensões distintas",
        "Valores publicados não são convertidos, imputados ou recalculados",
        "Todas as páginas são validadas antes da seleção de boletim, sem retorno após falha parcial",
        "Contagem da fonte, filtro de boletim e ausência de total independente são explícitos",
    ],
)

BCB_PTAX_MOEDAS_V1 = PtaxContract(
    name="bcb.ptax_moedas",
    version="1.0",
    effective_from="2.0.0",
    primary_key=["moeda"],
    columns=[
        contracts.Column(name="moeda", type=contracts.ColumnType.STRING),
        contracts.Column(name="nome", type=contracts.ColumnType.STRING),
        contracts.Column(name="tipo_moeda", type=contracts.ColumnType.STRING),
    ],
    guarantees=[
        "Três colunas textuais estáveis, inclusive em resultado vazio",
        "Símbolo, nome e tipo preservados sem substituir o catálogo por lista fixa",
        "Símbolos únicos, ordenados, com contexto da resposta e aquisição UTC",
        "Catálogo corrente não comprova vigência histórica ou cobertura da tabela geral de moedas",
    ],
)

contracts.register_contract("bcb_ptax", BCB_PTAX_V2)
contracts.register_contract("bcb_ptax_moedas", BCB_PTAX_MOEDAS_V1)
