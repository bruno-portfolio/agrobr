from __future__ import annotations

import re
from typing import Any

import pandas as pd

from agrobr import constants, contracts


class ComexstatContract(contracts.Contract):
    def empty_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                column.name: pd.Series(
                    dtype={
                        contracts.ColumnType.INTEGER: "Int64",
                        contracts.ColumnType.FLOAT: "float64",
                        contracts.ColumnType.STRING: "string[python]",
                    }[column.type]
                )
                for column in self.columns
            }
        )

    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        _, errors = super().validate(df)
        if list(df.columns) != self.list_columns():
            errors.append("Column names and order must match the Comex Stat schema exactly")
        if errors:
            return False, errors
        for column in self.columns:
            dtype = df[column.name].dtype
            if column.type == contracts.ColumnType.INTEGER and str(dtype) != "Int64":
                errors.append(f"Column '{column.name}' must use Int64 dtype")
            elif column.type == contracts.ColumnType.FLOAT and str(dtype) != "float64":
                errors.append(f"Column '{column.name}' must use float64 dtype")
            elif column.type == contracts.ColumnType.STRING and not (
                isinstance(dtype, pd.StringDtype) and dtype.storage == "python"
            ):
                errors.append(f"Column '{column.name}' must use string[python] dtype")
        if not errors:
            errors.extend(_code_errors(df))
            errors.extend(_volume_errors(df))
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            exact_column_order=True,
            integer_dtype="Int64",
            float_dtype="float64",
            string_dtype="string[python]",
            codes="Literal ASCII codes; leading zeros and special codes are preserved",
            urf="Unidade da Receita Federal; no physical port is inferred",
            missing_measures="Missing contributions propagate to the corresponding aggregate",
        )
        return schema


def _code_errors(frame: pd.DataFrame) -> list[str]:
    patterns = {
        constants.COMEXSTAT_RENAME_MAP[raw]: rf"[0-9]{{{width}}}"
        for raw, width in constants.COMEXSTAT_CODE_WIDTHS.items()
    }
    patterns.update(uf=r"[A-Z]{2}")
    return [
        f"Column '{name}' requires the literal pattern {pattern}"
        for name, pattern in patterns.items()
        if name in frame and not all(re.fullmatch(pattern, value) for value in frame[name])
    ]


def _volume_errors(frame: pd.DataFrame) -> list[str]:
    if "volume_ton" not in frame:
        return []
    for kg, tons in frame[["kg_liquido", "volume_ton"]].itertuples(index=False, name=None):
        if pd.isna(kg):
            if not pd.isna(tons):
                return ["Missing kg_liquido requires missing volume_ton"]
        elif pd.isna(tons) or int(kg) / 1000 != tons:
            return ["volume_ton must equal kg_liquido divided by 1000"]
    return []


def _column(name: str) -> contracts.Column:
    if name in ("ano", "mes", "qtd_estatistica", "kg_liquido"):
        return contracts.Column(
            name=name,
            type=contracts.ColumnType.INTEGER,
            nullable=name in ("qtd_estatistica", "kg_liquido"),
            min_value=1997 if name == "ano" else 1 if name == "mes" else 0,
            max_value=9999 if name == "ano" else 12 if name == "mes" else None,
            unit="kg" if name == "kg_liquido" else None,
        )
    if name.startswith("valor_") or name == "volume_ton":
        return contracts.Column(
            name=name,
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            min_value=0,
            unit="t" if name == "volume_ton" else "USD",
        )
    return contracts.Column(name=name, type=contracts.ColumnType.STRING)


def _trade_contract(fluxo: str, agregacao: str) -> ComexstatContract:
    properties = (
        constants.COMEXSTAT_EXPORT_PROPERTIES
        if fluxo == "exportacao"
        else constants.COMEXSTAT_IMPORT_PROPERTIES
    )
    names = [constants.COMEXSTAT_RENAME_MAP[raw] for raw in properties]
    key = ["ano", "mes", "ncm", "uf"]
    if agregacao == "mensal":
        names = [*key, "kg_liquido", "valor_fob_usd", "volume_ton"]
        if fluxo == "importacao":
            names += ["valor_frete_usd", "valor_seguro_usd"]
    return ComexstatContract(
        name=f"comexstat.{fluxo}.{agregacao}",
        version="2.0",
        effective_from="2.0.0",
        columns=[_column(name) for name in names],
        primary_key=key if agregacao == "mensal" else [],
        guarantees=[
            "Annual resource validated before any product or dimension filter",
            "Country, statistical unit, transport mode and Receita Federal codes retain leading zeros",
            "Detail preserves occurrences; monthly groups only year, month, NCM and UF",
            "Statistical quantities are available in detail with their reported unit",
            "Import freight and insurance are retained as separate USD measures",
            "Output schema and dtypes remain stable for an empty selection",
        ],
    )


def _dictionary_contract(tabela: str) -> ComexstatContract:
    return ComexstatContract(
        name=f"comexstat.dicionario.{tabela}",
        version="1.0",
        effective_from="2.0.0",
        columns=[
            _column(constants.COMEXSTAT_RENAME_MAP[raw])
            for raw in constants.COMEXSTAT_DICTIONARY_PROPERTIES[tabela]
        ],
        guarantees=[
            "All published dictionary fields and row order are preserved",
            "Names, whitespace, special codes and multiplicity remain literal",
            "MDIC codes are not deduplicated by ISO code or description",
            "The acquired dictionary does not establish historical validity or a snapshot",
        ],
    )


for _fluxo in ("exportacao", "importacao"):
    for _agregacao in ("mensal", "detalhado"):
        contracts.register_contract(
            f"comexstat_{_fluxo}_{_agregacao}", _trade_contract(_fluxo, _agregacao)
        )

for _tabela in constants.COMEXSTAT_DICTIONARY_PROPERTIES:
    contracts.register_contract(f"comexstat_dicionario_{_tabela}", _dictionary_contract(_tabela))
