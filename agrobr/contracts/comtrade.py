from __future__ import annotations

import re
from typing import Any

import pandas as pd

from agrobr import constants, contracts


class ComtradeContract(contracts.Contract):
    def empty_frame(self) -> pd.DataFrame:
        df = super().empty_frame()
        for column in self.columns:
            if column.type == contracts.ColumnType.FLOAT:
                df[column.name] = pd.Series(dtype="float64")
        return df

    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        dtypes = {
            contracts.ColumnType.INTEGER: "Int64",
            contracts.ColumnType.FLOAT: "float64",
            contracts.ColumnType.BOOLEAN: "boolean",
        }
        for column in self.columns:
            if (
                column.name in df
                and column.type in dtypes
                and str(df[column.name].dtype) != dtypes[column.type]
            ):
                errors.append(f"Column '{column.name}' must use {dtypes[column.type]} dtype")
        if errors:
            return False, errors
        errors.extend(self._validate_dimensions(df))
        return not errors, errors

    def _validate_dimensions(self, df: pd.DataFrame) -> list[str]:
        errors: list[str] = []
        if not all(re.fullmatch(constants.COMTRADE_HS_PATTERN, value) for value in df["hs_code"]):
            errors.append("hs_code requires 2, 4 or 6 ASCII digits")
        for column in self.columns:
            if (
                column.name.startswith("classificacao")
                and "original" not in column.name
                and not all(
                    re.fullmatch(constants.COMTRADE_CLASSIFICATION_PATTERN, value)
                    for value in df[column.name].dropna()
                )
            ):
                errors.append(f"Column '{column.name}' requires the reported Hn revision")
        errors.extend(_validate_periods(df))
        if "fluxo_code" in df and not df["fluxo_code"].isin(["X", "M"]).all():
            errors.append("fluxo_code requires X or M")
        if "nivel_hs" in df and not (df["nivel_hs"] == df["hs_code"].str.len()).all():
            errors.append("nivel_hs must equal the HS code length")
        if {"classificacao_reporter", "classificacao_partner"}.issubset(df.columns):
            both = df.dropna(subset=["classificacao_reporter", "classificacao_partner"])
            if not (both["classificacao_reporter"] == both["classificacao_partner"]).all():
                errors.append("Mirror legs require matching reported HS revisions")
        return errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            integer_dtype="Int64",
            float_dtype="float64",
            boolean_dtype="boolean",
            hs_code_pattern=f"^{constants.COMTRADE_HS_PATTERN}$",
            classification_pattern=f"^{constants.COMTRADE_CLASSIFICATION_PATTERN}$",
            period_semantics="YYYY annual or YYYYMM monthly, coherent with ano and mes",
            country_identity="numeric codes; descriptive ISO may be absent",
        )
        return schema


def _validate_periods(df: pd.DataFrame) -> list[str]:
    for period, year, month in df[["periodo", "ano", "mes"]].itertuples(index=False, name=None):
        if not re.fullmatch(r"[0-9]{4}(?:[0-9]{2})?", period) or int(period[:4]) != year:
            return ["periodo requires a civil YYYY or YYYYMM coherent with ano"]
        if len(period) == 4 and not pd.isna(month):
            return ["Annual periodo requires a null mes"]
        if len(period) == 6 and (pd.isna(month) or int(period[4:]) != month):
            return ["Monthly periodo requires a coherent mes"]
    return []


COMERCIO_BILATERAL_V2 = ComtradeContract(
    name="comtrade.comercio",
    version="2.1",
    effective_from="2.0.0",
    primary_key=[
        "periodo",
        "reporter_code",
        "partner_code",
        "hs_code",
        "fluxo_code",
        "classificacao",
    ],
    columns=[
        contracts.Column(name="periodo", type=contracts.ColumnType.STRING),
        contracts.Column(
            name="ano", type=contracts.ColumnType.INTEGER, min_value=1, max_value=9999
        ),
        contracts.Column(
            name="mes", type=contracts.ColumnType.INTEGER, nullable=True, min_value=1, max_value=12
        ),
        contracts.Column(name="reporter_code", type=contracts.ColumnType.INTEGER, min_value=1),
        contracts.Column(name="reporter_iso", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="reporter", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="partner_code", type=contracts.ColumnType.INTEGER, min_value=0),
        contracts.Column(name="partner_iso", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="partner", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="fluxo_code", type=contracts.ColumnType.STRING),
        contracts.Column(name="fluxo", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="hs_code", type=contracts.ColumnType.STRING),
        contracts.Column(name="produto_desc", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(
            name="nivel_hs", type=contracts.ColumnType.INTEGER, min_value=2, max_value=6
        ),
        contracts.Column(
            name="peso_liquido_kg",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="kg",
            min_value=0,
        ),
        contracts.Column(
            name="peso_bruto_kg",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="kg",
            min_value=0,
        ),
        contracts.Column(
            name="volume_ton",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="ton",
            min_value=0,
        ),
        contracts.Column(
            name="valor_fob_usd",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="USD",
            min_value=0,
        ),
        contracts.Column(
            name="valor_cif_usd",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="USD",
            min_value=0,
        ),
        contracts.Column(
            name="valor_primario_usd",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="USD",
            min_value=0,
        ),
        contracts.Column(
            name="quantidade", type=contracts.ColumnType.FLOAT, nullable=True, min_value=0
        ),
        contracts.Column(name="unidade_qtd", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="classificacao", type=contracts.ColumnType.STRING),
        contracts.Column(
            name="classificacao_original", type=contracts.ColumnType.BOOLEAN, nullable=True
        ),
        contracts.Column(
            name="peso_liquido_estimado", type=contracts.ColumnType.BOOLEAN, nullable=True
        ),
        contracts.Column(
            name="peso_bruto_estimado", type=contracts.ColumnType.BOOLEAN, nullable=True
        ),
        contracts.Column(
            name="quantidade_estimada", type=contracts.ColumnType.BOOLEAN, nullable=True
        ),
    ],
    guarantees=[
        "Identidade por códigos numéricos e revisão HS efetivamente publicada",
        "World é o agregado explícito; all preserva parceiros publicados sem somar totais",
        "27 colunas estáveis, inclusive em resultados vazios",
        "Marcas de estimativa da ONU (isNetWgtEstimated, isGrossWgtEstimated e isQtyEstimated) "
        "preservadas como flags anuláveis",
        "Inteiros Int64, medidas float64 e flags boolean anuláveis",
        "Ausências não são convertidas em zero; quantidades e valores são finitos e não negativos",
        "Cobertura operacional comprovada por countOnly e partições consta dos metadados",
    ],
)

TRADE_MIRROR_V2 = ComtradeContract(
    name="comtrade.trade_mirror",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["periodo", "hs_code", "reporter_code", "partner_code"],
    columns=[
        contracts.Column(name="periodo", type=contracts.ColumnType.STRING),
        contracts.Column(
            name="ano", type=contracts.ColumnType.INTEGER, min_value=1, max_value=9999
        ),
        contracts.Column(
            name="mes", type=contracts.ColumnType.INTEGER, nullable=True, min_value=1, max_value=12
        ),
        contracts.Column(name="hs_code", type=contracts.ColumnType.STRING),
        contracts.Column(name="produto_desc", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="reporter_iso", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="partner_iso", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(
            name="peso_liquido_kg_reporter",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="kg",
            min_value=0,
        ),
        contracts.Column(
            name="valor_fob_usd_reporter",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="USD",
            min_value=0,
        ),
        contracts.Column(
            name="volume_ton_reporter",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="ton",
            min_value=0,
        ),
        contracts.Column(
            name="peso_liquido_kg_partner",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="kg",
            min_value=0,
        ),
        contracts.Column(
            name="valor_fob_usd_partner",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="USD",
            min_value=0,
        ),
        contracts.Column(
            name="valor_cif_usd_partner",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="USD",
            min_value=0,
        ),
        contracts.Column(
            name="volume_ton_partner",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="ton",
            min_value=0,
        ),
        contracts.Column(
            name="diff_peso_kg", type=contracts.ColumnType.FLOAT, nullable=True, unit="kg"
        ),
        contracts.Column(
            name="diff_valor_fob_usd", type=contracts.ColumnType.FLOAT, nullable=True, unit="USD"
        ),
        contracts.Column(
            name="ratio_valor", type=contracts.ColumnType.FLOAT, nullable=True, min_value=0
        ),
        contracts.Column(
            name="ratio_peso", type=contracts.ColumnType.FLOAT, nullable=True, min_value=0
        ),
        contracts.Column(
            name="classificacao_reporter", type=contracts.ColumnType.STRING, nullable=True
        ),
        contracts.Column(
            name="classificacao_partner", type=contracts.ColumnType.STRING, nullable=True
        ),
        contracts.Column(
            name="classificacao_original_reporter", type=contracts.ColumnType.BOOLEAN, nullable=True
        ),
        contracts.Column(
            name="classificacao_original_partner", type=contracts.ColumnType.BOOLEAN, nullable=True
        ),
        contracts.Column(name="reporter_code", type=contracts.ColumnType.INTEGER, min_value=1),
        contracts.Column(name="partner_code", type=contracts.ColumnType.INTEGER, min_value=1),
    ],
    guarantees=[
        "Junção externa 1:1 por período e HS entre exportação e importação inversa",
        "Revisões HS diferentes na mesma célula geram erro; não se declara harmonização",
        "24 colunas estáveis, com códigos numéricos dos países e classificação por perna",
        "Perna ausente permanece nula e não produz zero nem ratio infinito",
        "Metadados preservam as duas aquisições e sua cobertura conjunta",
    ],
)

contracts.register_contract("comercio_bilateral", COMERCIO_BILATERAL_V2)
contracts.register_contract("comercio_internacional", COMERCIO_BILATERAL_V2)
contracts.register_contract("trade_mirror", TRADE_MIRROR_V2)
