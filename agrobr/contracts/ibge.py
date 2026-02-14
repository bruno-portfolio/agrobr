from __future__ import annotations

from agrobr.contracts import (
    BreakingChangePolicy,
    Column,
    ColumnType,
    Contract,
    register_contract,
)

IBGE_PAM_V1 = Contract(
    name="ibge.pam",
    version="1.0",
    effective_from="0.3.0",
    primary_key=["ano", "produto", "localidade"],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1974,
        ),
        Column(
            name="localidade",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="produto",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="area_plantada",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ha",
            stable=True,
            min_value=0,
        ),
        Column(
            name="area_colhida",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ha",
            stable=True,
            min_value=0,
        ),
        Column(
            name="producao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ton",
            stable=True,
            min_value=0,
        ),
        Column(
            name="rendimento",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="kg/ha",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_producao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_reais",
            stable=True,
            min_value=0,
        ),
        Column(
            name="fonte",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always a valid year (>= 1974)",
        "Numeric values are always >= 0",
        "'fonte' is always 'ibge_pam'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_LSPA_V1 = Contract(
    name="ibge.lspa",
    version="1.0",
    effective_from="0.3.0",
    primary_key=["ano", "mes", "produto"],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1974,
        ),
        Column(
            name="mes",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
            min_value=1,
            max_value=12,
        ),
        Column(
            name="produto",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="variavel",
            type=ColumnType.STRING,
            nullable=True,
            stable=False,
        ),
        Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=False,
        ),
        Column(
            name="fonte",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always a valid year",
        "'mes' is between 1 and 12 when present",
        "'fonte' is always 'ibge_lspa'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("producao_anual", IBGE_PAM_V1)

__all__ = ["IBGE_LSPA_V1", "IBGE_PAM_V1"]
