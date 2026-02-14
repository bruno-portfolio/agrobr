from __future__ import annotations

from agrobr.contracts import (
    BreakingChangePolicy,
    Column,
    ColumnType,
    Contract,
    register_contract,
)

CEPEA_INDICADOR_V1 = Contract(
    name="cepea.indicador",
    version="1.0",
    effective_from="0.3.0",
    primary_key=["data", "produto"],
    columns=[
        Column(
            name="data",
            type=ColumnType.DATE,
            nullable=False,
            stable=True,
        ),
        Column(
            name="produto",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="praca",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=False,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
        Column(
            name="unidade",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="fonte",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="metodologia",
            type=ColumnType.STRING,
            nullable=True,
            stable=False,
        ),
        Column(
            name="anomalies",
            type=ColumnType.STRING,
            nullable=True,
            stable=False,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "Types only widen (int -> float, str -> categorical)",
        "Dates always in local timezone (Sao Paulo)",
        "Units explicit in 'unidade' column",
        "'valor' is always positive",
        "'data' is always a valid business day",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("preco_diario", CEPEA_INDICADOR_V1)

__all__ = ["CEPEA_INDICADOR_V1"]
