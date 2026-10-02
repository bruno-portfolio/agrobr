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
    version="1.1",
    effective_from="2.0.0",
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
            unit="BRL por unidade da coluna unidade; algodão em centavos de BRL por libra-peso (cBRL/lb)",
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
        Column(
            name="valor_usd",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="USD",
            stable=False,
            min_value=0,
            description="Preço em dólar publicado na mesma linha pelo CEPEA; nulo quando a fonte não divulga.",
        ),
        Column(
            name="peso_medio_kg",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="kg",
            stable=False,
            min_value=0,
            description="Bezerro MS: peso médio da tabela auxiliar do CEPEA; nulo para os demais produtos.",
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
