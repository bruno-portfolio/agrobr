"""Contratos de estabilidade para dados CEPEA."""

from agrobr.contracts import BreakingChangePolicy, Column, ColumnType, Contract

CEPEA_INDICADOR_V1 = Contract(
    name="cepea.indicador",
    version="1.0",
    effective_from="0.3.0",
    columns=[
        Column(
            name="data",
            type=ColumnType.DATE,
            nullable=False,
            stable=True,
            description="Data do indicador",
        ),
        Column(
            name="produto",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Nome do produto (soja, milho, etc)",
        ),
        Column(
            name="praca",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
            description="Praca de referencia",
        ),
        Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=False,
            unit="BRL",
            stable=True,
            description="Preco em reais",
        ),
        Column(
            name="unidade",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Unidade do preco (BRL/sc60kg, BRL/@, etc)",
        ),
        Column(
            name="fonte",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Fonte dos dados",
        ),
        Column(
            name="metodologia",
            type=ColumnType.STRING,
            nullable=True,
            stable=False,
            description="Descricao da metodologia",
        ),
        Column(
            name="anomalies",
            type=ColumnType.STRING,
            nullable=True,
            stable=False,
            description="Lista de anomalias detectadas",
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


__all__ = ["CEPEA_INDICADOR_V1"]
