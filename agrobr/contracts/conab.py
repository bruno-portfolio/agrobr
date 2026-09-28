from __future__ import annotations

from agrobr.contracts import (
    BreakingChangePolicy,
    Column,
    ColumnType,
    Contract,
    _legacy,
    register_contract,
)

CONAB_SAFRA_V2 = Contract(
    name="conab.safras",
    version="2.0",
    effective_from="0.3.0",
    primary_key=["safra", "produto", "uf", "levantamento"],
    columns=[
        Column(
            name="fonte",
            type=ColumnType.STRING,
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
            name="safra",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="uf",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="area_plantada",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_ha",
            stable=True,
            min_value=0,
        ),
        Column(
            name="area_colhida",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_ha",
            stable=True,
            min_value=0,
        ),
        Column(
            name="produtividade",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="kg/ha",
            stable=True,
            min_value=0,
        ),
        Column(
            name="producao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_ton",
            stable=True,
            min_value=0,
        ),
        Column(
            name="levantamento",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
            min_value=1,
            max_value=12,
        ),
        Column(
            name="data_publicacao",
            type=ColumnType.DATE,
            nullable=True,
            stable=True,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'safra' always matches pattern YYYY/YY",
        "'uf' is always a valid Brazilian state code",
        "'levantamento' is between 1 and 12",
        "Numeric values are always >= 0",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

CONAB_BALANCO_V1 = Contract(
    name="conab.balanco",
    version="1.0",
    effective_from="0.3.0",
    primary_key=["safra", "produto"],
    columns=[
        Column(
            name="produto",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="safra",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="estoque_inicial",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_ton",
            stable=True,
            min_value=0,
        ),
        Column(
            name="producao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_ton",
            stable=True,
            min_value=0,
        ),
        Column(
            name="importacao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_ton",
            stable=True,
            min_value=0,
        ),
        Column(
            name="suprimento",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_ton",
            stable=True,
            min_value=0,
        ),
        Column(
            name="consumo",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_ton",
            stable=True,
            min_value=0,
        ),
        Column(
            name="exportacao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_ton",
            stable=True,
            min_value=0,
        ),
        Column(
            name="estoque_final",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_ton",
            stable=True,
            min_value=0,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "All numeric values represent thousands of tons",
        "suprimento = estoque_inicial + producao + importacao",
        "estoque_final = suprimento - consumo - exportacao",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)


CONAB_BALANCO_V1_1 = Contract(
    name=CONAB_BALANCO_V1.name,
    version="1.1",
    effective_from="2.0.0",
    primary_key=list(CONAB_BALANCO_V1.primary_key),
    columns=[
        *CONAB_BALANCO_V1.columns,
        Column(
            name="demanda_total",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_ton",
            stable=False,
            min_value=0,
            description="Demanda publicada; nula no balanço wide e no layout long antigo",
        ),
        Column(
            name="levantamento",
            type=ColumnType.STRING,
            nullable=True,
            stable=False,
            description="Rótulo textual da revisão da linha; nulo quando não publicado",
        ),
        Column(
            name="unidade",
            type=ColumnType.STRING,
            nullable=False,
            stable=False,
            description="Unidade das métricas: mil_ton",
        ),
        Column(
            name="fonte",
            type=ColumnType.STRING,
            nullable=False,
            stable=False,
            description="Fonte selecionada pelo dataset: conab",
        ),
    ],
    guarantees=[
        *CONAB_BALANCO_V1.guarantees,
        "Numeric output columns use float64, including demanda_total",
        "Unpublished demand and revision remain null",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("balanco", CONAB_BALANCO_V1_1)

__all__ = ["CONAB_BALANCO_V1", "CONAB_BALANCO_V1_1", "CONAB_SAFRA_V2"]


def __getattr__(name: str) -> Contract:
    return _legacy.resolve(
        name,
        module=__name__,
        names=frozenset(["CONAB_SAFRA_V1", "CONAB_CUSTO_PRODUCAO_V1", "CONAB_CUSTO_PRODUCAO_V2"]),
    )
