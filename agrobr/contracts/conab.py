from __future__ import annotations

from agrobr.contracts import (
    BreakingChangePolicy,
    Column,
    ColumnType,
    Contract,
    register_contract,
)

CONAB_SAFRA_V1 = Contract(
    name="conab.safras",
    version="1.0",
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
            nullable=False,
            stable=True,
            min_value=1,
            max_value=12,
        ),
        Column(
            name="data_publicacao",
            type=ColumnType.DATE,
            nullable=False,
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

CONAB_CUSTO_PRODUCAO_V1 = Contract(
    name="conab.custo_producao",
    version="1.0",
    effective_from="0.10.0",
    primary_key=["cultura", "uf", "safra", "categoria", "item"],
    columns=[
        Column(
            name="cultura",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="uf",
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
            name="tecnologia",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="categoria",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="item",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="unidade",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="quantidade_ha",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="unidade/ha",
            stable=True,
            min_value=0,
        ),
        Column(
            name="preco_unitario",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_ha",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="BRL/ha",
            stable=True,
            min_value=0,
        ),
        Column(
            name="participacao_pct",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="%",
            stable=True,
            min_value=0,
            max_value=100,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'safra' always matches pattern YYYY/YY",
        "'uf' is always a valid Brazilian state code",
        "Numeric values are always >= 0",
        "'participacao_pct' is between 0 and 100",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("estimativa_safra", CONAB_SAFRA_V1)
register_contract("balanco", CONAB_BALANCO_V1)
register_contract("custo_producao", CONAB_CUSTO_PRODUCAO_V1)

__all__ = ["CONAB_BALANCO_V1", "CONAB_CUSTO_PRODUCAO_V1", "CONAB_SAFRA_V1"]
