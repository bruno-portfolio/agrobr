from __future__ import annotations

from agrobr.contracts import (
    BreakingChangePolicy,
    Column,
    ColumnType,
    Contract,
    register_contract,
)

CREDITO_RURAL_V1 = Contract(
    name="bcb.credito_rural",
    version="1.0",
    effective_from="0.10.0",
    primary_key=["safra", "produto", "uf", "finalidade"],
    columns=[
        Column(
            name="safra",
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
            name="uf",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="finalidade",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="agregacao",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="volume",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'safra' always matches pattern YYYY/YY",
        "'uf' is always a valid Brazilian state code when present",
        "Numeric values are always >= 0",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

EXPORTACAO_V1 = Contract(
    name="comexstat.exportacao",
    version="1.0",
    effective_from="0.10.0",
    primary_key=["ano", "mes", "produto", "uf"],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1997,
        ),
        Column(
            name="mes",
            type=ColumnType.INTEGER,
            nullable=False,
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
            name="uf",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="kg_liquido",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="kg",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_fob_usd",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="USD",
            stable=True,
            min_value=0,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always >= 1997",
        "'mes' is between 1 and 12",
        "Numeric values are always >= 0",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

FERTILIZANTE_V1 = Contract(
    name="anda.fertilizante",
    version="1.0",
    effective_from="0.10.0",
    primary_key=["ano", "mes", "uf", "produto_fertilizante"],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=2000,
        ),
        Column(
            name="mes",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1,
            max_value=12,
        ),
        Column(
            name="uf",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="produto_fertilizante",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="volume_ton",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ton",
            stable=True,
            min_value=0,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always >= 2000",
        "'mes' is between 1 and 12",
        "Numeric values are always >= 0",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

FOCOS_QUEIMADAS_V1 = Contract(
    name="queimadas.focos",
    version="1.0",
    effective_from="0.10.0",
    primary_key=["data", "lat", "lon", "satelite", "hora_gmt"],
    columns=[
        Column(
            name="data",
            type=ColumnType.DATE,
            nullable=False,
            stable=True,
        ),
        Column(
            name="hora_gmt",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="lat",
            type=ColumnType.FLOAT,
            nullable=False,
            stable=True,
            min_value=-35.0,
            max_value=6.0,
        ),
        Column(
            name="lon",
            type=ColumnType.FLOAT,
            nullable=False,
            stable=True,
            min_value=-74.0,
            max_value=-30.0,
        ),
        Column(
            name="satelite",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="municipio",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="municipio_id",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
        ),
        Column(
            name="estado",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="uf",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="bioma",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="numero_dias_sem_chuva",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=0,
        ),
        Column(
            name="precipitacao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mm",
            stable=True,
            min_value=0,
        ),
        Column(
            name="risco_fogo",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=0,
            max_value=1,
        ),
        Column(
            name="frp",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="MW",
            stable=True,
            min_value=0,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'lat' is always between -35 and 6 (Brazil bounding box)",
        "'lon' is always between -74 and -30 (Brazil bounding box)",
        "'data' is always a valid date",
        "Numeric values are always >= 0 when present",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("credito_rural", CREDITO_RURAL_V1)
register_contract("exportacao", EXPORTACAO_V1)
register_contract("fertilizante", FERTILIZANTE_V1)
register_contract("focos_queimadas", FOCOS_QUEIMADAS_V1)

__all__ = [
    "CREDITO_RURAL_V1",
    "EXPORTACAO_V1",
    "FERTILIZANTE_V1",
    "FOCOS_QUEIMADAS_V1",
]
