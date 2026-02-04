"""Contratos de estabilidade para dados IBGE."""

from agrobr.contracts import BreakingChangePolicy, Column, ColumnType, Contract

IBGE_PAM_V1 = Contract(
    name="ibge.pam",
    version="1.0",
    effective_from="0.3.0",
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            description="Ano de referencia",
        ),
        Column(
            name="localidade",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
            description="Nome da localidade (UF ou municipio)",
        ),
        Column(
            name="produto",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Nome do produto",
        ),
        Column(
            name="area_plantada",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ha",
            stable=True,
            description="Area plantada em hectares",
        ),
        Column(
            name="area_colhida",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ha",
            stable=True,
            description="Area colhida em hectares",
        ),
        Column(
            name="producao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ton",
            stable=True,
            description="Quantidade produzida em toneladas",
        ),
        Column(
            name="rendimento",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="kg/ha",
            stable=True,
            description="Rendimento medio em kg/ha",
        ),
        Column(
            name="valor_producao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_reais",
            stable=True,
            description="Valor da producao em mil reais",
        ),
        Column(
            name="fonte",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Fonte dos dados (ibge_pam)",
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
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            description="Ano de referencia",
        ),
        Column(
            name="mes",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
            description="Mes de referencia (1-12)",
        ),
        Column(
            name="produto",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Nome do produto",
        ),
        Column(
            name="variavel",
            type=ColumnType.STRING,
            nullable=True,
            stable=False,
            description="Nome da variavel",
        ),
        Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=False,
            description="Valor da variavel",
        ),
        Column(
            name="fonte",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Fonte dos dados (ibge_lspa)",
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


__all__ = ["IBGE_PAM_V1", "IBGE_LSPA_V1"]
