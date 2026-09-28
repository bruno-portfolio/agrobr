from __future__ import annotations

from dataclasses import replace

from agrobr import contracts
from agrobr.contracts import conab

ESTIMATIVA_SAFRA_V3_1 = contracts.Contract(
    name="estimativa_safra",
    version="3.1",
    effective_from="2.0.0",
    primary_key=["fonte", "safra", "produto", "uf", "levantamento", "ano_lspa", "mes_lspa"],
    columns=[
        *[replace(column) for column in conab.CONAB_SAFRA_V2.columns],
        contracts.Column(
            "ano_lspa",
            contracts.ColumnType.INTEGER,
            nullable=True,
            min_value=1900,
            description="Ano civil efetivo do LSPA; nulo para CONAB",
        ),
        contracts.Column(
            "mes_lspa",
            contracts.ColumnType.INTEGER,
            nullable=True,
            min_value=1,
            max_value=12,
            description="Mês efetivo do LSPA; não corresponde ao levantamento CONAB",
        ),
        contracts.Column(
            "unidade_producao",
            contracts.ColumnType.STRING,
            stable=False,
            description="Unidade da produção: mil_ton nas 2 fontes, como em conab.brasil_total",
        ),
        contracts.Column(
            "unidade_area",
            contracts.ColumnType.STRING,
            stable=False,
            description="Unidade das áreas plantada e colhida: mil_ha nas 2 fontes",
        ),
    ],
    guarantees=[
        "Chave distingue fontes, levantamentos CONAB e meses LSPA",
        "Seleção temporal explícita nunca é substituída por outra referência em fallback",
        "LSPA conserva ano e mês efetivamente retornados; safra usa o ano civil como ano final",
        "Sub-safras LSPA incompletas não são apresentadas como totais",
        "Áreas em mil ha, produção em mil ton e produtividade em kg/ha nas 2 fontes; unidade_area e unidade_producao declaram a escala",
        "Valores ausentes não são convertidos em zero",
    ],
)

contracts.register_contract("estimativa_safra", ESTIMATIVA_SAFRA_V3_1)
