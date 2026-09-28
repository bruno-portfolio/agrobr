from __future__ import annotations

from agrobr import contracts


def _edition_columns() -> list[contracts.Column]:
    return [
        contracts.Column("ano_relatorio", contracts.ColumnType.INTEGER, min_value=2026),
        contracts.Column(
            "semana_relatorio", contracts.ColumnType.INTEGER, min_value=1, max_value=53
        ),
        contracts.Column("edicao_id", contracts.ColumnType.STRING),
        contracts.Column("publicado_em", contracts.ColumnType.DATETIME),
        contracts.Column("revisado_em", contracts.ColumnType.DATETIME),
    ]


EMBARQUES_MENSAIS_ANEC_V1 = contracts.Contract(
    name="embarques_mensais_anec",
    version="1.0",
    effective_from="2.0.0",
    primary_key=["edicao_id", "revisado_em", "ano", "mes", "produto"],
    columns=[
        contracts.Column("ano", contracts.ColumnType.INTEGER, min_value=1900),
        contracts.Column("mes", contracts.ColumnType.INTEGER, min_value=1, max_value=12),
        contracts.Column("produto", contracts.ColumnType.STRING),
        contracts.Column(
            "valor_ton", contracts.ColumnType.FLOAT, nullable=True, unit="ton", min_value=0
        ),
        contracts.Column("eh_estimativa", contracts.ColumnType.BOOLEAN),
        contracts.Column(
            "valor_min_ton", contracts.ColumnType.FLOAT, nullable=True, unit="ton", min_value=0
        ),
        contracts.Column(
            "valor_max_ton", contracts.ColumnType.FLOAT, nullable=True, unit="ton", min_value=0
        ),
        *_edition_columns(),
    ],
    guarantees=[
        "Volumes em toneladas; ausências não são convertidas em zero",
        "Faixas publicadas preservam os limites e deixam valor_ton ausente",
        "eh_estimativa indica a marca de programação do boletim; falso não garante mês realizado",
        "Edição e atualização do arquivo identificam a versão publicada",
    ],
)

COMPARACAO_ANUAL_ANEC_V1 = contracts.Contract(
    name="comparacao_anual_anec",
    version="1.0",
    effective_from="2.0.0",
    primary_key=["edicao_id", "revisado_em", "ano_base", "ano_comparacao", "mes", "produto"],
    columns=[
        contracts.Column("mes", contracts.ColumnType.INTEGER, min_value=1, max_value=12),
        contracts.Column("produto", contracts.ColumnType.STRING),
        contracts.Column("ano_base", contracts.ColumnType.INTEGER, min_value=1900),
        contracts.Column("ano_comparacao", contracts.ColumnType.INTEGER, min_value=1900),
        contracts.Column(
            "valor_base_ton", contracts.ColumnType.FLOAT, nullable=True, unit="ton", min_value=0
        ),
        contracts.Column(
            "valor_comparacao_ton",
            contracts.ColumnType.FLOAT,
            nullable=True,
            unit="ton",
            min_value=0,
        ),
        contracts.Column("eh_estimativa", contracts.ColumnType.BOOLEAN),
        *_edition_columns(),
    ],
    guarantees=[
        "Anos das duas séries explícitos; valores em toneladas",
        "eh_estimativa aplica-se ao ano_comparacao",
        "total_products preserva o agregado publicado separadamente dos produtos",
        "Uma observação por edição, atualização, par de anos, mês e produto",
    ],
)

DESTINOS_ANEC_V1 = contracts.Contract(
    name="destinos_anec",
    version="1.0",
    effective_from="2.0.0",
    primary_key=["edicao_id", "revisado_em", "produto", "destino"],
    columns=[
        contracts.Column("produto", contracts.ColumnType.STRING),
        contracts.Column("destino", contracts.ColumnType.STRING),
        contracts.Column(
            "share_pct",
            contracts.ColumnType.FLOAT,
            nullable=True,
            unit="%",
            min_value=0,
            max_value=100,
        ),
        contracts.Column("ano", contracts.ColumnType.INTEGER, nullable=True, min_value=1900),
        contracts.Column(
            "mes_inicio", contracts.ColumnType.INTEGER, nullable=True, min_value=1, max_value=12
        ),
        contracts.Column(
            "mes_fim", contracts.ColumnType.INTEGER, nullable=True, min_value=1, max_value=12
        ),
        *_edition_columns(),
    ],
    guarantees=[
        "Participações percentuais acumuladas no período indicado pelo boletim",
        "Período ausente no cabeçalho permanece nulo, sem inferência pela semana",
        "Arredondamentos da fonte não são corrigidos para forçar soma de 100%",
        "Destino pode incluir agregados como OTHERS",
    ],
)

contracts.register_contract("embarques_mensais_anec", EMBARQUES_MENSAIS_ANEC_V1)
contracts.register_contract("comparacao_anual_anec", COMPARACAO_ANUAL_ANEC_V1)
contracts.register_contract("destinos_anec", DESTINOS_ANEC_V1)
