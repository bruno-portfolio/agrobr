from __future__ import annotations

import copy
import dataclasses

from agrobr.contracts import (
    BreakingChangePolicy,
    Column,
    ColumnType,
    Contract,
    _legacy,
    register_contract,
)

IBGE_PAM_V2 = Contract(
    name="ibge.pam",
    version="2.2",
    effective_from="2.0.0",
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
            name="localidade_cod",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=False,
            description="Código IBGE da localidade (D1C do SIDRA); só nas linhas do IBGE.",
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
            unit="unidade_producao",
            stable=True,
            min_value=0,
        ),
        Column(
            name="rendimento",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="unidade_rendimento",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_producao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="unidade_valor_producao",
            stable=True,
            min_value=0,
        ),
        Column(
            name="fonte",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(name="unidade_producao", type=ColumnType.STRING, nullable=True, stable=False),
        Column(name="unidade_rendimento", type=ColumnType.STRING, nullable=True, stable=False),
        Column(name="unidade_valor_producao", type=ColumnType.STRING, nullable=True, stable=False),
        Column(name="condicao_produto", type=ColumnType.STRING, nullable=True, stable=False),
        Column(
            name="cod_municipio",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always a valid year (>= 1974)",
        "Numeric values are always >= 0",
        "As unidades variam por período e são identificadas nas colunas unidade_*",
        "Café até 2001 é em coco; desde 2002 é beneficiado, sem conversão implícita",
        "'fonte' identifies 'ibge_pam' or the 'conab' fallback",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_LSPA_V2 = Contract(
    name="ibge.lspa",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["ano", "mes", "produto", "localidade", "variavel"],
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
            name="variavel",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(name="localidade", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="localidade_cod", type=ColumnType.INTEGER, nullable=False, stable=True),
        Column(name="variavel_cod", type=ColumnType.INTEGER, nullable=False, stable=True),
        Column(name="unidade", type=ColumnType.STRING, nullable=False, stable=True),
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
        "'mes' is always present and between 1 and 12",
        "'fonte' is always 'ibge_lspa'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_PPM_V1 = Contract(
    name="ibge.ppm",
    version="1.1",
    effective_from="0.10.0",
    primary_key=["ano", "especie", "localidade"],
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
            name="localidade_cod",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
        ),
        Column(
            name="especie",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=True,
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
            name="cod_municipio",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always a valid year (>= 1974)",
        "Numeric values are always >= 0",
        "'fonte' is always 'ibge_ppm'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_ABATE_V1 = Contract(
    name="ibge.abate",
    version="1.0",
    effective_from="0.10.0",
    primary_key=["trimestre", "especie", "localidade"],
    columns=[
        Column(
            name="trimestre",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="localidade",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="localidade_cod",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
        ),
        Column(
            name="especie",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="animais_abatidos",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="cabeças",
            stable=True,
            min_value=0,
        ),
        Column(
            name="peso_carcacas",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="kg",
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
        "'trimestre' format is YYYYQQ (e.g. 202303)",
        "Numeric values are always >= 0",
        "'fonte' is always 'ibge_abate'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_ABATE_V2 = dataclasses.replace(
    copy.deepcopy(IBGE_ABATE_V1),
    version="2.0",
    effective_from="2.0.0",
    columns=[
        dataclasses.replace(column, type=ColumnType.INTEGER)
        if column.name == "animais_abatidos"
        else copy.deepcopy(column)
        for column in IBGE_ABATE_V1.columns
    ],
)

IBGE_CENSO_AGRO_V1 = Contract(
    name="ibge.censo_agro",
    version="1.2",
    effective_from="2.0.0",
    primary_key=["ano", "tema", "categoria", "variavel", "localidade"],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1995,
        ),
        Column(
            name="localidade",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="localidade_cod",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
        ),
        Column(
            name="tema",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="categoria",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="variavel",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=True,
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
            name="cod_municipio",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always a valid census year (>= 1995)",
        "Numeric values are always >= 0",
        "'fonte' is always 'ibge_censo_agro'",
        "'categoria' 'Total' is the source's own total row; it is not added to the other categories",
        "Establishment counts ('estabelecimentos') do not add up across categories",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_CENSO_AGRO_LEGADO_V2 = Contract(
    name="ibge.censo_agro_legado",
    version="2.1",
    effective_from="2.0.0",
    primary_key=["ano", "tema", "categoria", "variavel", "localidade", "uf"],
    columns=[
        Column(
            name="uf",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
            description="UF do diretório/cabeçalho oficial; nula para Brasil",
        ),
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1995,
            max_value=1995,
        ),
        Column(
            name="localidade",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="localidade_cod",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
        ),
        Column(
            name="tema",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="categoria",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="variavel",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=True,
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
            name="cod_municipio",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always 1995 (Censo 1995/96)",
        "Numeric values are always >= 0",
        "'fonte' is always 'ibge_censo_agro_legado'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_CENSO_AGRO_HISTORICO_V1 = Contract(
    name="ibge.censo_agro_historico",
    version="1.1",
    effective_from="0.13.0",
    primary_key=["ano", "tema", "categoria", "variavel", "localidade"],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1920,
        ),
        Column(
            name="localidade",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="localidade_cod",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
        ),
        Column(
            name="tema",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="categoria",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="variavel",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=True,
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
            name="cod_municipio",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always a valid census year (1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006)",
        "Numeric values are always >= 0",
        "'fonte' is always 'ibge_censo_agro_historico'",
        "Nível territorial máximo é UF (sem dados municipais)",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_CENSO_AGRO_MUNICIPAL_V2 = Contract(
    name="ibge.censo_agro_municipal_1985",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["volume", "tabela", "pagina_pdf", "linha", "coluna"],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1985,
            max_value=1985,
        ),
        Column(name="uf", type=ColumnType.STRING, nullable=False, stable=True),
        Column(
            name="volume",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Volume do IBGE de onde a casa foi lida (MG tem 2: n18_p1_mg e n18_p2_mg).",
        ),
        Column(
            name="tabela",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=67,
            max_value=119,
        ),
        Column(name="tema", type=ColumnType.STRING, nullable=False, stable=True),
        Column(
            name="pagina_pdf", type=ColumnType.INTEGER, nullable=False, stable=True, min_value=1
        ),
        Column(
            name="pagina_impressa",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
            description="Número impresso no rodapé; nulo quando o rodapé não foi lido nem se deduz das vizinhas.",
        ),
        Column(name="linha", type=ColumnType.INTEGER, nullable=False, stable=True, min_value=0),
        Column(
            name="coluna",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            description="Coluna física na página (0 à esquerda); negativa quando a célula não tem coluna identificada.",
        ),
        Column(name="nivel", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="localidade", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="coluna_nome", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="coluna_nome_lido", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="coluna_nome_status", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="variavel", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="unidade", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="unidade_lida", type=ColumnType.STRING, nullable=True, stable=True),
        Column(
            name="valor",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
            min_value=0,
            description="Só na casa confirmada pelas somas impressas (status confirmado_*).",
        ),
        Column(name="valor_lido", type=ColumnType.INTEGER, nullable=True, stable=True, min_value=0),
        Column(name="marcador", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="status", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="reparado", type=ColumnType.BOOLEAN, nullable=False, stable=True),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always 1985",
        "1 linha por casa do PDF: a do número e as sem leitura, incertas e fora de coluna, cada uma com o seu 'status'",
        "'valor' só é preenchido quando 'status' é confirmado_soma_exata ou confirmado_soma_arredondada_2a_compativel",
        "'valor_lido' traz a leitura sempre que houve leitura, confirmada ou não",
        "'coluna_nome' só é preenchido quando o nome foi confirmado ('coluna_nome_status' diferente de 'lido' e 'sem_nome')",
        "A chave (volume, tabela, pagina_pdf, linha, coluna) é única",
        "O tema de cada tabela vem do título impresso, igual em todos os volumes; o volume omite a tabela que não se aplica",
        "Sem código de município: os municípios de 1985 não correspondem 1:1 aos códigos atuais",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_SILVICULTURA_V1 = Contract(
    name="ibge.silvicultura",
    version="1.1",
    effective_from="0.15.0",
    primary_key=["ano", "produto", "localidade"],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1986,
        ),
        Column(
            name="localidade",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="localidade_cod",
            type=ColumnType.INTEGER,
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
            name="valor",
            type=ColumnType.FLOAT,
            nullable=True,
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
            name="cod_municipio",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always a valid year (>= 1986)",
        "Numeric values are always >= 0",
        "'fonte' is always 'ibge_silvicultura'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_EXTRACAO_VEGETAL_V1 = Contract(
    name="ibge.extracao_vegetal",
    version="1.1",
    effective_from="0.15.0",
    primary_key=["ano", "produto", "localidade"],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1986,
        ),
        Column(
            name="localidade",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="localidade_cod",
            type=ColumnType.INTEGER,
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
            name="valor",
            type=ColumnType.FLOAT,
            nullable=True,
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
            name="cod_municipio",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always a valid year (>= 1986)",
        "Numeric values are always >= 0",
        "'fonte' is always 'ibge_extracao_vegetal'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_LEITE_TRIMESTRAL_V1 = Contract(
    name="ibge.leite_trimestral",
    version="1.0",
    effective_from="0.15.0",
    primary_key=["trimestre", "localidade"],
    columns=[
        Column(
            name="trimestre",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="localidade",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="localidade_cod",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
        ),
        Column(
            name="leite_adquirido",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_litros",
            stable=True,
            min_value=0,
        ),
        Column(
            name="leite_industrializado",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_litros",
            stable=True,
            min_value=0,
        ),
        Column(
            name="preco_medio",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="reais_por_litro",
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
        "'trimestre' format is YYYYQQ (e.g. 202303)",
        "Numeric values are always >= 0",
        "'fonte' is always 'ibge_leite_trimestral'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)


register_contract("lspa", IBGE_LSPA_V2)
register_contract("silvicultura", IBGE_SILVICULTURA_V1)
register_contract("extrativismo_vegetal", IBGE_EXTRACAO_VEGETAL_V1)
register_contract("leite_industrial", IBGE_LEITE_TRIMESTRAL_V1)
register_contract("producao_anual", IBGE_PAM_V2)
register_contract("pecuaria_municipal", IBGE_PPM_V1)
register_contract("abate_trimestral", IBGE_ABATE_V2)
register_contract("censo_agropecuario", IBGE_CENSO_AGRO_V1)
register_contract("censo_agropecuario_legado", IBGE_CENSO_AGRO_LEGADO_V2)
register_contract("censo_agropecuario_historico", IBGE_CENSO_AGRO_HISTORICO_V1)
register_contract("censo_agropecuario_municipal_1985", IBGE_CENSO_AGRO_MUNICIPAL_V2)

__all__ = [
    "IBGE_ABATE_V1",
    "IBGE_ABATE_V2",
    "IBGE_CENSO_AGRO_HISTORICO_V1",
    "IBGE_CENSO_AGRO_LEGADO_V2",
    "IBGE_CENSO_AGRO_MUNICIPAL_V2",
    "IBGE_CENSO_AGRO_V1",
    "IBGE_EXTRACAO_VEGETAL_V1",
    "IBGE_LEITE_TRIMESTRAL_V1",
    "IBGE_LSPA_V2",
    "IBGE_PAM_V2",
    "IBGE_PPM_V1",
    "IBGE_SILVICULTURA_V1",
]


def __getattr__(name: str) -> Contract:
    return _legacy.resolve(
        name,
        module=__name__,
        names=frozenset(
            [
                "IBGE_LSPA_V1",
                "IBGE_CENSO_AGRO_LEGADO_V1",
                "IBGE_PAM_V1",
                "IBGE_CENSO_AGRO_MUNICIPAL_V1",
            ]
        ),
    )
