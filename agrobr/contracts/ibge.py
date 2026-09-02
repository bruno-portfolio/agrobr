from __future__ import annotations

from agrobr.contracts import (
    BreakingChangePolicy,
    Column,
    ColumnType,
    Contract,
    register_contract,
)

IBGE_PAM_V1 = Contract(
    name="ibge.pam",
    version="1.0",
    effective_from="0.3.0",
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
            unit="ton",
            stable=True,
            min_value=0,
        ),
        Column(
            name="rendimento",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="kg/ha",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_producao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="mil_reais",
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
        "'ano' is always a valid year (>= 1974)",
        "Numeric values are always >= 0",
        "'fonte' identifies 'ibge_pam' or the 'conab' fallback",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_LSPA_V1 = Contract(
    name="ibge.lspa",
    version="1.0",
    effective_from="0.3.0",
    primary_key=["ano", "mes", "produto"],
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
            nullable=True,
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
            nullable=True,
            stable=False,
        ),
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
        "'mes' is between 1 and 12 when present",
        "'fonte' is always 'ibge_lspa'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_PPM_V1 = Contract(
    name="ibge.ppm",
    version="1.0",
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

IBGE_CENSO_AGRO_V1 = Contract(
    name="ibge.censo_agro",
    version="1.0",
    effective_from="0.10.0",
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
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always a valid census year (>= 1995)",
        "Numeric values are always >= 0",
        "'fonte' is always 'ibge_censo_agro'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_CENSO_AGRO_LEGADO_V1 = Contract(
    name="ibge.censo_agro_legado",
    version="1.0",
    effective_from="0.12.0",
    primary_key=["ano", "tema", "categoria", "variavel", "localidade"],
    columns=[
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
    version="1.0",
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

IBGE_CENSO_AGRO_MUNICIPAL_V1 = Contract(
    name="ibge.censo_agro_municipal_1985",
    version="1.0",
    effective_from="0.12.0",
    primary_key=[],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1985,
            max_value=1985,
        ),
        Column(
            name="uf",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="uf_cod",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=11,
            max_value=53,
        ),
        Column(
            name="localidade",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="localidade_cod",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
        ),
        Column(
            name="nivel",
            type=ColumnType.STRING,
            nullable=False,
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
            name="confianca",
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
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always 1985",
        "Numeric values are always >= 0",
        "'fonte' is always 'ibge_censo_agro_municipal_1985'",
        "Dados extraídos via OCR de PDFs do IBGE — 22 UFs (excluídos MA/PI/CE/RN)",
        "'confianca' indica qualidade OCR: alta, media ou baixa",
        "'localidade_cod' pode ser None quando OCR não permite match exato",
        "PK vazio: OCR pode gerar labels homônimos (ex: 3 'Santo Antônio' em MG)",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

IBGE_SILVICULTURA_V1 = Contract(
    name="ibge.silvicultura",
    version="1.0",
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
    version="1.0",
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

register_contract("lspa", IBGE_LSPA_V1)
register_contract("silvicultura", IBGE_SILVICULTURA_V1)
register_contract("extrativismo_vegetal", IBGE_EXTRACAO_VEGETAL_V1)
register_contract("leite_industrial", IBGE_LEITE_TRIMESTRAL_V1)
register_contract("producao_anual", IBGE_PAM_V1)
register_contract("pecuaria_municipal", IBGE_PPM_V1)
register_contract("abate_trimestral", IBGE_ABATE_V1)
register_contract("censo_agropecuario", IBGE_CENSO_AGRO_V1)
register_contract("censo_agropecuario_legado", IBGE_CENSO_AGRO_LEGADO_V1)
register_contract("censo_agropecuario_historico", IBGE_CENSO_AGRO_HISTORICO_V1)
register_contract("censo_agropecuario_municipal_1985", IBGE_CENSO_AGRO_MUNICIPAL_V1)

__all__ = [
    "IBGE_ABATE_V1",
    "IBGE_CENSO_AGRO_HISTORICO_V1",
    "IBGE_CENSO_AGRO_LEGADO_V1",
    "IBGE_CENSO_AGRO_MUNICIPAL_V1",
    "IBGE_CENSO_AGRO_V1",
    "IBGE_EXTRACAO_VEGETAL_V1",
    "IBGE_LEITE_TRIMESTRAL_V1",
    "IBGE_LSPA_V1",
    "IBGE_PAM_V1",
    "IBGE_PPM_V1",
    "IBGE_SILVICULTURA_V1",
]
