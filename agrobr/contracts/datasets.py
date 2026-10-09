from __future__ import annotations

from dataclasses import replace

from agrobr.contracts import (
    BreakingChangePolicy,
    Column,
    ColumnType,
    Contract,
    _legacy,
    register_contract,
)
from agrobr.contracts.antt_pedagio import ANTT_PEDAGIO_FLUXO_V3
from agrobr.contracts.mapbiomas import MAPBIOMAS_COBERTURA_MUNICIPAL_V1
from agrobr.contracts.rnc import RNC_PROTEGIDAS_V1, RNC_REGISTRADAS_V1
from agrobr.contracts.zarc import ZONEAMENTO_AGRICOLA_V2

register_contract("rnc_registradas", RNC_REGISTRADAS_V1)
register_contract("rnc_protegidas", RNC_PROTEGIDAS_V1)

CREDITO_RURAL_V2 = Contract(
    name="bcb.credito_rural",
    version="2.0",
    effective_from="1.2.0",
    primary_key=["safra", "produto", "uf", "finalidade", "programa"],
    columns=[
        Column(
            name="safra",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Safra de julho a junho do mês de emissão, no formato AAAA/AA (2023/24) da camada; o SICOR não publica safra",
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
            nullable=False,
            stable=True,
        ),
        Column(
            name="programa",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="cd_programa",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="qtd_contratos",
            type=ColumnType.INTEGER,
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
        Column(
            name="area_financiada",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ha",
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
        "'safra' always matches pattern YYYY/YY",
        "'uf' is always a valid Brazilian state code when present",
        "Numeric values are always >= 0",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

_COMEX_COLUMNS = [
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
]

_VOLUME_TON = Column(
    name="volume_ton",
    type=ColumnType.FLOAT,
    nullable=True,
    unit="t",
    stable=False,
    min_value=0,
    description="kg_liquido / 1000",
)

_COMEX_GUARANTEES = [
    "Column names never change (additions only)",
    "'ano' is always >= 1997",
    "'mes' is between 1 and 12",
    "Numeric values are always >= 0",
]

EXPORTACAO_V1_1 = Contract(
    name="comexstat.exportacao",
    version="1.1",
    effective_from="2.0.0",
    primary_key=["ano", "mes", "produto", "uf"],
    columns=[*_COMEX_COLUMNS, _VOLUME_TON],
    guarantees=_COMEX_GUARANTEES,
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)


FERTILIZANTE_V2 = Contract(
    name="anda.fertilizante",
    version="2.0",
    columns=[
        Column(name="ano", type=ColumnType.INTEGER, min_value=2000),
        Column(name="mes", type=ColumnType.INTEGER, min_value=1, max_value=12),
        Column(name="uf", type=ColumnType.STRING, nullable=True),
        Column(name="produto_fertilizante", type=ColumnType.STRING),
        Column(name="volume_ton", type=ColumnType.FLOAT, nullable=True, unit="ton", min_value=0),
    ],
    primary_key=["ano", "mes", "uf", "produto_fertilizante"],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always >= 2000",
        "'mes' is between 1 and 12",
        "Numeric values are always >= 0",
        "'produto_fertilizante' is always 'total'",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
    effective_from="1.2.0",
)

FOCOS_QUEIMADAS_V1 = Contract(
    name="queimadas.focos",
    version="1.1",
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
        "'lat' is always between -35 and 6 (Brazil bounding box)",
        "'lon' is always between -74 and -30 (Brazil bounding box)",
        "'data' is always a valid date",
        "Numeric values are always >= 0 when present",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)


MAPBIOMAS_COBERTURA_V2 = Contract(
    name="mapbiomas.cobertura",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["bioma", "uf", "classe_id", "ano"],
    columns=[
        Column(
            name="bioma",
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
            name="classe_id",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
        ),
        Column(
            name="classe",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="nivel_0",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1985,
        ),
        Column(
            name="area_ha",
            type=ColumnType.FLOAT,
            nullable=False,
            unit="ha",
            stable=True,
            min_value=0,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'bioma' is always a valid Brazilian biome name",
        "'uf' is always a valid Brazilian state code (UF)",
        "'ano' is always between 1985 and current year",
        "'area_ha' is always >= 0",
        "'classe_id' maps to MapBiomas LULC legend codes",
        "'classe' is null only for a published class id outside the known legend, with a warning",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

MAPBIOMAS_TRANSICAO_V2 = Contract(
    name="mapbiomas.transicao",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["bioma", "uf", "classe_de_id", "classe_para_id", "periodo"],
    columns=[
        Column(
            name="bioma",
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
            name="classe_de_id",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
        ),
        Column(
            name="classe_de",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="classe_para_id",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
        ),
        Column(
            name="classe_para",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="periodo",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="area_ha",
            type=ColumnType.FLOAT,
            nullable=False,
            unit="ha",
            stable=True,
            min_value=0,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'bioma' is always a valid Brazilian biome name",
        "'uf' is always a valid Brazilian state code (UF)",
        "'periodo' always matches pattern YYYY-YYYY",
        "'area_ha' is always >= 0",
        "'classe_de_id' and 'classe_para_id' map to MapBiomas LULC legend codes",
        "'classe_de' and 'classe_para' are null only for a published class id outside the known legend, with a warning",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

CONAB_PROGRESSO_V1 = Contract(
    name="conab.progresso_safra",
    version="1.0",
    effective_from="0.10.0",
    primary_key=["cultura", "safra", "operacao", "estado", "semana_atual"],
    columns=[
        Column(
            name="cultura",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Cultura (ex: Soja, Milho 2a)",
        ),
        Column(
            name="safra",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Safra no formato YYYY/YY",
        ),
        Column(
            name="operacao",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Semeadura ou Colheita",
        ),
        Column(
            name="estado",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Codigo UF (ex: MT, GO, PR)",
        ),
        Column(
            name="semana_atual",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Data da semana no formato YYYY-MM-DD",
        ),
        Column(
            name="pct_ano_anterior",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="fracao",
            min_value=0.0,
            max_value=1.0,
        ),
        Column(
            name="pct_semana_anterior",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="fracao",
            min_value=0.0,
            max_value=1.0,
        ),
        Column(
            name="pct_semana_atual",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="fracao",
            min_value=0.0,
            max_value=1.0,
        ),
        Column(
            name="pct_media_5_anos",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="fracao",
            min_value=0.0,
            max_value=1.0,
        ),
    ],
    guarantees=[
        "PK unica por combinacao cultura + safra + operacao + estado + semana",
        "Valores percentuais entre 0.0 e 1.0 (fracao, nao %)",
        "Dados semanais publicados pela CONAB",
        "'estado' e codigo UF de 2 letras (ex: MT, GO, PR)",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

CONAB_PROGRESSO_V1_1 = Contract(
    name=CONAB_PROGRESSO_V1.name,
    version="1.1",
    effective_from="2.0.0",
    primary_key=list(CONAB_PROGRESSO_V1.primary_key),
    columns=[
        *CONAB_PROGRESSO_V1.columns,
        Column(
            name="revisado",
            type=ColumnType.BOOLEAN,
            nullable=True,
            stable=False,
            description="Marca de revisão * em algum percentual; nulo sem percentual numérico",
        ),
    ],
    guarantees=[
        *CONAB_PROGRESSO_V1.guarantees,
        "Percentuais com sufixo * preservam o valor e a indicação de revisão pela CONAB",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

CONAB_PROGRESSO_V2 = Contract(
    name=CONAB_PROGRESSO_V1.name,
    version="2.0",
    effective_from="2.0.0",
    primary_key=["cultura", "safra", "operacao", "uf", "semana_atual"],
    columns=[
        *(
            Column(
                name="uf",
                type=ColumnType.STRING,
                nullable=False,
                stable=True,
                description=(
                    "UF de 2 letras; MEDIA_ESTADOS para a média da CONAB dos estados monitorados; "
                    "BR só quando a planilha publica Brasil"
                ),
            )
            if column.name == "estado"
            else column
            for column in CONAB_PROGRESSO_V1_1.columns
        ),
        Column(
            name="n_estados",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Estados da média da CONAB, lido da nota publicada; nulo nas UFs",
        ),
        Column(
            name="cobertura_area_pct",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=False,
            unit="fracao",
            min_value=0.0,
            max_value=1.0,
            description=(
                "Fração da área cultivada coberta pelos estados monitorados, lida da nota "
                "publicada (0.98 = 98%); nulo nas UFs"
            ),
        ),
    ],
    guarantees=[
        "PK unica por combinacao cultura + safra + operacao + uf + semana",
        "Valores percentuais entre 0.0 e 1.0 (fracao, nao %)",
        "Dados semanais publicados pela CONAB",
        "'uf' é a UF de 2 letras; MEDIA_ESTADOS é a média da própria CONAB dos estados "
        "monitorados (n_estados, cobertura_area_pct), não o Brasil; BR só quando a CONAB publica "
        "Brasil",
        "n_estados e cobertura_area_pct saem da nota publicada, sem recálculo",
        "Percentuais com sufixo * preservam o valor e a indicação de revisão pela CONAB",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

AJUSTE_DIARIO_V1 = Contract(
    name="b3.ajuste_diario",
    version="1.0",
    effective_from="0.10.0",
    primary_key=["data", "ticker", "vencimento_codigo"],
    columns=[
        Column(name="data", type=ColumnType.DATE, nullable=False, stable=True),
        Column(name="ticker", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="descricao", type=ColumnType.STRING, nullable=True, stable=True),
        Column(
            name="vencimento_codigo",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="vencimento_mes",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1,
            max_value=12,
        ),
        Column(
            name="vencimento_ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=2000,
        ),
        Column(
            name="ajuste_anterior",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=0,
        ),
        Column(
            name="ajuste_atual",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=0,
        ),
        Column(name="variacao", type=ColumnType.FLOAT, nullable=True, stable=True),
        Column(
            name="ajuste_por_contrato",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
        ),
        Column(name="unidade", type=ColumnType.STRING, nullable=True, stable=True),
    ],
    guarantees=[
        "PK unica por combinacao data + ticker + vencimento_codigo",
        "'ticker' sempre em {BGI, CCM, ICF, CNL, ETH, SJC, SOY}",
        "'vencimento_mes' entre 1 e 12",
        "'ajuste_atual' e 'ajuste_anterior' sempre >= 0 quando presentes",
        "Dados apenas para dias uteis (sem pregao em weekends/feriados)",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

PRECO_ATACADO_V2 = Contract(
    name="conab.preco_atacado",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["data", "produto", "ceasa"],
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
            name="categoria",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="unidade",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="ceasa",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="ceasa_uf",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="preco",
            type=ColumnType.FLOAT,
            nullable=False,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
    ],
    guarantees=[
        "PK unica por combinacao data + produto + ceasa",
        "'produto' sempre em maiusculas (ex: TOMATE, ABACAXI)",
        "'categoria' FRUTAS, HORTALICAS ou OVOS; nula só para produto fora da tabela do agrobr, com aviso",
        "'unidade' sempre KG, UN ou DZ",
        "'ceasa_uf' sempre codigo UF de 2 letras",
        "'preco' sempre > 0 (nulls filtrados)",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

POSICOES_ABERTAS_V1 = Contract(
    name="b3.posicoes_abertas",
    version="1.1",
    effective_from="2.0.0",
    primary_key=["data", "ticker_completo"],
    columns=[
        Column(name="data", type=ColumnType.DATE, nullable=False, stable=True),
        Column(name="ticker", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="descricao", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="ticker_completo", type=ColumnType.STRING, nullable=False, stable=True),
        Column(
            name="vencimento_codigo",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="vencimento_mes",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1,
            max_value=12,
        ),
        Column(
            name="vencimento_ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=2000,
        ),
        Column(name="tipo", type=ColumnType.STRING, nullable=False, stable=True),
        Column(
            name="posicoes_abertas",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=0,
        ),
        Column(name="variacao_posicoes", type=ColumnType.INTEGER, nullable=True, stable=True),
        Column(name="unidade", type=ColumnType.STRING, nullable=True, stable=True),
    ],
    guarantees=[
        "PK unica por combinacao data + ticker_completo",
        "'ticker' sempre em {BGI, CCM, ETH, ICF, SJC, CNL}",
        "'tipo' sempre 'futuro' ou 'opcao'",
        "'vencimento_mes'/'vencimento_ano' do contrato em futuros e opcoes (a expiracao pode cair no mes anterior)",
        "'posicoes_abertas' sempre >= 0",
        "Dados apenas para dias uteis (sem pregao em weekends/feriados)",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)


MOVIMENTACAO_PORTUARIA_V1 = Contract(
    name="antaq.movimentacao",
    version="1.0",
    effective_from="0.11.0",
    primary_key=["ano", "mes", "porto", "cd_mercadoria", "sentido", "tipo_navegacao"],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=2010,
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
            name="data_atracacao",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="tipo_navegacao",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="tipo_operacao",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="natureza_carga",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="sentido",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="porto",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="complexo_portuario",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="terminal",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="municipio",
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
            name="regiao",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="cd_mercadoria",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="mercadoria",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="grupo_mercadoria",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="origem",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="destino",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="peso_bruto_ton",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ton",
            stable=True,
            min_value=0,
        ),
        Column(
            name="qt_carga",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=0,
        ),
        Column(
            name="teu",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
            min_value=0,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always >= 2010",
        "'mes' is between 1 and 12",
        "'uf' is always a valid Brazilian state code when present",
        "'sentido' is always 'Embarcados' or 'Desembarcados' when present",
        "'peso_bruto_ton' is always >= 0 when present",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

MOVIMENTACAO_PORTUARIA_V2 = Contract(
    name="antaq.movimentacao",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["ano", "mes", "porto", "cd_mercadoria", "sentido", "tipo_navegacao"],
    columns=[
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=2010,
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
            name="data_atracacao",
            type=ColumnType.DATETIME,
            nullable=True,
            stable=True,
        ),
        Column(
            name="tipo_navegacao",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="tipo_operacao",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="natureza_carga",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="sentido",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="porto",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="complexo_portuario",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="terminal",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="municipio",
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
            name="regiao",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="cd_mercadoria",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="mercadoria",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="grupo_mercadoria",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="origem",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="destino",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="peso_bruto_ton",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ton",
            stable=True,
            min_value=0,
        ),
        Column(
            name="qt_carga",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=0,
        ),
        Column(
            name="teu",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
            min_value=0,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano' is always >= 2010",
        "'mes' is between 1 and 12",
        "'uf' is always a valid Brazilian state code when present",
        "'sentido' is always 'Embarcados' or 'Desembarcados' when present",
        "'peso_bruto_ton' is always >= 0 when present",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("ajuste_diario", AJUSTE_DIARIO_V1)
register_contract("conab_progresso", CONAB_PROGRESSO_V2)
register_contract("preco_atacado", PRECO_ATACADO_V2)
register_contract("credito_rural", CREDITO_RURAL_V2)
register_contract("exportacao", EXPORTACAO_V1_1)
register_contract("fertilizante", FERTILIZANTE_V2)
register_contract("focos_queimadas", FOCOS_QUEIMADAS_V1)
register_contract("queimadas", FOCOS_QUEIMADAS_V1)
register_contract("mapbiomas_cobertura", MAPBIOMAS_COBERTURA_V2)
register_contract("mapbiomas_cobertura_municipal", MAPBIOMAS_COBERTURA_MUNICIPAL_V1)
register_contract("mapbiomas_transicao", MAPBIOMAS_TRANSICAO_V2)
register_contract("movimentacao_portuaria", MOVIMENTACAO_PORTUARIA_V2)
register_contract("posicoes_abertas", POSICOES_ABERTAS_V1)


ANP_DIESEL_VENDAS_V1 = Contract(
    name="anp_diesel.vendas",
    version="1.0",
    effective_from="0.11.0",
    primary_key=["data", "uf", "produto"],
    columns=[
        Column(
            name="data",
            type=ColumnType.DATE,
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
            name="regiao",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="produto",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="volume_m3",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="m3",
            stable=True,
            min_value=0,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'uf' is a valid Brazilian state code when present",
        "'volume_m3' is in cubic meters when present",
        "Data is monthly (first day of month)",
        "'produto' contains DIESEL variants only",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("anp_diesel_vendas", ANP_DIESEL_VENDAS_V1)

MAPA_PSR_SINISTROS_V1 = Contract(
    name="mapa_psr.sinistros",
    version="1.2",
    effective_from="0.12.0",
    primary_key=["nr_apolice", "ano_apolice", "uf", "cultura", "cd_ibge", "evento"],
    columns=[
        Column(name="nr_apolice", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="ano_apolice", type=ColumnType.INTEGER, nullable=False, stable=True),
        Column(name="uf", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="municipio", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="cd_ibge", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="cultura", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="classificacao", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="evento", type=ColumnType.STRING, nullable=False, stable=True),
        Column(
            name="area_total",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ha",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_indenizacao",
            type=ColumnType.FLOAT,
            nullable=False,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_premio",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_subvencao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_limite_garantia",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
        Column(name="produtividade_estimada", type=ColumnType.FLOAT, nullable=True, stable=True),
        Column(name="produtividade_segurada", type=ColumnType.FLOAT, nullable=True, stable=True),
        Column(name="nivel_cobertura", type=ColumnType.FLOAT, nullable=True, stable=True),
        Column(name="seguradora", type=ColumnType.STRING, nullable=True, stable=True),
        Column(
            name="cod_municipio",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
        Column(
            name="inicio_vigencia",
            type=ColumnType.DATE,
            nullable=True,
            stable=False,
            description="Início da vigência da apólice (DT_INICIO_VIGENCIA); nulo quando o MAPA publica início igual ao fim (vigência não publicada; em todas as apólices de 2006 a 2015).",
        ),
        Column(
            name="fim_vigencia",
            type=ColumnType.DATE,
            nullable=True,
            stable=False,
            description="Fim da vigência da apólice (DT_FIM_VIGENCIA); nulo junto com inicio_vigencia quando início e fim publicados são iguais; data ilegível ou com ano fora de 1900–2099 anula só esta coluna.",
        ),
        Column(
            name="data_apolice",
            type=ColumnType.DATE,
            nullable=True,
            stable=False,
            description="Data de emissão da apólice (DT_APOLICE); o ano é o de ano_apolice.",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'uf' is a valid Brazilian state code",
        "'valor_indenizacao' is always > 0 (sinistros only)",
        "'evento' is always non-empty (sinistros only)",
        "Monetary values are in BRL",
        "Data since 2006 (PSR inception)",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

MAPA_PSR_APOLICES_V2 = Contract(
    name="mapa_psr.apolices",
    version="2.1",
    effective_from="2.0.0",
    primary_key=["nr_apolice", "ano_apolice", "uf", "cultura", "cd_ibge", "seguradora"],
    columns=[
        Column(name="nr_apolice", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="ano_apolice", type=ColumnType.INTEGER, nullable=False, stable=True),
        Column(name="uf", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="municipio", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="cd_ibge", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="cultura", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="classificacao", type=ColumnType.STRING, nullable=True, stable=True),
        Column(
            name="area_total",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="ha",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_premio",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_subvencao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_limite_garantia",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
        Column(
            name="valor_indenizacao",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="BRL",
            stable=True,
            min_value=0,
        ),
        Column(name="evento", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="produtividade_estimada", type=ColumnType.FLOAT, nullable=True, stable=True),
        Column(name="produtividade_segurada", type=ColumnType.FLOAT, nullable=True, stable=True),
        Column(name="nivel_cobertura", type=ColumnType.FLOAT, nullable=True, stable=True),
        Column(name="taxa", type=ColumnType.FLOAT, nullable=True, stable=True),
        Column(name="seguradora", type=ColumnType.STRING, nullable=False, stable=True),
        Column(
            name="cod_municipio",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
        Column(
            name="inicio_vigencia",
            type=ColumnType.DATE,
            nullable=True,
            stable=False,
            description="Início da vigência da apólice (DT_INICIO_VIGENCIA); nulo quando o MAPA publica início igual ao fim (vigência não publicada; em todas as apólices de 2006 a 2015).",
        ),
        Column(
            name="fim_vigencia",
            type=ColumnType.DATE,
            nullable=True,
            stable=False,
            description="Fim da vigência da apólice (DT_FIM_VIGENCIA); nulo junto com inicio_vigencia quando início e fim publicados são iguais; data ilegível ou com ano fora de 1900–2099 anula só esta coluna.",
        ),
        Column(
            name="data_apolice",
            type=ColumnType.DATE,
            nullable=True,
            stable=False,
            description="Data de emissão da apólice (DT_APOLICE); o ano é o de ano_apolice.",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'uf' is a valid Brazilian state code",
        "All policies with federal subsidy since 2006",
        "'valor_indenizacao' is nullable (0 or null if no claim)",
        "Monetary values are in BRL",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("mapa_psr_sinistros", MAPA_PSR_SINISTROS_V1)
register_contract("mapa_psr_apolices", MAPA_PSR_APOLICES_V2)


ANTT_PEDAGIO_PRACAS_V1 = Contract(
    name="antt_pedagio.pracas",
    version="1.0",
    effective_from="0.12.0",
    primary_key=["concessionaria", "praca_de_pedagio"],
    columns=[
        Column(
            name="concessionaria",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="praca_de_pedagio",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="rodovia",
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
            name="km_m",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="municipio",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="lat",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=-35.0,
            max_value=6.0,
        ),
        Column(
            name="lon",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=-74.0,
            max_value=-30.0,
        ),
        Column(
            name="situacao",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'uf' is a valid Brazilian state code when present",
        "'lat' is within Brazil bounding box when present",
        "'lon' is within Brazil bounding box when present",
        "200+ toll plazas registered",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

ANTT_PEDAGIO_PRACAS_V2 = Contract(
    name="antt_pedagio.pracas",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["concessionaria", "praca_de_pedagio"],
    columns=[
        Column(
            name="concessionaria",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="praca_de_pedagio",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="rodovia",
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
            name="km_m",
            type=ColumnType.FLOAT,
            description="Quilômetro da praça na rodovia, em km",
            nullable=True,
            stable=True,
        ),
        Column(
            name="municipio",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="lat",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=-35.0,
            max_value=6.0,
        ),
        Column(
            name="lon",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=-74.0,
            max_value=-30.0,
        ),
        Column(
            name="situacao",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="ano_do_pnv_snv",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Ano da edição do PNV/SNV da localização",
        ),
        Column(
            name="data_da_inativacao",
            type=ColumnType.DATE,
            nullable=True,
            stable=False,
            description="Data da inativação da praça; vazio vira nulo",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'uf' is a valid Brazilian state code when present",
        "'lat' is within Brazil bounding box when present",
        "'lon' is within Brazil bounding box when present",
        "200+ toll plazas registered",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("antt_pedagio_fluxo", ANTT_PEDAGIO_FLUXO_V3)
register_contract("antt_pedagio_pracas", ANTT_PEDAGIO_PRACAS_V2)


IMPORTACAO_V1_2 = Contract(
    name="comexstat.importacao",
    version="1.2",
    effective_from="2.0.0",
    primary_key=["ano", "mes", "produto", "uf"],
    columns=[
        *_COMEX_COLUMNS,
        Column(
            name="valor_frete_usd",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=False,
            min_value=0,
            unit="USD",
        ),
        Column(
            name="valor_seguro_usd",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=False,
            min_value=0,
            unit="USD",
        ),
        _VOLUME_TON,
    ],
    guarantees=[
        *_COMEX_GUARANTEES,
        "Freight and insurance are retained when published as separate USD measures",
        "NCM prefixes are consolidated at the product grain without summing statistical units",
    ],
)

PIB_AGRO_V1 = Contract(
    name="ibge.pib_agro",
    version="1.0",
    effective_from="0.13.0",
    primary_key=["trimestre", "setor", "precos"],
    columns=[
        Column(
            name="trimestre",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Trimestre no formato YYYYQQ",
        ),
        Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=True,
            unit="R$ (milhões)",
            stable=True,
        ),
        Column(
            name="unidade",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="setor",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Setor: agropecuaria, industria, servicos, pib_total",
        ),
        Column(
            name="precos",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="Tipo de precos: corrente, real_1995 (injetado pelo dataset)",
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
        "'trimestre' always matches pattern YYYYQQ",
        "'setor' is one of: agropecuaria, industria, servicos, pib_total",
        "'precos' is one of: corrente, real_1995",
        "'precos' in PK prevents collision between different deflator calls",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

SERIE_HISTORICA_SAFRA_V1 = Contract(
    name="conab.serie_historica_safra",
    version="1.1",
    effective_from="2.0.0",
    primary_key=["produto", "safra", "regiao", "uf"],
    columns=[
        Column(name="produto", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="safra", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="regiao", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="uf", type=ColumnType.STRING, nullable=True, stable=True),
        Column(
            name="area_plantada_mil_ha",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=0,
        ),
        Column(
            name="producao_mil_ton",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=0,
        ),
        Column(
            name="produtividade_kg_ha",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            min_value=0,
        ),
        Column(
            name="area_em_producao_mil_ha",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=False,
            min_value=0,
            description="Café: área em produção, em mil hectares.",
        ),
        Column(
            name="area_formacao_mil_ha",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=False,
            min_value=0,
            description="Café: área em formação, em mil hectares.",
        ),
        Column(
            name="area_colhida_mil_ha",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=False,
            min_value=0,
            description="Cana: área colhida, em mil hectares.",
        ),
    ],
    guarantees=[
        "PK unica por combinacao produto + safra + regiao + uf",
        "'produto' lowercase (ex: soja, milho_2)",
        "'safra' e o periodo publicado pela CONAB: YYYY/YY (ex: 2023/24) ou YYYY (ex: 2025; cafe e cereais de inverno)",
        "'regiao' quando presente: NORTE, NORDESTE, CENTRO-OESTE, SUDESTE, SUL",
        "'uf' quando presente: codigo UF de 2 letras uppercase",
        "Metricas (area, producao, produtividade) >= 0 quando presentes",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)


CLIMA_ESTACAO_V1 = Contract(
    name="datasets.clima_estacao",
    version="1.0",
    effective_from="0.13.0",
    primary_key=["data", "estacao"],
    columns=[
        Column(name="data", type=ColumnType.DATE, nullable=False, stable=True),
        Column(name="estacao", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="uf", type=ColumnType.STRING, nullable=True, stable=True),
        Column(name="temp_media", type=ColumnType.FLOAT, nullable=True, stable=True, unit="°C"),
        Column(name="temp_max", type=ColumnType.FLOAT, nullable=True, stable=True, unit="°C"),
        Column(name="temp_min", type=ColumnType.FLOAT, nullable=True, stable=True, unit="°C"),
        Column(
            name="precipitacao_mm",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="mm",
            min_value=0,
        ),
        Column(
            name="umidade_media",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="%",
            min_value=0,
            max_value=100,
        ),
        Column(
            name="radiacao_total_kj_m2",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="kJ/m²",
            min_value=0,
        ),
    ],
    guarantees=[
        "PK unica por combinacao data + estacao",
        "'estacao' codigo INMET (ex: A301)",
        "Apenas agregacao='diario' validada contra este contrato",
        "Todas as colunas de medicao sao nullable (estacoes podem falhar)",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

OFERTA_DEMANDA_GLOBAL_V1 = Contract(
    name="usda.psd",
    version="1.0",
    effective_from="0.13.0",
    primary_key=["commodity_code", "country_code", "market_year", "attribute"],
    columns=[
        Column(
            name="commodity_code",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="commodity",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="country_code",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="country",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="market_year",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1960,
        ),
        Column(
            name="attribute",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="attribute_br",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="value",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
        ),
        Column(
            name="unit",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'market_year' is always >= 1960",
        "Long format: one row per commodity/country/year/attribute",
        "Contract validates long format only; pivot=True skips validation",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

OFERTA_DEMANDA_GLOBAL_V2 = Contract(
    name="usda.psd",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["codigo_produto", "codigo_pais", "ano_comercial", "atributo"],
    columns=[
        Column(
            name="codigo_produto",
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
            name="codigo_pais",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="pais",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="ano_comercial",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1960,
        ),
        Column(
            name="atributo",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="atributo_br",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="valor",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
        ),
        Column(
            name="unidade",
            type=ColumnType.STRING,
            nullable=True,
            stable=True,
        ),
        Column(
            name="codigo_atributo",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
        ),
        Column(
            name="codigo_unidade",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
        ),
        Column(
            name="ano_atualizacao",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            min_value=1960,
        ),
        Column(
            name="mes_atualizacao",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
            min_value=1,
            max_value=12,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'ano_comercial' is always >= 1960",
        "Long format: one row per produto/pais/year/atributo",
        "Contract validates long format only; pivotar=True skips validation",
        "'atributo', 'unidade' and 'pais' are the official names of the PSD catalogs "
        "(countryCode '00' is the world aggregate, 'World')",
        "'ano_atualizacao'/'mes_atualizacao' are the USDA's last update of the series "
        "(pais x market year), not the queried edition; month '00' of old series is null",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)


register_contract("importacao", IMPORTACAO_V1_2)
register_contract("pib_agro", PIB_AGRO_V1)
register_contract("progresso_safra", CONAB_PROGRESSO_V2)
register_contract("serie_historica_safra", SERIE_HISTORICA_SAFRA_V1)
register_contract("clima_estacao", CLIMA_ESTACAO_V1)
register_contract("oferta_demanda_global", OFERTA_DEMANDA_GLOBAL_V2)
register_contract("zoneamento_agricola", ZONEAMENTO_AGRICOLA_V2)

CONDICAO_LAVOURAS_V1 = Contract(
    name="deral.condicao_lavouras",
    version="1.0",
    effective_from="0.13.0",
    primary_key=["produto", "data", "condicao"],
    columns=[
        Column(
            name="produto",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="data",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="dd/mm/yyyy",
        ),
        Column(
            name="condicao",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="boa|media|ruim|plantio|colheita",
        ),
        Column(
            name="pct",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="%",
            min_value=0,
            max_value=100,
        ),
        Column(
            name="plantio_pct",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="%",
            min_value=0,
            max_value=100,
        ),
        Column(
            name="colheita_pct",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="%",
            min_value=0,
            max_value=100,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'produto' is always a normalized DERAL key (lowercase)",
        "'condicao' is always one of: boa, media, ruim, plantio, colheita",
        "'pct', 'plantio_pct', 'colheita_pct' are percentages 0-100 when present",
        "Data covers only Paraná (PR)",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

CONDICAO_LAVOURAS_V2 = Contract(
    name="deral.condicao_lavouras",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["produto", "data", "condicao"],
    columns=[
        Column(
            name="produto",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
        ),
        Column(
            name="data",
            type=ColumnType.DATE,
            nullable=False,
            stable=True,
            description="Data de referência publicada na planilha",
        ),
        Column(
            name="condicao",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="boa|media|ruim|plantio|colheita",
        ),
        Column(
            name="pct",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="%",
            min_value=0,
            max_value=100,
        ),
        Column(
            name="plantio_pct",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="%",
            min_value=0,
            max_value=100,
        ),
        Column(
            name="colheita_pct",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="%",
            min_value=0,
            max_value=100,
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'produto' is always a normalized DERAL key (lowercase)",
        "'condicao' is always one of: boa, media, ruim, plantio, colheita",
        "'pct', 'plantio_pct', 'colheita_pct' are percentages 0-100 when present",
        "Data covers only Paraná (PR)",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("condicao_lavouras", CONDICAO_LAVOURAS_V2)

EMBARQUES_ANEC_V1 = Contract(
    name="anec.embarques",
    version="1.1",
    effective_from="1.0.6",
    primary_key=["porto", "produto", "periodo"],
    columns=[
        Column(
            name="porto",
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
            name="periodo",
            type=ColumnType.STRING,
            nullable=False,
            stable=True,
            description="last_week|current_week",
        ),
        Column(
            name="valor_ton",
            type=ColumnType.FLOAT,
            nullable=True,
            stable=True,
            unit="ton",
            min_value=0,
        ),
        Column(
            name="ano",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=False,
            description="Ano da edição impresso no boletim (Week NN/AAAA)",
            min_value=2026,
        ),
        Column(
            name="semana",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=False,
            description="Semana da edição impressa no boletim (Week NN/AAAA)",
            min_value=1,
            max_value=53,
        ),
        Column(
            name="data_inicio",
            type=ColumnType.DATE,
            nullable=True,
            stable=False,
            description="Primeiro dia do período, lido do rótulo do boletim",
        ),
        Column(
            name="data_fim",
            type=ColumnType.DATE,
            nullable=True,
            stable=False,
            description="Último dia do período, lido do rótulo do boletim",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'valor_ton' is always >= 0 when present",
        "One row per porto x produto x periodo",
        "Datas lidas dos rótulos do boletim, nunca da semana ISO; nulas quando os dois rótulos "
        "não formam semanas consecutivas de 7 dias",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("embarques_anec", EMBARQUES_ANEC_V1)

POSICIONAMENTO_FUNDOS_V1 = Contract(
    name="cftc.cot",
    version="1.1",
    effective_from="1.1.0",
    primary_key=["data", "codigo_cftc"],
    columns=[
        Column(name="data", type=ColumnType.DATE, nullable=False, stable=True),
        Column(name="commodity", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="contrato", type=ColumnType.STRING, nullable=False, stable=True),
        Column(name="codigo_cftc", type=ColumnType.STRING, nullable=False, stable=True),
        Column(
            name="open_interest",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="managed_money_long",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="managed_money_short",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="managed_money_spread",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="managed_money_net",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
        ),
        Column(
            name="producer_long",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="producer_short",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="swap_long",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="swap_short",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="swap_spread",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=False,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="other_long",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="other_short",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="other_spread",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=False,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="nonreportable_long",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="nonreportable_short",
            type=ColumnType.INTEGER,
            nullable=False,
            stable=True,
            unit="contratos",
            min_value=0,
        ),
        Column(
            name="change_managed_money_long",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
            unit="contratos",
        ),
        Column(
            name="change_managed_money_short",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
            unit="contratos",
        ),
        Column(
            name="change_open_interest",
            type=ColumnType.INTEGER,
            nullable=True,
            stable=True,
            unit="contratos",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'data' is the weekly report date (Tuesday)",
        "'commodity' is the canonical agrobr crop name",
        "'codigo_cftc' is the stable CFTC contract market code",
        "Position columns are always >= 0; managed_money_net = long - short",
        "open_interest = producer + swap + managed_money + other + nonreportable longs, plus the swap, managed_money and other spreads (exact in futures; the combined report can leave 1 contract)",
        "'change_*' columns are null only on the first observation of a contract",
        "One row per data x codigo_cftc",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

POSICIONAMENTO_FUNDOS_COLUNAS_V2: dict[str, str] = {
    "commodity": "produto",
    "open_interest": "posicoes_abertas",
    "managed_money_long": "fundos_compra",
    "managed_money_short": "fundos_venda",
    "managed_money_spread": "fundos_spread",
    "managed_money_net": "fundos_saldo",
    "producer_long": "produtores_compra",
    "producer_short": "produtores_venda",
    "swap_long": "swap_compra",
    "swap_short": "swap_venda",
    "swap_spread": "swap_spread",
    "other_long": "outros_compra",
    "other_short": "outros_venda",
    "other_spread": "outros_spread",
    "nonreportable_long": "nao_reportaveis_compra",
    "nonreportable_short": "nao_reportaveis_venda",
    "change_managed_money_long": "variacao_fundos_compra",
    "change_managed_money_short": "variacao_fundos_venda",
    "change_open_interest": "variacao_posicoes",
}

POSICIONAMENTO_FUNDOS_V2 = Contract(
    name=POSICIONAMENTO_FUNDOS_V1.name,
    version="2.0",
    effective_from="2.0.0",
    primary_key=list(POSICIONAMENTO_FUNDOS_V1.primary_key),
    columns=[
        replace(column, name=POSICIONAMENTO_FUNDOS_COLUNAS_V2.get(column.name, column.name))
        for column in POSICIONAMENTO_FUNDOS_V1.columns
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'data' is the weekly report date (Tuesday)",
        "'produto' is the canonical agrobr crop name",
        "'codigo_cftc' is the stable CFTC contract market code",
        "Position columns are always >= 0; fundos_saldo = fundos_compra - fundos_venda",
        "posicoes_abertas = produtores + swap + fundos + outros + nao_reportaveis compra, plus the swap, fundos and "
        "outros spreads (exact in futures; the combined report can leave 1 contract)",
        "'variacao_*' columns are null only on the first observation of a contract",
        "One row per data x codigo_cftc",
    ],
    breaking_policy=BreakingChangePolicy.MAJOR_VERSION,
)

register_contract("posicionamento_fundos", POSICIONAMENTO_FUNDOS_V2)

__all__ = [
    "AJUSTE_DIARIO_V1",
    "ANP_DIESEL_VENDAS_V1",
    "ANTT_PEDAGIO_FLUXO_V3",
    "ANTT_PEDAGIO_PRACAS_V1",
    "ANTT_PEDAGIO_PRACAS_V2",
    "CLIMA_ESTACAO_V1",
    "CONAB_PROGRESSO_V1",
    "CONAB_PROGRESSO_V1_1",
    "CONAB_PROGRESSO_V2",
    "CONDICAO_LAVOURAS_V1",
    "CONDICAO_LAVOURAS_V2",
    "CREDITO_RURAL_V2",
    "EXPORTACAO_V1_1",
    "FERTILIZANTE_V2",
    "FOCOS_QUEIMADAS_V1",
    "IMPORTACAO_V1_2",
    "MAPA_PSR_APOLICES_V2",
    "MAPA_PSR_SINISTROS_V1",
    "MAPBIOMAS_COBERTURA_V2",
    "MAPBIOMAS_COBERTURA_MUNICIPAL_V1",
    "MAPBIOMAS_TRANSICAO_V2",
    "MOVIMENTACAO_PORTUARIA_V1",
    "MOVIMENTACAO_PORTUARIA_V2",
    "OFERTA_DEMANDA_GLOBAL_V1",
    "OFERTA_DEMANDA_GLOBAL_V2",
    "PIB_AGRO_V1",
    "POSICIONAMENTO_FUNDOS_V1",
    "POSICIONAMENTO_FUNDOS_V2",
    "POSICOES_ABERTAS_V1",
    "PRECO_ATACADO_V2",
    "RNC_PROTEGIDAS_V1",
    "RNC_REGISTRADAS_V1",
    "SERIE_HISTORICA_SAFRA_V1",
]


def __getattr__(name: str) -> Contract:
    return _legacy.resolve(
        name,
        module=__name__,
        names=frozenset(
            [
                "ANP_DIESEL_PRECOS_V1",
                "PRECO_ATACADO_V1",
                "MAPA_PSR_APOLICES_V1",
                "ANTT_PEDAGIO_FLUXO_V1",
                "ANTT_PEDAGIO_FLUXO_V2",
                "COMERCIO_BILATERAL_V1",
                "TRADE_MIRROR_V1",
                "DESMATAMENTO_PRODES_V1",
                "DESMATAMENTO_DETER_V1",
                "CLIMA_V1",
                "FERTILIZANTE_V1",
                "IMPORTACAO_V1",
                "EXPORTACAO_V1",
                "CLIMA_V2",
                "ZONEAMENTO_AGRICOLA_V1",
                "SICAR_IMOVEIS_V1",
                "MAPBIOMAS_COBERTURA_V1",
                "MAPBIOMAS_TRANSICAO_V1",
                "CREDITO_RURAL_V1_1",
            ]
        ),
    )
