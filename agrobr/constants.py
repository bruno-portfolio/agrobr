"""Constantes e configurações do agrobr."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Fonte(StrEnum):
    ABIOVE = "abiove"
    ANDA = "anda"
    ANP_DIESEL = "anp_diesel"
    ANTAQ = "antaq"
    ANTT_PEDAGIO = "antt_pedagio"
    B3 = "b3"
    BCB = "bcb"
    CEPEA = "cepea"
    COMEXSTAT = "comexstat"
    CONAB = "conab"
    DERAL = "deral"
    IBGE = "ibge"
    IMEA = "imea"
    INMET = "inmet"
    NASA_POWER = "nasa_power"
    NOTICIAS_AGRICOLAS = "noticias_agricolas"
    DESMATAMENTO = "desmatamento"
    MAPBIOMAS = "mapbiomas"
    QUEIMADAS = "queimadas"
    USDA = "usda"


URLS = {
    Fonte.ABIOVE: {
        "base": "https://abiove.org.br",
        "estatisticas": "https://abiove.org.br/estatisticas",
        "exportacao": "https://abiove.org.br/abiove_content/Abiove",
    },
    Fonte.ANDA: {
        "base": "https://anda.org.br",
        "estatisticas": "https://anda.org.br/recursos/",
    },
    Fonte.ANP_DIESEL: {
        "base": "https://www.gov.br/anp/pt-br",
        "shlp": "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp",
        "vendas_m3": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos/de/vdpb/vendas-combustiveis-m3.xls",
    },
    Fonte.ANTAQ: {
        "base": "https://web3.antaq.gov.br/ea/sense/download.html",
        "bulk_txt": "https://web3.antaq.gov.br/ea/txt",
    },
    Fonte.ANTT_PEDAGIO: {
        "base": "https://dados.antt.gov.br",
        "trafego": "https://dados.antt.gov.br/dataset/fluxo-de-veiculos-nas-pracas-de-pedagio",
        "pracas": "https://dados.antt.gov.br/dataset/cadastro-de-pracas-de-pedagio",
    },
    Fonte.BCB: {
        "base": "https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata",
        "dados_abertos": "https://dadosabertos.bcb.gov.br/dataset/sicor",
    },
    Fonte.CEPEA: {
        "base": "https://www.cepea.org.br",
        "indicadores": "https://www.cepea.org.br/br/indicador",
    },
    Fonte.COMEXSTAT: {
        "base": "https://comexstat.mdic.gov.br",
        "bulk_csv": "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm",
    },
    Fonte.CONAB: {
        "base": "https://www.gov.br/conab",
        "safras": "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras",
        "boletim_graos": "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/safra-de-graos/boletim-da-safra-de-graos",
        "ceasa_prohort": "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/plugin/cda/api/doQuery",
    },
    Fonte.DERAL: {
        "base": "https://www.agricultura.pr.gov.br/deral",
        "safras": "https://www.agricultura.pr.gov.br/deral/safras",
        "pc_xls": "https://www.agricultura.pr.gov.br/system/files/publico/Safras/PC.xls",
    },
    Fonte.IBGE: {
        "base": "https://sidra.ibge.gov.br",
        "api": "https://apisidra.ibge.gov.br",
    },
    Fonte.IMEA: {
        "base": "https://api1.imea.com.br/api",
        "cotacoes": "https://api1.imea.com.br/api/v2/mobile/cadeias",
    },
    Fonte.INMET: {
        "base": "https://apitempo.inmet.gov.br",
        "estacoes": "https://apitempo.inmet.gov.br/estacoes",
        "dados": "https://apitempo.inmet.gov.br/estacao",
    },
    Fonte.NASA_POWER: {
        "base": "https://power.larc.nasa.gov",
        "daily": "https://power.larc.nasa.gov/api/temporal/daily/point",
    },
    Fonte.USDA: {
        "base": "https://apps.fas.usda.gov/OpenData/api",
        "psd": "https://apps.fas.usda.gov/OpenData/api/psd",
    },
    Fonte.NOTICIAS_AGRICOLAS: {
        "base": "https://www.noticiasagricolas.com.br",
        "cotacoes": "https://www.noticiasagricolas.com.br/cotacoes",
    },
    Fonte.DESMATAMENTO: {
        "base": "https://terrabrasilis.dpi.inpe.br",
        "geoserver": "https://terrabrasilis.dpi.inpe.br/geoserver",
    },
    Fonte.MAPBIOMAS: {
        "base": "https://brasil.mapbiomas.org",
        "estatisticas": "https://brasil.mapbiomas.org/estatisticas/",
        "gcs": "https://storage.googleapis.com/mapbiomas-public/initiatives/brasil",
    },
    Fonte.QUEIMADAS: {
        "base": "https://terrabrasilis.dpi.inpe.br/queimadas/portal/",
        "dados_abertos": "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv",
    },
    Fonte.B3: {
        "base": "https://www.b3.com.br",
        "ajustes": "https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/Ajustes1.asp",
    },
}

NOTICIAS_AGRICOLAS_PRODUTOS = {
    "soja": "soja/soja-indicador-cepea-esalq-porto-paranagua",
    "soja_parana": "soja/indicador-cepea-esalq-soja-parana",
    "milho": "milho/indicador-cepea-esalq-milho",
    "boi": "boi-gordo/boi-gordo-indicador-esalq-bmf",
    "boi_gordo": "boi-gordo/boi-gordo-indicador-esalq-bmf",
    "cafe": "cafe/indicador-cepea-esalq-cafe-arabica",
    "cafe_arabica": "cafe/indicador-cepea-esalq-cafe-arabica",
    "algodao": "algodao/algodao-indicador-cepea-esalq-a-prazo",
    "trigo": "trigo/preco-medio-do-trigo-cepea-esalq",
    "arroz": "arroz/arroz-em-casca-esalq-bbm",
    "acucar": "sucroenergetico/acucar-cristal-cepea",
    "acucar_refinado": "sucroenergetico/acucar-refinado-amorfo",
    "etanol_hidratado": "sucroenergetico/indicador-semanal-etanol-hidratado-cepea-esalq",
    "etanol_anidro": "sucroenergetico/indicador-semanal-etanol-anidro-cepea-esalq",
    "frango_congelado": "frango/precos-do-frango-congelado-cepea-esalq",
    "frango_resfriado": "frango/precos-do-frango-resfriado-cepea-esalq",
    "suino": "suinos/indicador-do-suino-vivo-cepea-esalq",
    "leite": "leite/leite-precos-ao-produtor-cepea-rs-litro",
    "laranja_industria": "laranja/laranja-industria",
    "laranja_in_natura": "laranja/laranja-pera-in-natura",
}

CEPEA_PRODUTOS = {
    "soja": "soja",
    "soja_parana": "soja",
    "milho": "milho",
    "cafe": "cafe",
    "cafe_arabica": "cafe",
    "boi": "boi-gordo",
    "boi_gordo": "boi-gordo",
    "trigo": "trigo",
    "algodao": "algodao",
    "arroz": "arroz",
    "acucar": "acucar",
    "acucar_refinado": "acucar",
    "frango_congelado": "frango",
    "frango_resfriado": "frango",
    "suino": "suino",
    "etanol_hidratado": "etanol",
    "etanol_anidro": "etanol",
    "leite": "leite",
    "laranja_industria": "laranja",
    "laranja_in_natura": "laranja",
}

CONAB_PRODUTOS = {
    "soja": "Soja",
    "milho": "Milho Total",
    "milho_1": "Milho 1a",
    "milho_2": "Milho 2a",
    "milho_3": "Milho 3a",
    "arroz": "Arroz Total",
    "arroz_irrigado": "Arroz Irrigado",
    "arroz_sequeiro": "Arroz Sequeiro",
    "feijao": "Feijão Total",
    "feijao_1": "Feijão 1a Total",
    "feijao_2": "Feijão 2a Total",
    "feijao_3": "Feijão 3a Total",
    "algodao": "Algodao Total",
    "algodao_pluma": "Algodao em Pluma",
    "trigo": "Trigo",
    "sorgo": "Sorgo",
    "aveia": "Aveia",
    "cevada": "Cevada",
    "canola": "Canola",
    "girassol": "Girassol",
    "mamona": "Mamona",
    "amendoim": "Amendoim Total",
    "centeio": "Centeio",
    "triticale": "Triticale",
    "gergelim": "Gergelim",
}

CONAB_UFS = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]

CONAB_REGIOES = ["NORTE", "NORDESTE", "CENTRO-OESTE", "SUDESTE", "SUL"]


class CacheSettings(BaseSettings):
    cache_dir: Path = Path.home() / ".agrobr" / "cache"
    db_name: str = "agrobr.duckdb"

    ttl_cepea_diario: int = 4 * 3600
    ttl_cepea_semanal: int = 24 * 3600
    ttl_conab: int = 24 * 3600
    ttl_ibge_pam: int = 168 * 3600
    ttl_ibge_lspa: int = 24 * 3600
    ttl_ibge_ppm: int = 7 * 24 * 3600
    ttl_ibge_abate: int = 7 * 24 * 3600
    ttl_ibge_censo_agro: int = 30 * 24 * 3600
    ttl_abiove: int = 7 * 24 * 3600
    ttl_anda: int = 7 * 24 * 3600
    ttl_deral: int = 24 * 3600
    ttl_imea: int = 12 * 3600
    ttl_inmet: int = 24 * 3600
    ttl_bcb: int = 24 * 3600
    ttl_comexstat: int = 24 * 3600
    ttl_nasa_power: int = 7 * 24 * 3600
    ttl_desmatamento: int = 24 * 3600
    ttl_mapbiomas: int = 7 * 24 * 3600
    ttl_conab_progresso: int = 12 * 3600
    ttl_queimadas: int = 12 * 3600
    ttl_usda: int = 24 * 3600
    ttl_b3: int = 4 * 3600
    ttl_anp_diesel: int = 7 * 24 * 3600
    ttl_antaq: int = 7 * 24 * 3600
    ttl_antt_pedagio: int = 7 * 24 * 3600
    ttl_conab_ceasa: int = 4 * 3600

    stale_multiplier: float = 12.0

    offline_mode: bool = False
    strict_mode: bool = False
    save_to_history: bool = True

    cache_max_age_days: int = 30
    history_max_age_days: int = 0

    model_config = SettingsConfigDict(env_prefix="AGROBR_CACHE_")


class HTTPSettings(BaseSettings):
    timeout_connect: float = 10.0
    timeout_read: float = 30.0
    timeout_write: float = 10.0
    timeout_pool: float = 10.0

    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0
    retry_exponential_base: int = 2

    rate_limit_abiove: float = 3.0
    rate_limit_anda: float = 3.0
    rate_limit_anp_diesel: float = 2.0
    rate_limit_antaq: float = 1.0
    rate_limit_antt_pedagio: float = 2.0
    rate_limit_bcb: float = 1.0
    rate_limit_cepea: float = 2.0
    rate_limit_comexstat: float = 2.0
    rate_limit_conab: float = 3.0
    rate_limit_deral: float = 3.0
    rate_limit_ibge: float = 1.0
    rate_limit_imea: float = 1.0
    rate_limit_inmet: float = 0.5
    rate_limit_nasa_power: float = 1.0
    rate_limit_noticias_agricolas: float = 2.0
    rate_limit_desmatamento: float = 2.0
    rate_limit_mapbiomas: float = 2.0
    rate_limit_queimadas: float = 1.0
    rate_limit_usda: float = 1.0
    rate_limit_b3: float = 1.0
    rate_limit_conab_ceasa: float = 2.0
    rate_limit_default: float = 1.0

    model_config = SettingsConfigDict(env_prefix="AGROBR_HTTP_")


class AlertSettings(BaseSettings):
    enabled: bool = True

    slack_webhook: str | None = None
    discord_webhook: str | None = None

    sendgrid_api_key: str | None = None
    email_from: str = "alerts@agrobr.dev"
    email_to: list[str] = []

    alert_on_parse_error: bool = True
    alert_on_layout_change: bool = True
    alert_on_source_down: bool = True
    alert_on_anomaly: bool = True

    model_config = SettingsConfigDict(env_prefix="AGROBR_ALERT_")


class TelemetrySettings(BaseSettings):
    enabled: bool = False
    endpoint: str = "https://telemetry.agrobr.dev/v1/events"
    batch_size: int = 10
    flush_interval_seconds: int = 60

    model_config = SettingsConfigDict(env_prefix="AGROBR_TELEMETRY_")


CONFIDENCE_HIGH: float = 0.85
CONFIDENCE_MEDIUM: float = 0.70
CONFIDENCE_LOW: float = 0.50

RETRIABLE_STATUS_CODES: set[int] = {408, 429, 500, 502, 503, 504}
