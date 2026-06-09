from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = [
    "CEPEA_PRODUTOS",
    "CONAB_PRODUTOS",
    "CONAB_REGIOES",
    "CONAB_UFS",
    "CacheSettings",
    "Fonte",
    "HTTPSettings",
    "NOTICIAS_AGRICOLAS_PRODUTOS",
    "URLS",
]


class Fonte(StrEnum):
    ABIOVE = "abiove"
    ACERVO_FUNDIARIO = "acervo_fundiario"
    ANA = "ana"
    ANDA = "anda"
    ANEC = "anec"
    ANP_DIESEL = "anp_diesel"
    ANTAQ = "antaq"
    ANTT_PEDAGIO = "antt_pedagio"
    B3 = "b3"
    BCB = "bcb"
    CEPEA = "cepea"
    COMEXSTAT = "comexstat"
    COMTRADE = "comtrade"
    CONAB = "conab"
    DEFENSIVOS = "defensivos"
    DERAL = "deral"
    EMBRAPA_SOLOS = "embrapa_solos"
    FUNAI = "funai"
    IBAMA = "ibama"
    IBGE = "ibge"
    ICMBIO = "icmbio"
    IMEA = "imea"
    INCRA = "incra"
    INMET = "inmet"
    LISTA_SUJA = "lista_suja"
    MAPA_PSR = "mapa_psr"
    MAPBIOMAS_ALERTA = "mapbiomas_alerta"
    NASA_POWER = "nasa_power"
    NOTICIAS_AGRICOLAS = "noticias_agricolas"
    DESMATAMENTO = "desmatamento"
    MAPBIOMAS = "mapbiomas"
    QUEIMADAS = "queimadas"
    SFB = "sfb"
    SICAR = "sicar"
    USDA = "usda"
    RNC = "rnc"
    RIO_VERDE = "rio_verde"
    ZARC = "zarc"


URLS = {
    Fonte.ABIOVE: {
        "base": "https://abiove.org.br",
        "estatisticas": "https://abiove.org.br/estatisticas",
        "exportacao": "https://abiove.org.br/abiove_content/Abiove",
    },
    Fonte.ACERVO_FUNDIARIO: {
        "base": "https://certificacao.incra.gov.br",
        "download": "https://certificacao.incra.gov.br/csv_shp/zip/",
    },
    Fonte.ANA: {
        "base": "https://portal1.snirh.gov.br",
        "arcgis": "https://portal1.snirh.gov.br/server/rest/services/dados_abertos",
    },
    Fonte.ANDA: {
        "base": "https://anda.org.br",
        "estatisticas": "https://anda.org.br/recursos/",
    },
    Fonte.ANEC: {
        "base": "https://www.anec.com.br",
        "search": "https://www.anec.com.br/search",
        "article": "https://www.anec.com.br/article",
    },
    Fonte.ANP_DIESEL: {
        "base": "https://www.gov.br/anp/pt-br",
        "shlp": "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp",
        "vendas_diesel_csv": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2025.csv",
    },
    Fonte.ANTAQ: {
        "base": "https://web3.antaq.gov.br/ea/sense/download.html",
        "bulk_txt": "https://web3.antaq.gov.br/ea/txt",
    },
    Fonte.ANTT_PEDAGIO: {
        "base": "https://dados.antt.gov.br",
        "trafego": "https://dados.antt.gov.br/dataset/volume-trafego-praca-pedagio",
        "pracas": "https://dados.antt.gov.br/dataset/praca-de-pedagio",
    },
    Fonte.BCB: {
        "base": "https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata",
        "ptax": "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata",
        "sgs": "https://api.bcb.gov.br/dados/serie/bcdata.sgs",
        "focus": "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata",
    },
    Fonte.CEPEA: {
        "base": "https://www.cepea.org.br",
        "indicadores": "https://www.cepea.org.br/br/indicador",
    },
    Fonte.COMEXSTAT: {
        "base": "https://comexstat.mdic.gov.br",
        "bulk_csv": "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm",
    },
    Fonte.COMTRADE: {
        "base": "https://comtradeapi.un.org",
        "auth": "https://comtradeapi.un.org/data/v1/get",
        "guest": "https://comtradeapi.un.org/public/v1/preview",
    },
    Fonte.CONAB: {
        "base": "https://www.gov.br/conab",
        "boletim_graos": "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/safra-de-graos/boletim-da-safra-de-graos",
        "ceasa_prohort": "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/plugin/cda/api/doQuery",
    },
    Fonte.DEFENSIVOS: {
        "base": "https://dados.agricultura.gov.br",
        "formulados": "https://dados.agricultura.gov.br/dataset/6c913699-e82e-4da3-a0a1-fb6c431e367f/resource/d30b30d7-e256-484e-9ab8-cd40974e1238/download/agrofitprodutosformulados.csv",
        "tecnicos": "https://dados.agricultura.gov.br/dataset/6c913699-e82e-4da3-a0a1-fb6c431e367f/resource/a200c70b-e025-4a9a-be1b-ec7275d7921f/download/agrofitprodutostecnicos.csv",
    },
    Fonte.DERAL: {
        "base": "https://www.agricultura.pr.gov.br/deral",
        "downloads": "https://www.agricultura.pr.gov.br/system/files/publico/Safras",
    },
    Fonte.EMBRAPA_SOLOS: {
        "base": "https://geoinfo.dados.embrapa.br",
        "geoserver": "https://geoinfo.dados.embrapa.br/geoserver/ows",
    },
    Fonte.IBGE: {
        "base": "https://sidra.ibge.gov.br",
        "api": "https://apisidra.ibge.gov.br",
        "ftp_censo_agro_1996": "https://ftp.ibge.gov.br/Censo_Agropecuario/Censo_Agropecuario_1995_96",
    },
    Fonte.LISTA_SUJA: {
        "base": "https://www.gov.br/trabalho-e-emprego",
        "download": "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/areas-de-atuacao/cadastro_de_empregadores.pdf",
    },
    Fonte.MAPA_PSR: {
        "base": "https://dados.agricultura.gov.br",
        "dataset": "https://dados.agricultura.gov.br/dataset/baefdc68-9bad-4204-83e8-f2888b79ab48",
    },
    Fonte.IMEA: {
        "base": "https://api1.imea.com.br/api",
        "cotacoes": "https://api1.imea.com.br/api/v2/mobile/cadeias",
    },
    Fonte.FUNAI: {
        "base": "https://geoserver.funai.gov.br",
        "geoserver": "https://geoserver.funai.gov.br/geoserver/Funai/ows",
    },
    Fonte.IBAMA: {
        "base": "https://www.ibama.gov.br",
        "geoserver": "https://siscom.ibama.gov.br/geoserver/wfs",
        "dados_abertos": "https://dadosabertos.ibama.gov.br",
    },
    Fonte.ICMBIO: {
        "base": "https://geoservicos.inde.gov.br",
        "geoserver": "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
    },
    Fonte.INCRA: {
        "base": "https://cmr.funai.gov.br",
        "geoserver": "https://cmr.funai.gov.br/geoserver/ows",
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
        "dataverse": "https://data.mapbiomas.org/api/access/datafile",
        "biome_state_file_id": "457",
        "biome_state_municipality_file_id": "254",
    },
    Fonte.MAPBIOMAS_ALERTA: {
        "base": "https://plataforma.alerta.mapbiomas.org",
        "graphql": "https://plataforma.alerta.mapbiomas.org/api/v2/graphql",
    },
    Fonte.QUEIMADAS: {
        "base": "https://terrabrasilis.dpi.inpe.br/queimadas/portal/",
        "dados_abertos": "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv",
    },
    Fonte.SFB: {
        "base": "https://mapas.florestal.gov.br",
        "arcgis": "https://mapas.florestal.gov.br/server/rest/services",
    },
    Fonte.SICAR: {
        "base": "https://www.car.gov.br",
        "geoserver": "https://geoserver.car.gov.br/geoserver/sicar/wfs",
    },
    Fonte.B3: {
        "base": "https://www.b3.com.br",
        "ajustes_zip": "https://www.b3.com.br/pesquisapregao/download",
        "arquivos": "https://arquivos.b3.com.br/api/download",
    },
    Fonte.RNC: {
        "base": "https://sistemas.agricultura.gov.br/snpc",
        "cultivarweb": "https://sistemas.agricultura.gov.br/snpc/cultivarweb",
    },
    Fonte.RIO_VERDE: {
        "base": "https://fundacaorioverde.com.br",
        "publicacoes": "https://fundacaorioverde.com.br/publicacoes/",
    },
    Fonte.ZARC: {
        "base": "https://dados.agricultura.gov.br",
        "ckan_api": "https://dados.agricultura.gov.br/api/3/action",
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
    rate_limit_acervo_fundiario: float = 3.0
    rate_limit_ana: float = 2.0
    rate_limit_anda: float = 3.0
    rate_limit_anec: float = 3.0
    rate_limit_anp_diesel: float = 2.0
    rate_limit_antaq: float = 1.0
    rate_limit_antt_pedagio: float = 2.0
    rate_limit_bcb: float = 1.0
    rate_limit_cepea: float = 5.0
    rate_limit_comexstat: float = 2.0
    rate_limit_comtrade: float = 2.0
    rate_limit_conab: float = 3.0
    rate_limit_defensivos: float = 2.0
    rate_limit_deral: float = 3.0
    rate_limit_ibge: float = 1.0
    rate_limit_imea: float = 1.0
    rate_limit_lista_suja: float = 2.0
    rate_limit_funai: float = 2.0
    rate_limit_ibama: float = 2.0
    rate_limit_icmbio: float = 2.0
    rate_limit_incra: float = 2.0
    rate_limit_inmet: float = 0.5
    rate_limit_nasa_power: float = 1.0
    rate_limit_noticias_agricolas: float = 2.0
    rate_limit_desmatamento: float = 2.0
    rate_limit_mapbiomas: float = 2.0
    rate_limit_mapbiomas_alerta: float = 3.0
    rate_limit_queimadas: float = 1.0
    rate_limit_sfb: float = 2.0
    rate_limit_sicar: float = 2.0
    rate_limit_usda: float = 1.0
    rate_limit_b3: float = 1.0
    rate_limit_embrapa_solos: float = 2.0
    rate_limit_rnc: float = 3.0
    rate_limit_rio_verde: float = 3.0
    rate_limit_zarc: float = 2.0
    rate_limit_conab_ceasa: float = 2.0
    rate_limit_default: float = 1.0

    max_concurrent_default: int = 1
    max_concurrent_ana: int = 1
    max_concurrent_b3: int = 3
    max_concurrent_ibge: int = 3

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
    alert_on_soft_block: bool = True

    consecutive_failures_warning: int = 2
    consecutive_failures_critical: int = 3
    alert_on_recovery: bool = True
    discord_embed_char_limit: int = 3900

    model_config = SettingsConfigDict(env_prefix="AGROBR_ALERT_")


_CEPEA_ENDPOINTS: tuple[str, ...] = (
    "https://www.cepea.org.br",
    "https://cepea.org.br",
)

CONFIDENCE_HIGH: float = 0.85
CONFIDENCE_LOW: float = 0.50

RETRIABLE_STATUS_CODES: set[int] = {408, 429, 500, 502, 503, 504}

MIN_WFS_SIZE: int = 50
MIN_CSV_SIZE: int = 100
MIN_HTML_SIZE: int = 500
MIN_ZIP_SIZE: int = 500
MIN_XLSX_SIZE: int = 1_000
MIN_PDF_SIZE: int = 10_000
MIN_HTML_PAGE_SIZE: int = 5_000
