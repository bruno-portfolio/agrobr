"""Constantes e configurações do agrobr."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic_settings import BaseSettings


class Fonte(StrEnum):
    CEPEA = "cepea"
    CONAB = "conab"
    IBGE = "ibge"
    NOTICIAS_AGRICOLAS = "noticias_agricolas"  # Fonte alternativa para CEPEA


URLS = {
    Fonte.CEPEA: {
        "base": "https://www.cepea.org.br",
        "indicadores": "https://www.cepea.org.br/br/indicador",
    },
    Fonte.CONAB: {
        "base": "https://www.gov.br/conab",
        "safras": "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras",
        "boletim_graos": "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/safra-de-graos/boletim-da-safra-de-graos",
    },
    Fonte.IBGE: {
        "base": "https://sidra.ibge.gov.br",
        "api": "https://apisidra.ibge.gov.br",
    },
    Fonte.NOTICIAS_AGRICOLAS: {
        "base": "https://www.noticiasagricolas.com.br",
        "cotacoes": "https://www.noticiasagricolas.com.br/cotacoes",
    },
}

# Mapeamento de produtos para URLs do Notícias Agrícolas (indicadores CEPEA)
NOTICIAS_AGRICOLAS_PRODUTOS = {
    "soja": "soja/soja-indicador-cepea-esalq-porto-paranagua",
    "soja_parana": "soja/indicador-cepea-esalq-soja-parana",
    "milho": "milho/milho-indicador-cepea-esalq-campinas",
    "boi": "boi/boi-gordo-indicador-cepea-esalq-sao-paulo",
    "boi_gordo": "boi/boi-gordo-indicador-cepea-esalq-sao-paulo",
    "cafe": "cafe/cafe-arabica-indicador-cepea-esalq",
    "cafe_arabica": "cafe/cafe-arabica-indicador-cepea-esalq",
    "algodao": "algodao/algodao-indicador-cepea-esalq",
    "trigo": "trigo/trigo-indicador-cepea-esalq-parana",
}

CEPEA_PRODUTOS = {
    "soja": "soja",
    "milho": "milho",
    "cafe": "cafe",
    "cafe_arabica": "cafe",
    "boi": "boi-gordo",
    "boi_gordo": "boi-gordo",
    "trigo": "trigo",
    "algodao": "algodao",
    "arroz": "arroz",
    "frango": "frango",
    "suino": "suino",
    "acucar": "acucar",
    "etanol": "etanol",
    "etanol_hidratado": "etanol",
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
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
    "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
    "SE", "SP", "TO",
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

    stale_multiplier: float = 12.0

    offline_mode: bool = False
    strict_mode: bool = False
    save_to_history: bool = True

    cache_max_age_days: int = 30
    history_max_age_days: int = 0

    class Config:
        env_prefix = "AGROBR_CACHE_"


class HTTPSettings(BaseSettings):
    timeout_connect: float = 10.0
    timeout_read: float = 30.0
    timeout_write: float = 10.0
    timeout_pool: float = 10.0

    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0
    retry_exponential_base: int = 2

    rate_limit_cepea: float = 2.0
    rate_limit_conab: float = 3.0
    rate_limit_ibge: float = 1.0
    rate_limit_noticias_agricolas: float = 2.0

    class Config:
        env_prefix = "AGROBR_HTTP_"


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
    alert_on_anomaly: bool = False

    class Config:
        env_prefix = "AGROBR_ALERT_"


class TelemetrySettings(BaseSettings):
    enabled: bool = False
    endpoint: str = "https://telemetry.agrobr.dev/v1/events"
    batch_size: int = 10
    flush_interval_seconds: int = 60

    class Config:
        env_prefix = "AGROBR_TELEMETRY_"


CONFIDENCE_HIGH: float = 0.85
CONFIDENCE_MEDIUM: float = 0.70
CONFIDENCE_LOW: float = 0.50

RETRIABLE_STATUS_CODES: set[int] = {408, 429, 500, 502, 503, 504}
