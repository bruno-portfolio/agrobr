"""Health check registry — maps every Fonte to its connectivity config."""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

from agrobr.constants import URLS, Fonte


@dataclass(frozen=True)
class SourceHealthConfig:
    source: Fonte
    url: str
    method: Literal["GET", "HEAD"] = "GET"
    timeout: float = 15.0
    expected_status: int = 200
    follow_redirects: bool = True
    verify: bool | ssl.SSLContext = True
    has_deep_check: bool = False
    tier: Literal["critical", "standard", "best_effort"] = "standard"
    requires_api_key: bool = False
    api_key_env_var: str | None = None
    soft_block_codes: tuple[int, ...] = ()


def _ckan_package_url(api_url: str, slug: str) -> str:
    return f"{api_url}/package_show?id={slug}"


def _wfs_capabilities_url(url: str) -> str:
    return f"{url}?service=WFS&request=GetCapabilities"


def _arcgis_directory_url(url: str) -> str:
    return f"{url}?f=pjson"


def _legacy_tls_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.set_ciphers("DEFAULT:@SECLEVEL=1")
    return context


def _build_registry() -> dict[Fonte, SourceHealthConfig]:
    """Build the registry by iterating constants.URLS with per-source overrides."""
    stable_year = date.today().year - 2
    overrides: dict[Fonte, dict[str, Any]] = {
        Fonte.ANA: {
            "url": _arcgis_directory_url(URLS[Fonte.ANA]["arcgis"]),
        },
        Fonte.ANP_DIESEL: {
            "url": URLS[Fonte.ANP_DIESEL]["vendas_diesel_csv"],
        },
        Fonte.ANTAQ: {
            "url": f"{URLS[Fonte.ANTAQ]['bulk_txt']}/Mercadoria.zip",
            "method": "HEAD",
            "tier": "best_effort",
        },
        Fonte.ANTT_PEDAGIO: {
            "url": _ckan_package_url(
                f"{URLS[Fonte.ANTT_PEDAGIO]['base']}/api/3/action",
                "volume-trafego-praca-pedagio",
            ),
        },
        Fonte.IBGE: {
            "url": (
                f"{URLS[Fonte.IBGE]['api']}/values/t/5457/n1/all/v/allxp/p/last%201/c782/40124"
            ),
        },
        Fonte.BCB: {
            "url": URLS[Fonte.BCB]["base"],
        },
        Fonte.CEPEA: {
            "has_deep_check": True,
            "tier": "critical",
            "soft_block_codes": (403,),
        },
        Fonte.COMEXSTAT: {
            "url": f"{URLS[Fonte.COMEXSTAT]['bulk_csv']}/EXP_{stable_year}.csv",
            "method": "HEAD",
            "verify": False,
        },
        Fonte.CONAB: {
            "url": URLS[Fonte.CONAB]["boletim_graos"],
        },
        Fonte.DEFENSIVOS: {
            "url": URLS[Fonte.DEFENSIVOS]["formulados"],
            "method": "HEAD",
        },
        Fonte.FUNAI: {
            "url": _wfs_capabilities_url(URLS[Fonte.FUNAI]["geoserver"]),
        },
        Fonte.ICMBIO: {
            "url": _wfs_capabilities_url(URLS[Fonte.ICMBIO]["geoserver"]),
        },
        Fonte.INCRA: {
            "url": _wfs_capabilities_url(URLS[Fonte.INCRA]["geoserver"]),
        },
        Fonte.LISTA_SUJA: {
            "url": URLS[Fonte.LISTA_SUJA]["download"],
        },
        Fonte.MAPA_PSR: {
            "url": URLS[Fonte.MAPA_PSR]["dataset"],
        },
        Fonte.IMEA: {
            "url": URLS[Fonte.IMEA]["cotacoes"],
        },
        Fonte.USDA: {
            "requires_api_key": True,
            "api_key_env_var": "AGROBR_USDA_API_KEY",
        },
        Fonte.INMET: {
            "requires_api_key": True,
            "api_key_env_var": "AGROBR_INMET_TOKEN",
        },
        Fonte.COMTRADE: {
            "requires_api_key": True,
            "api_key_env_var": "AGROBR_COMTRADE_API_KEY",
        },
        Fonte.SFB: {
            "url": _arcgis_directory_url(URLS[Fonte.SFB]["arcgis"]),
        },
        Fonte.SICAR: {
            "url": _wfs_capabilities_url(URLS[Fonte.SICAR]["geoserver"]),
            "verify": _legacy_tls_context(),
        },
        Fonte.RNC: {
            "url": URLS[Fonte.RNC]["cultivarweb"],
        },
        Fonte.ZARC: {
            "url": _ckan_package_url(
                URLS[Fonte.ZARC]["ckan_api"],
                "tabua-de-risco-zoneamento-agricola-de-risco-climatico",
            ),
        },
    }

    registry: dict[Fonte, SourceHealthConfig] = {}

    for fonte in Fonte:
        urls = URLS.get(fonte)
        if not urls:
            continue

        fonte_overrides = overrides.get(fonte, {})
        url = fonte_overrides.pop("url", urls.get("base", ""))

        registry[fonte] = SourceHealthConfig(source=fonte, url=url, **fonte_overrides)

    return registry


HEALTH_REGISTRY: dict[Fonte, SourceHealthConfig] = _build_registry()

SOURCE_DATASET_MAP: dict[str, list[str]] = {
    "abiove": ["exportacao"],
    "anda": ["fertilizante"],
    "antaq": ["movimentacao_portuaria"],
    "b3": ["futuros_agricolas"],
    "bcb": ["credito_rural"],
    "cepea": ["preco_diario"],
    "comexstat": ["exportacao", "importacao"],
    "comtrade": ["comercio_internacional"],
    "conab": [
        "balanco",
        "custo_producao",
        "estimativa_safra",
        "preco_atacado",
        "producao_anual",
        "progresso_safra",
        "serie_historica_safra",
    ],
    "deral": ["condicao_lavouras"],
    "desmatamento": ["desmatamento"],
    "ibge": [
        "abate_trimestral",
        "censo_agropecuario",
        "censo_agropecuario_historico",
        "censo_agropecuario_legado",
        "censo_agropecuario_municipal_1985",
        "estimativa_safra",
        "extrativismo_vegetal",
        "leite_industrial",
        "pecuaria_municipal",
        "pib_agro",
        "producao_anual",
        "silvicultura",
    ],
    "inmet": ["clima"],
    "mapa_psr": ["seguro_rural"],
    "mapbiomas": ["uso_do_solo"],
    "nasa_power": ["clima"],
    "queimadas": ["queimadas"],
    "sicar": ["cadastro_rural"],
    "usda": ["oferta_demanda_global"],
    "zarc": ["zoneamento_agricola"],
}


def get_affected_datasets(source: Fonte) -> list[str]:
    """Return datasets affected when *source* is down."""
    return SOURCE_DATASET_MAP.get(source.value, [])
