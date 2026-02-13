"""Cliente HTTP para USDA FAS OpenData API v2 (PSD Online).

API: https://apps.fas.usda.gov/OpenData/api
Auth: API_KEY header (grátis via api.data.gov/signup/)

Configuração: defina AGROBR_USDA_API_KEY no ambiente
ou passe api_key diretamente.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status

logger = structlog.get_logger()

BASE_URL = "https://apps.fas.usda.gov/OpenData/api"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=60.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)


def _get_api_key(api_key: str | None = None) -> str:
    """Obtém API key do parâmetro ou variável de ambiente.

    Args:
        api_key: Key fornecida diretamente. Se None, usa env var.

    Returns:
        API key string.

    Raises:
        SourceUnavailableError: Se nenhuma key configurada.
    """
    key = api_key or os.environ.get("AGROBR_USDA_API_KEY", "")
    if not key:
        raise SourceUnavailableError(
            source="usda",
            url=BASE_URL,
            last_error=(
                "USDA API key não configurada. "
                "Defina AGROBR_USDA_API_KEY ou passe api_key=. "
                "Obtenha em: https://api.data.gov/signup/"
            ),
        )
    return key


async def _fetch_json(
    url: str, api_key: str, params: dict[str, str] | None = None
) -> list[dict[str, Any]]:
    """Fetch JSON da API USDA com retry.

    Args:
        url: URL completa.
        api_key: USDA API key.
        params: Query params opcionais.

    Returns:
        Lista de dicts com dados JSON.

    Raises:
        SourceUnavailableError: Se API indisponível.
    """
    headers = {
        "Accept": "application/json",
        "API_KEY": api_key,
        "User-Agent": "agrobr/0.8.0 (https://github.com/your-org/agrobr)",
    }

    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
        logger.debug("usda_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url, headers=headers, params=params),
            source="usda",
        )

        if response.status_code == 401:
            raise SourceUnavailableError(
                source="usda",
                url=url,
                last_error="API key inválida (HTTP 401). Verifique AGROBR_USDA_API_KEY.",
            )

        if response.status_code == 404:
            return []

        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []


async def fetch_psd_country(
    commodity_code: str,
    country_code: str,
    market_year: int,
    api_key: str | None = None,
) -> list[dict[str, Any]]:
    """Busca dados PSD para commodity/país/ano.

    Args:
        commodity_code: Código commodity USDA (ex: "2222000").
        country_code: Código país USDA (ex: "BR").
        market_year: Ano do marketing year (ex: 2024).
        api_key: API key (ou usa env var).

    Returns:
        Lista de dicts com registros PSD.
    """
    key = _get_api_key(api_key)
    url = f"{BASE_URL}/psd/commodity/{commodity_code}/country/{country_code}/year/{market_year}"
    logger.info(
        "usda_fetch_psd",
        commodity=commodity_code,
        country=country_code,
        year=market_year,
    )
    return await _fetch_json(url, key)


async def fetch_psd_world(
    commodity_code: str,
    market_year: int,
    api_key: str | None = None,
) -> list[dict[str, Any]]:
    """Busca dados PSD world-aggregated para commodity/ano.

    Args:
        commodity_code: Código commodity USDA.
        market_year: Marketing year.
        api_key: API key.

    Returns:
        Lista de dicts com registros PSD.
    """
    key = _get_api_key(api_key)
    url = f"{BASE_URL}/psd/commodity/{commodity_code}/world/year/{market_year}"
    logger.info("usda_fetch_psd_world", commodity=commodity_code, year=market_year)
    return await _fetch_json(url, key)


async def fetch_psd_all_countries(
    commodity_code: str,
    market_year: int,
    api_key: str | None = None,
) -> list[dict[str, Any]]:
    """Busca dados PSD para commodity/ano em todos os países.

    Args:
        commodity_code: Código commodity USDA.
        market_year: Marketing year.
        api_key: API key.

    Returns:
        Lista de dicts com registros PSD.
    """
    key = _get_api_key(api_key)
    url = f"{BASE_URL}/psd/commodity/{commodity_code}/country/all/year/{market_year}"
    logger.info("usda_fetch_psd_all", commodity=commodity_code, year=market_year)
    return await _fetch_json(url, key)
