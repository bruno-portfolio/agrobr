from __future__ import annotations

import os
from typing import Any, NamedTuple

import httpx
import structlog

from agrobr.constants import URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BASE_URL = URLS[Fonte.USDA]["base"]

TIMEOUT = get_timeout(read=60.0)


class RespostaPSD(NamedTuple):
    url: str
    corpo: bytes
    dados: Any
    status: int = 200


def _get_api_key(api_key: str | None = None) -> str:
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


def _codigo_de_erro(response: httpx.Response) -> str:
    try:
        return str(response.json()["error"]["code"])
    except (ValueError, KeyError, TypeError):
        return ""


async def _fetch_json(url: str, api_key: str) -> RespostaPSD:
    headers = UserAgentRotator.get_bot_headers()
    headers["X-Api-Key"] = api_key

    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
        logger.debug("usda_request", url=url)
        response = await retry_on_status(
            lambda: client.get(url, headers=headers),
            source="usda",
        )

        codigo = _codigo_de_erro(response) if response.status_code in (401, 403) else ""
        if codigo.startswith("API_KEY"):
            raise SourceUnavailableError(
                source="usda",
                url=url,
                last_error=(
                    f"O gateway do USDA recusou a chave (HTTP {response.status_code}, {codigo}). "
                    "Verifique AGROBR_USDA_API_KEY."
                ),
            )

        if response.status_code == 404:
            return RespostaPSD(url, response.content, [], 404)

        responses.raise_for_status(response, source="usda")
        dados = responses.parse_json_response(response, source="usda", url=url)
        return RespostaPSD(url, response.content, dados)


async def fetch_psd_country(
    commodity_code: str,
    country_code: str,
    market_year: int,
    api_key: str | None = None,
) -> RespostaPSD:
    key = _get_api_key(api_key)
    url = f"{BASE_URL}/commodity/{commodity_code}/country/{country_code}/year/{market_year}"
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
) -> RespostaPSD:
    key = _get_api_key(api_key)
    url = f"{BASE_URL}/commodity/{commodity_code}/world/year/{market_year}"
    logger.info("usda_fetch_psd_world", commodity=commodity_code, year=market_year)
    return await _fetch_json(url, key)


async def fetch_psd_all_countries(
    commodity_code: str,
    market_year: int,
    api_key: str | None = None,
) -> RespostaPSD:
    key = _get_api_key(api_key)
    url = f"{BASE_URL}/commodity/{commodity_code}/country/all/year/{market_year}"
    logger.info("usda_fetch_psd_all", commodity=commodity_code, year=market_year)
    return await _fetch_json(url, key)
