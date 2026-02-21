from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_XLSX_SIZE, HTTPSettings
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=180.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)


async def download_xlsx(url: str) -> bytes:
    logger.info("anp_diesel_download", url=url)

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers=UserAgentRotator.get_headers(source="anp_diesel"),
        follow_redirects=True,
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="anp_diesel",
        )

        response.raise_for_status()

        content = response.content
        if len(content) < MIN_XLSX_SIZE:
            from agrobr.exceptions import SourceUnavailableError

            raise SourceUnavailableError(
                source="anp_diesel",
                url=url,
                last_error=(
                    f"Downloaded file too small ({len(content)} bytes), "
                    f"expected a valid XLSX/XLS spreadsheet"
                ),
            )

        logger.info(
            "anp_diesel_download_ok",
            url=url,
            size_bytes=len(content),
        )
        return content


async def fetch_precos_municipios(periodo: str) -> bytes:
    from agrobr.alt.anp_diesel.models import PRECOS_MUNICIPIOS_URLS

    if periodo not in PRECOS_MUNICIPIOS_URLS:
        raise ValueError(
            f"Periodo '{periodo}' invalido. Disponiveis: {sorted(PRECOS_MUNICIPIOS_URLS.keys())}"
        )

    url = PRECOS_MUNICIPIOS_URLS[periodo]
    return await download_xlsx(url)


async def fetch_precos_estados() -> bytes:
    from agrobr.alt.anp_diesel.models import PRECOS_ESTADOS_URL

    return await download_xlsx(PRECOS_ESTADOS_URL)


async def fetch_precos_brasil() -> bytes:
    from agrobr.alt.anp_diesel.models import PRECOS_BRASIL_URL

    return await download_xlsx(PRECOS_BRASIL_URL)


async def fetch_vendas_m3() -> bytes:
    from agrobr.alt.anp_diesel.models import VENDAS_M3_URL

    return await download_xlsx(VENDAS_M3_URL)
