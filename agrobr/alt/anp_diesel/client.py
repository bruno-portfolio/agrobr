from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_CSV_SIZE, MIN_XLSX_SIZE
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

logger = structlog.get_logger()

TIMEOUT = get_timeout(read=180.0)


async def download_xlsx(url: str) -> bytes:
    logger.debug("anp_diesel_download", url=url)

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
        io_utils.validate_download(
            content,
            kinds=("xlsx",),
            source="anp_diesel",
            url=url,
            min_size=MIN_XLSX_SIZE,
        )

        logger.info(
            "anp_diesel_download_ok",
            source="anp_diesel",
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


async def download_csv(url: str) -> bytes:
    logger.debug("anp_diesel_download_csv", url=url)

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
        io_utils.validate_download(
            content,
            kinds=("csv",),
            source="anp_diesel",
            url=url,
            min_size=MIN_CSV_SIZE,
        )

        logger.info(
            "anp_diesel_download_csv_ok",
            source="anp_diesel",
            size_bytes=len(content),
        )
        return content


async def fetch_vendas_m3() -> bytes:
    from agrobr.alt.anp_diesel.models import VENDAS_DIESEL_CSV_URL

    return await download_csv(VENDAS_DIESEL_CSV_URL)
