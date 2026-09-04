from __future__ import annotations

import httpx
import structlog

from agrobr.constants import MIN_WFS_SIZE, URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils.io import extract_csv_from_zip

logger = structlog.get_logger()

BASE_URL = URLS[Fonte.QUEIMADAS]["dados_abertos"]

ANUAL_URL = f"{BASE_URL}/anual/Brasil_todos_sats"

TIMEOUT = get_timeout(read=120.0)


async def _try_fetch(client: httpx.AsyncClient, url: str) -> bytes | None:
    logger.debug("queimadas_request", url=url)
    response = await retry_on_status(
        lambda: client.get(url),
        source="queimadas",
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()

    content = response.content
    if len(content) < MIN_WFS_SIZE:
        logger.debug("queimadas_response_too_small_detail", url=url)
        logger.warning(
            "queimadas_response_too_small",
            source="queimadas",
            size=len(content),
        )
        return None
    return content


async def fetch_focos_diario(data: str) -> tuple[bytes, str]:
    url = f"{BASE_URL}/diario/Brasil/focos_diario_br_{data}.csv"
    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as c:
        content = await _try_fetch(c, url)
    if content is None:
        raise SourceUnavailableError(
            source="queimadas",
            url=url,
            last_error=(
                "HTTP 404: o arquivo diário só existe para os últimos dias; "
                "remova o parâmetro dia para usar o arquivo mensal"
            ),
        )
    logger.info("queimadas_csv_found", source="queimadas", size=len(content))
    return content, url


async def fetch_focos_mensal(ano: int, mes: int) -> tuple[bytes, str]:
    periodo = f"{ano:04d}{mes:02d}"
    csv_url = f"{BASE_URL}/mensal/Brasil/focos_mensal_br_{periodo}.csv"
    zip_url = f"{BASE_URL}/mensal/Brasil/focos_mensal_br_{periodo}.zip"
    anual_url = f"{ANUAL_URL}/focos_br_todos-sats_{ano:04d}.zip"

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as c:
        content = await _try_fetch(c, csv_url)
        if content is not None:
            logger.info("queimadas_csv_found", source="queimadas", size=len(content))
            return content, csv_url

        logger.debug("queimadas_csv_404_trying_zip", periodo=periodo)
        content = await _try_fetch(c, zip_url)
        if content is not None:
            csv_bytes = extract_csv_from_zip(content, source="queimadas", url=zip_url)
            logger.info("queimadas_zip_found", source="queimadas", size=len(csv_bytes))
            return csv_bytes, zip_url

        logger.debug("queimadas_zip_404_trying_anual", ano=ano)
        content = await _try_fetch(c, anual_url)
        if content is not None:
            csv_bytes = extract_csv_from_zip(content, source="queimadas", url=anual_url)
            logger.info(
                "queimadas_anual_found",
                source="queimadas",
                size_raw=len(csv_bytes),
                filtering_month=mes,
            )
            return csv_bytes, anual_url

    raise SourceUnavailableError(
        source="queimadas",
        url=csv_url,
        last_error=(
            f"Focos mensal {periodo} nao encontrado. "
            f"Tentativas: .csv, .zip mensal, .zip anual ({ano})"
        ),
    )
