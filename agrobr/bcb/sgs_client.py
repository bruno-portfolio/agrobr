from __future__ import annotations

import httpx
import structlog

from agrobr.constants import URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

SGS_BASE = URLS[Fonte.BCB]["sgs"]

TIMEOUT = get_timeout(read=30.0)


async def fetch_sgs(
    codigo: int,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
) -> tuple[list[dict[str, str]], str]:
    if ultimos and ultimos > 0 and not data_inicial and not data_final:
        url = f"{SGS_BASE}.{codigo}/dados/ultimos/{ultimos}"
    else:
        url = f"{SGS_BASE}.{codigo}/dados"

    params: dict[str, str] = {"formato": "json"}
    if data_inicial:
        params["dataInicial"] = data_inicial
    if data_final:
        params["dataFinal"] = data_final

    logger.info(
        "bcb_sgs_request",
        codigo=codigo,
        data_inicial=data_inicial,
        data_final=data_final,
        ultimos=ultimos,
    )

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url, params=params),
            source="bcb",
        )

        response.raise_for_status()
        data = response.json()

    if not data or not isinstance(data, list):
        raise SourceUnavailableError(
            source="bcb_sgs",
            url=url,
            last_error=f"Resposta vazia para serie {codigo}",
        )

    logger.info("bcb_sgs_ok", codigo=codigo, records=len(data))
    return data, url
