from __future__ import annotations

from datetime import date

import httpx
import structlog

from agrobr.constants import URLS, Fonte
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

SGS_BASE = URLS[Fonte.BCB]["sgs"]

TIMEOUT = get_timeout(read=30.0)


def _default_start_date() -> str:
    today = date.today()
    try:
        start = today.replace(year=today.year - 10)
    except ValueError:
        start = today.replace(year=today.year - 10, day=28)
    return start.strftime("%d/%m/%Y")


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
    if not data_inicial and not data_final and not ultimos:
        data_inicial = _default_start_date()
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
            max_attempts=2,
        )

        if 400 <= response.status_code < 500:
            try:
                payload = response.json()
            except ValueError:
                payload = None
            detail = payload.get("error") if isinstance(payload, dict) else None
            message = str(detail or f"HTTP {response.status_code} na API SGS")
            raise InvalidParameterError(message)
        if response.status_code >= 400:
            raise SourceUnavailableError(
                source="bcb_sgs",
                url=url,
                last_error=f"HTTP {response.status_code}",
            )
        data = responses.parse_json_response(response, source="bcb_sgs", url=url)

    if not data or not isinstance(data, list):
        raise SourceUnavailableError(
            source="bcb_sgs",
            url=url,
            last_error=f"Resposta vazia para serie {codigo}",
        )

    logger.info("bcb_sgs_ok", codigo=codigo, records=len(data))
    return data, url
