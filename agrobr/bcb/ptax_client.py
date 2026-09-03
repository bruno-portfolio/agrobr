from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import httpx
import structlog

from agrobr.constants import URLS, Fonte
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils.time import utcnow

logger = structlog.get_logger()

PTAX_BASE = URLS[Fonte.BCB]["ptax"]
TIMEOUT = get_timeout(read=30.0)
PTAX_MAX_RETRIES = 4


def _to_api_date(date_br: str) -> str:
    dt = datetime.strptime(date_br, "%d/%m/%Y")
    return dt.strftime("%m-%d-%Y")


async def fetch_ptax(
    *,
    data: str | None = None,
    data_inicial: str | None = None,
    data_final: str | None = None,
) -> tuple[list[dict[str, Any]], str]:
    if data:
        api_date = _to_api_date(data)
        url = f"{PTAX_BASE}/CotacaoDolarDia(dataCotacao=@d)?@d='{api_date}'&$format=json"
    elif data_inicial and data_final:
        di = _to_api_date(data_inicial)
        df = _to_api_date(data_final)
        url = (
            f"{PTAX_BASE}/CotacaoDolarPeriodo(dataInicial=@di,dataFinalCotacao=@df)"
            f"?@di='{di}'&@df='{df}'&$format=json"
        )
    else:
        hoje = utcnow()
        inicio = hoje - timedelta(days=30)
        di = inicio.strftime("%m-%d-%Y")
        df = hoje.strftime("%m-%d-%Y")
        url = (
            f"{PTAX_BASE}/CotacaoDolarPeriodo(dataInicial=@di,dataFinalCotacao=@df)"
            f"?@di='{di}'&@df='{df}'&$format=json"
        )

    logger.info("bcb_ptax_request", url=url)

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="bcb",
            max_attempts=PTAX_MAX_RETRIES,
        )
        response.raise_for_status()
        payload = responses.parse_json_response(response, source="bcb_ptax", url=url)

    records = payload.get("value", [])

    if not records:
        logger.warning("bcb_ptax_empty", url=url)

    logger.info("bcb_ptax_ok", records=len(records))
    return records, url
