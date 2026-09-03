from __future__ import annotations

import re
from functools import partial
from typing import Any
from urllib.parse import quote

import httpx
import structlog

from agrobr.constants import URLS, Fonte
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

FOCUS_BASE = URLS[Fonte.BCB]["focus"]

TIMEOUT = get_timeout(read=30.0)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


async def fetch_focus(
    indicador: str,
    *,
    top: int = 1000,
    data_inicial: str | None = None,
    max_registros: int | None = None,
) -> tuple[list[dict[str, Any]], str]:
    if data_inicial is not None and not _DATE_RE.match(data_inicial):
        raise ValueError(f"data_inicial inválida (esperado YYYY-MM-DD): {data_inicial!r}")
    if max_registros is not None and max_registros < 1:
        raise ValueError(f"max_registros deve ser >= 1: {max_registros}")

    safe_indicador = indicador.replace("'", "''")
    endpoint = "ExpectativasMercadoAnuais"
    url = f"{FOCUS_BASE}/{endpoint}"

    all_records: list[dict[str, Any]] = []
    skip = 0

    logger.info(
        "bcb_focus_request",
        indicador=indicador,
        top=top,
        data_inicial=data_inicial,
        max_registros=max_registros,
    )

    filtro_expr = f"Indicador eq '{safe_indicador}'"
    if data_inicial:
        filtro_expr += f" and Data ge '{data_inicial}'"
    filtro = quote(filtro_expr, safe="'")

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        while True:
            page_url = (
                f"{url}?$format=json&$filter={filtro}&$orderby=Data%20desc&$top={top}&$skip={skip}"
            )

            response = await retry_on_status(
                partial(client.get, page_url),
                source="bcb",
            )

            response.raise_for_status()
            data = responses.parse_json_response(response, source="bcb_focus", url=page_url)

            records = data.get("value", [])
            if not records:
                break

            all_records.extend(records)
            logger.debug(
                "bcb_focus_page_fetched",
                skip=skip,
                records_in_page=len(records),
                total_so_far=len(all_records),
            )

            if max_registros is not None and len(all_records) >= max_registros:
                all_records = all_records[:max_registros]
                break

            if len(records) < top:
                break

            skip += top

    logger.info("bcb_focus_ok", indicador=indicador, total_records=len(all_records))
    return all_records, url
