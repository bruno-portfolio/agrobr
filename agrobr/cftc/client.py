from __future__ import annotations

from datetime import date

import httpx

from agrobr import _log
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

from . import models

logger = _log.get_logger(__name__)

TIMEOUT = get_timeout(read=60.0)

MAX_ROWS = 50000


def _soql_date(value: str | date) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(value).isoformat()


async def fetch_cot(
    codes: list[str],
    start: str | date | None = None,
    end: str | date | None = None,
    combined: bool = False,
) -> tuple[list[dict[str, str]], str, bytes]:
    if start and end and _soql_date(start) > _soql_date(end):
        raise InvalidParameterError(f"start ({start}) posterior a end ({end})")
    resource = "disaggregated_combined" if combined else "disaggregated_futures"
    url = URLS[Fonte.CFTC][resource]

    quoted = ",".join(f"'{c}'" for c in codes)
    where = f"cftc_contract_market_code in({quoted})"
    if start:
        where += f" AND report_date_as_yyyy_mm_dd >= '{_soql_date(start)}T00:00:00.000'"
    if end:
        where += f" AND report_date_as_yyyy_mm_dd <= '{_soql_date(end)}T00:00:00.000'"

    params = {
        "$where": where,
        "$order": "report_date_as_yyyy_mm_dd,cftc_contract_market_code",
        "$limit": str(MAX_ROWS),
    }

    logger.info(
        "cftc_cot_request",
        codes=codes,
        start=str(start) if start else None,
        end=str(end) if end else None,
        combined=combined,
    )

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as client:
        response = await retry_on_status(
            lambda: client.get(url, params=params),
            source="cftc",
        )
        responses.raise_for_status(response, source="cftc")
        data = responses.parse_json_response(response, source="cftc", url=url)

    if not isinstance(data, list):
        raise ParseError(
            source="cftc",
            parser_version=models.PARSER_VERSION,
            reason="Resposta Socrata deve ser uma lista de registros",
        )

    if len(data) >= MAX_ROWS:
        logger.warning("cftc_cot_limit_reached", rows=len(data), limit=MAX_ROWS)

    logger.info("cftc_cot_ok", records=len(data))
    return data, str(response.url), response.content
