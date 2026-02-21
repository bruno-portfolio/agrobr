from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import structlog

from agrobr.constants import URLS, Fonte, HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BASE_URL_AUTH = URLS[Fonte.COMTRADE]["auth"]
BASE_URL_GUEST = URLS[Fonte.COMTRADE]["guest"]

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=120.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

_MAX_PERIOD_ITEMS = 12


def _get_api_key(api_key: str | None = None) -> str | None:
    key = api_key or os.environ.get("AGROBR_COMTRADE_API_KEY", "")
    return key if key else None


def _build_headers(api_key: str | None) -> dict[str, str]:
    headers = UserAgentRotator.get_bot_headers()
    if api_key:
        headers["Ocp-Apim-Subscription-Key"] = api_key
    return headers


def _max_records(api_key: str | None) -> int:
    return 100_000 if api_key else 500


def _chunk_period(period: str, freq: str) -> list[str]:
    period = str(period).strip()

    if "-" not in period:
        return [period]

    parts = period.split("-")
    if len(parts) != 2:
        return [period]

    start_str, end_str = parts[0].strip(), parts[1].strip()

    if not start_str.isdigit() or not end_str.isdigit():
        return [period]

    start_year = int(start_str)
    end_year = int(end_str)

    if start_year > end_year:
        return [period]

    if freq.upper() == "M":
        all_periods: list[str] = []
        for y in range(start_year, end_year + 1):
            for m in range(1, 13):
                all_periods.append(f"{y}{m:02d}")

        chunks: list[str] = []
        for i in range(0, len(all_periods), _MAX_PERIOD_ITEMS):
            chunk = all_periods[i : i + _MAX_PERIOD_ITEMS]
            chunks.append(",".join(chunk))
        return chunks

    all_years = [str(y) for y in range(start_year, end_year + 1)]
    chunks = []
    for i in range(0, len(all_years), _MAX_PERIOD_ITEMS):
        chunk = all_years[i : i + _MAX_PERIOD_ITEMS]
        chunks.append(",".join(chunk))
    return chunks


async def _fetch_chunks(
    http: httpx.AsyncClient,
    url: str,
    headers: dict[str, str],
    params_base: dict[str, str],
    chunks: list[str],
) -> list[dict[str, Any]] | None:
    records: list[dict[str, Any]] = []
    for chunk in chunks:
        params = {**params_base, "period": chunk}

        logger.debug("comtrade_request", url=url, chunk=chunk)

        def _make_getter(
            p: dict[str, str],
        ) -> Callable[[], Awaitable[httpx.Response]]:
            async def _do_get() -> httpx.Response:
                return await http.get(url, headers=headers, params=p)

            return _do_get

        response = await retry_on_status(
            _make_getter(params),
            source="comtrade",
        )

        if response.status_code in (401, 403):
            return None

        if response.status_code == 404:
            continue

        response.raise_for_status()

        data = response.json()
        if not isinstance(data, dict):
            logger.warning(
                "comtrade_unexpected_response_type",
                url=url,
                chunk=chunk,
                type=type(data).__name__,
            )
            continue

        recs = data.get("data", [])
        if isinstance(recs, list):
            records.extend(recs)

    return records


async def fetch_trade_data(
    *,
    reporter: int,
    partner: int,
    hs_codes: list[str],
    flow: str,
    period: str,
    freq: str = "A",
    api_key: str | None = None,
) -> tuple[list[dict[str, Any]], str]:
    key = _get_api_key(api_key)
    chunks = _chunk_period(period, freq)

    params_base: dict[str, str] = {
        "reporterCode": str(reporter),
        "flowCode": flow.upper(),
        "cmdCode": ",".join(hs_codes),
        "includeDesc": "True",
    }
    if partner != 0:
        params_base["partnerCode"] = str(partner)

    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as http:
        if key:
            url = f"{BASE_URL_AUTH}/C/{freq.upper()}/HS"
            headers = _build_headers(key)
            params_base["maxRecords"] = str(_max_records(key))

            result = await _fetch_chunks(http, url, headers, params_base, chunks)
            if result is not None:
                return result, url

            logger.warning("comtrade_auth_failed_fallback_guest", url=url)

        url = f"{BASE_URL_GUEST}/C/{freq.upper()}/HS"
        headers = _build_headers(None)
        params_base["maxRecords"] = str(_max_records(None))

        result = await _fetch_chunks(http, url, headers, params_base, chunks)
        if result is not None:
            return result, url

        raise SourceUnavailableError(
            source="comtrade",
            url=url,
            last_error=(
                "HTTP 401/403 em ambos endpoints (auth + guest). "
                "Verifique AGROBR_COMTRADE_API_KEY ou registre em "
                "https://comtradeplus.un.org"
            ),
        )
