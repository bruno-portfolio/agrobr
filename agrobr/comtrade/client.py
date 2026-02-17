from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status

logger = structlog.get_logger()

BASE_URL = "https://comtradeapi.un.org/data/v1/get"

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
    headers = {
        "Accept": "application/json",
        "User-Agent": "agrobr (https://github.com/bruno-portfolio/agrobr)",
    }
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
    headers = _build_headers(key)
    max_rec = _max_records(key)

    url = f"{BASE_URL}/C/{freq.upper()}/HS"

    chunks = _chunk_period(period, freq)
    all_records: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
        for chunk in chunks:
            params: dict[str, str] = {
                "reporterCode": str(reporter),
                "flowCode": flow.upper(),
                "cmdCode": ",".join(hs_codes),
                "period": chunk,
                "includeDesc": "True",
                "maxRecords": str(max_rec),
            }
            if partner != 0:
                params["partnerCode"] = str(partner)

            logger.debug("comtrade_request", url=url, chunk=chunk)

            def _make_getter(
                p: dict[str, str],
            ) -> Callable[[], Awaitable[httpx.Response]]:
                async def _do_get() -> httpx.Response:
                    return await client.get(url, headers=headers, params=p)

                return _do_get

            response = await retry_on_status(
                _make_getter(params),
                source="comtrade",
            )

            if response.status_code in (401, 403):
                raise SourceUnavailableError(
                    source="comtrade",
                    url=url,
                    last_error=(
                        f"HTTP {response.status_code}. "
                        "Verifique AGROBR_COMTRADE_API_KEY ou registre em "
                        "https://comtradeplus.un.org"
                    ),
                )

            if response.status_code == 404:
                continue

            response.raise_for_status()

            data = response.json()
            records = data.get("data", [])
            if isinstance(records, list):
                all_records.extend(records)

    return all_records, url
