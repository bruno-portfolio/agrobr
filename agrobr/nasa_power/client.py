from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import httpx
import structlog

from agrobr.constants import URLS, Fonte
from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()

BASE_URL = URLS[Fonte.NASA_POWER]["daily"]

TIMEOUT = get_timeout(read=60.0)

MAX_DAYS_PER_REQUEST = 365


async def _get_json(
    params: dict[str, Any],
    *,
    http: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    async def _do_request(c: httpx.AsyncClient) -> dict[str, Any]:
        response = await retry_on_status(
            lambda: c.get(BASE_URL, params=params),
            source="nasa_power",
        )
        response.raise_for_status()
        data = responses.parse_json_response(response, source="nasa_power", url=BASE_URL)
        if not isinstance(data, dict):
            return {}
        return data

    if http is not None:
        return await _do_request(http)

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as c:
        return await _do_request(c)


async def fetch_daily(
    lat: float,
    lon: float,
    start: date,
    end: date,
    parameters: list[str] | None = None,
) -> dict[str, Any]:
    from agrobr.nasa_power.models import PARAMS_AG

    if parameters is None:
        parameters = PARAMS_AG

    if start > end:
        raise ValueError(f"start ({start}) deve ser <= end ({end})")

    logger.info(
        "nasa_power_fetch",
        lat=lat,
        lon=lon,
        start=str(start),
        end=str(end),
        params=len(parameters),
    )

    total_days = (end - start).days
    if total_days <= MAX_DAYS_PER_REQUEST:
        params = {
            "parameters": ",".join(parameters),
            "community": "AG",
            "longitude": lon,
            "latitude": lat,
            "start": start.strftime("%Y%m%d"),
            "end": end.strftime("%Y%m%d"),
            "format": "JSON",
        }
        return await _get_json(params)

    merged: dict[str, Any] = {}
    chunk_start = start

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as http:
        while chunk_start <= end:
            chunk_end = min(chunk_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1), end)

            params = {
                "parameters": ",".join(parameters),
                "community": "AG",
                "longitude": lon,
                "latitude": lat,
                "start": chunk_start.strftime("%Y%m%d"),
                "end": chunk_end.strftime("%Y%m%d"),
                "format": "JSON",
            }

            try:
                chunk_data = await _get_json(params, http=http)

                chunk_params = chunk_data.get("properties", {}).get("parameter", {})
                if not merged:
                    merged = chunk_data
                else:
                    existing = merged.get("properties", {}).get("parameter", {})
                    for param_name, daily_values in chunk_params.items():
                        if param_name in existing:
                            existing[param_name].update(daily_values)
                        else:
                            existing[param_name] = daily_values

                logger.debug(
                    "nasa_power_chunk_ok",
                    chunk_start=str(chunk_start),
                    chunk_end=str(chunk_end),
                )

            except SourceUnavailableError as e:
                logger.warning(
                    "nasa_power_chunk_unavailable",
                    error=str(e),
                    chunk_start=str(chunk_start),
                )

            chunk_start = chunk_end + timedelta(days=1)

    return merged
