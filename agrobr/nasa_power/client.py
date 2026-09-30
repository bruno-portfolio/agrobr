from __future__ import annotations

import copy
import hashlib
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from agrobr import _log
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.http.responses import parse_json_response
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.nasa_power import models, parser, provenance

logger = _log.get_logger(__name__)


BASE_URL = URLS[Fonte.NASA_POWER]["daily"]


TIMEOUT = get_timeout(read=60.0)


MAX_DAYS_PER_REQUEST = 365


async def _get_json(
    params: dict[str, Any],
    *,
    http: httpx.AsyncClient | None = None,
) -> dict[str, Any]:

    async def _do_request(c: httpx.AsyncClient) -> dict[str, Any]:
        receipts: list[provenance.Receipt] = []
        response = await retry_on_status(lambda: _request(c, params, receipts), source="nasa_power")

        if response.status_code >= 400:
            raise SourceUnavailableError(
                source="nasa_power",
                url=BASE_URL,
                last_error=f"HTTP {response.status_code}",
            )

        data = parse_json_response(
            response, source="nasa_power", url=BASE_URL, object_pairs_hook=_unique_object
        )

        if not isinstance(data, dict):
            raise ParseError(
                source="nasa_power",
                reason="Resposta JSON deve ser objeto",
                parser_version=parser.PARSER_VERSION,
            )

        return provenance.FetchResult(data, tuple(receipts))

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

    parameters = models.validate_parameters(parameters)

    if start > end:
        raise InvalidParameterError(f"start ({start}) deve ser <= end ({end})")

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
            "time-standard": "LST",
        }

        result = await _get_json(params)
        parser.validate_period(result, start, end)
        return provenance.FetchResult(
            result, getattr(result, "receipts", ()), (_bloco(result, start, end),)
        )

    merged: dict[str, Any] = {}
    receipts: list[provenance.Receipt] = []
    blocos: list[provenance.Bloco] = []

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
                "time-standard": "LST",
            }

            chunk_data = await _get_json(params, http=http)

            parser.validate_period(chunk_data, chunk_start, chunk_end)
            if isinstance(chunk_data, provenance.FetchResult):
                receipts.extend(chunk_data.receipts)
            blocos.append(_bloco(chunk_data, chunk_start, chunk_end))

            chunk_params = chunk_data["properties"]["parameter"]

            if not merged:
                merged = copy.deepcopy(dict(chunk_data))

            else:
                previous = parser.validate_response(merged)
                current = parser.validate_response(chunk_data)
                if (
                    previous.properties.parameter.keys() != current.properties.parameter.keys()
                    or previous.header.model_dump(exclude={"sources"})
                    != current.header.model_dump(exclude={"sources"})
                    or previous.parameters != current.parameters
                ):
                    raise ParseError(
                        source="nasa_power",
                        reason="Parâmetros, unidades ou base de tempo divergentes entre blocos",
                        parser_version=parser.PARSER_VERSION,
                    )
                existing = merged.get("properties", {}).get("parameter", {})

                for param_name, daily_values in chunk_params.items():
                    existing[param_name].update(daily_values)

            logger.debug(
                "nasa_power_chunk_ok",
                chunk_start=str(chunk_start),
                chunk_end=str(chunk_end),
            )

            chunk_start = chunk_end + timedelta(days=1)

    return provenance.FetchResult(merged, tuple(receipts), tuple(blocos))


def _bloco(data: dict[str, Any], start: date, end: date) -> provenance.Bloco:
    return provenance.Bloco(start, end, tuple(data.get("header", {}).get("sources") or ()))


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ParseError(
                source="nasa_power",
                reason=f"Chave JSON duplicada: {key}",
                parser_version=parser.PARSER_VERSION,
            )
        result[key] = value
    return result


async def _request(
    http: httpx.AsyncClient, params: dict[str, Any], receipts: list[provenance.Receipt]
) -> httpx.Response:
    request_url = str(httpx.Request("GET", BASE_URL, params=params).url)
    try:
        response = await http.get(BASE_URL, params=params)
    except httpx.HTTPError as exc:
        receipts.append(
            provenance.Receipt(
                request_url=request_url, acquired_at=datetime.now(UTC), error=type(exc).__name__
            )
        )
        raise
    receipts.append(
        provenance.Receipt(
            request_url=request_url,
            effective_url=str(response.url),
            acquired_at=datetime.now(UTC),
            status=response.status_code,
            sha256=hashlib.sha256(response.content).hexdigest(),
            size_bytes=len(response.content),
        )
    )
    return response
