from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx
import structlog

from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

from .models import (
    ALERT_DATE_RANGE_QUERY,
    ALERTS_QUERY,
    GRAPHQL_URL,
    LAST_PUBLICATION_QUERY,
)

logger = structlog.get_logger()

TIMEOUT = get_timeout(read=60.0)

_THROTTLE_AFTER_PAGE = 5
_THROTTLE_DELAY = 3.0


def _get_token(token: str | None = None) -> str:
    if token:
        return token
    env_token = os.environ.get("AGROBR_MAPBIOMAS_ALERTA_TOKEN")
    if not env_token:
        raise SourceUnavailableError(
            source="mapbiomas_alerta",
            url=GRAPHQL_URL,
            last_error="Token nao encontrado. Defina AGROBR_MAPBIOMAS_ALERTA_TOKEN ou passe token=",
        )
    return env_token


async def _graphql_request(
    query: str,
    variables: dict[str, Any],
    *,
    token: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    headers = {**UserAgentRotator.get_bot_headers()}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    payload = {"query": query, "variables": variables}

    async def _do(http: httpx.AsyncClient) -> dict[str, Any]:
        response = await retry_on_status(
            lambda: http.post(GRAPHQL_URL, json=payload, headers=headers),
            source="mapbiomas_alerta",
        )
        response.raise_for_status()
        data = responses.parse_json_response(
            response,
            source="mapbiomas_alerta",
            url=GRAPHQL_URL,
        )
        if "errors" in data:
            errors = data["errors"]
            msg = errors[0].get("message", str(errors)) if errors else "Unknown GraphQL error"
            raise SourceUnavailableError(
                source="mapbiomas_alerta",
                url=GRAPHQL_URL,
                last_error=f"GraphQL error: {msg}",
            )
        result: dict[str, Any] = data.get("data", {})
        return result

    if client is not None:
        return await _do(client)
    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as http:
        return await _do(http)


async def fetch_alertas(
    *,
    token: str,
    start_date: str | None = None,
    end_date: str | None = None,
    sources: list[str] | None = None,
    bounding_box: list[float] | None = None,
    limit: int = 100,
    max_pages: int = 50,
) -> tuple[list[dict[str, Any]], str]:
    records: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as http:
        for page_num in range(1, max_pages + 1):
            variables: dict[str, Any] = {"limit": limit, "page": page_num}
            if start_date:
                variables["startDate"] = start_date
            if end_date:
                variables["endDate"] = end_date
            if sources:
                variables["sources"] = sources
            if bounding_box:
                variables["boundingBox"] = bounding_box

            if page_num > _THROTTLE_AFTER_PAGE:
                await asyncio.sleep(_THROTTLE_DELAY)

            data = await _graphql_request(
                ALERTS_QUERY,
                variables,
                token=token,
                client=http,
            )
            alerts_data = data.get("alerts", {})
            collection = alerts_data.get("collection", [])
            if not collection:
                break
            records.extend(collection)

            metadata = alerts_data.get("metadata", {})
            total_pages = metadata.get("totalPages", 1)
            logger.debug(
                "mapbiomas_alerta_page",
                page=page_num,
                total_pages=total_pages,
                records=len(collection),
            )
            if page_num >= total_pages:
                break

    return records, GRAPHQL_URL


async def fetch_alert_date_range() -> tuple[dict[str, Any], str]:
    data = await _graphql_request(ALERT_DATE_RANGE_QUERY, {})
    result: dict[str, Any] = data.get("alertDateRange", {})
    return result, GRAPHQL_URL


async def fetch_last_publication() -> tuple[dict[str, Any], str]:
    data = await _graphql_request(LAST_PUBLICATION_QUERY, {})
    result: dict[str, Any] = data.get("lastAlertPublication", {})
    return result, GRAPHQL_URL
