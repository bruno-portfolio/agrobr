"""Cliente HTTP para API NASA POWER (power.larc.nasa.gov)."""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Any

import httpx
import structlog

from agrobr.constants import RETRIABLE_STATUS_CODES, HTTPSettings

logger = structlog.get_logger()

BASE_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)

# NASA POWER pede para respeitar rate limits.
RATE_LIMIT_DELAY = 1.0

# Maximo de dias por request (API aceita ate ~366, usamos 365 por seguranca).
MAX_DAYS_PER_REQUEST = 365

HEADERS = {
    "User-Agent": "agrobr/0.7.1 (https://github.com/bruno-portfolio/agrobr)",
    "Accept": "application/json",
}


async def _get_json(params: dict[str, Any]) -> dict[str, Any]:
    """Faz GET na API NASA POWER e retorna JSON parseado."""
    settings = HTTPSettings()
    last_response: httpx.Response | None = None

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
        for attempt in range(settings.max_retries):
            response = await client.get(BASE_URL, params=params)
            if response.status_code != 429:
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, dict):
                    return {}
                return data
            last_response = response
            if attempt < settings.max_retries - 1:
                delay = settings.retry_base_delay * (settings.retry_exponential_base**attempt)
                logger.warning(
                    "nasa_power_retry",
                    attempt=attempt + 1,
                    status=response.status_code,
                    delay=delay,
                )
                await asyncio.sleep(delay)

    logger.warning(
        "nasa_power_retry_exhausted", status=last_response.status_code if last_response else None
    )
    return {}


async def fetch_daily(
    lat: float,
    lon: float,
    start: date,
    end: date,
    parameters: list[str] | None = None,
) -> dict[str, Any]:
    """Busca dados diarios de um ponto na API NASA POWER.

    Divide automaticamente em chunks de ate 365 dias para respeitar
    limites praticos da API.

    Args:
        lat: Latitude (-90 a 90).
        lon: Longitude (-180 a 180).
        start: Data inicial.
        end: Data final.
        parameters: Lista de parametros NASA POWER. Se None, usa PARAMS_AG.

    Returns:
        Dict com estrutura NASA POWER (properties.parameter contendo os dados).
    """
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

    # Se range cabe em um unico request, faz direto.
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

    # Chunking para ranges maiores.
    merged: dict[str, Any] = {}
    chunk_start = start

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
            chunk_data = await _get_json(params)

            # Merge properties.parameter de cada chunk.
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

        except httpx.HTTPStatusError as e:
            if e.response.status_code in RETRIABLE_STATUS_CODES:
                logger.warning(
                    "nasa_power_chunk_retriable",
                    status=e.response.status_code,
                    chunk_start=str(chunk_start),
                )
            else:
                raise

        chunk_start = chunk_end + timedelta(days=1)

        # Respeitar rate limit entre chunks.
        if chunk_start <= end:
            await asyncio.sleep(RATE_LIMIT_DELAY)

    return merged
