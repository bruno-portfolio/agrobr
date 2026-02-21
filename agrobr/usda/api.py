from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import resolve_commodity_code, resolve_country_code

logger = structlog.get_logger()


@overload
async def psd(
    commodity: str,
    *,
    country: str = "BR",
    market_year: int | None = None,
    attributes: list[str] | None = None,
    pivot: bool = False,
    api_key: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def psd(
    commodity: str,
    *,
    country: str = "BR",
    market_year: int | None = None,
    attributes: list[str] | None = None,
    pivot: bool = False,
    api_key: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def psd(
    commodity: str,
    *,
    country: str = "BR",
    market_year: int | None = None,
    attributes: list[str] | None = None,
    pivot: bool = False,
    api_key: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    commodity_code = resolve_commodity_code(commodity)
    year = market_year or datetime.now(UTC).year

    logger.info(
        "usda_psd",
        commodity=commodity,
        commodity_code=commodity_code,
        country=country,
        year=year,
    )

    t0 = time.monotonic()
    source_url = f"{client.BASE_URL}/psd/commodity/{commodity_code}"

    country_lower = country.strip().lower()
    if country_lower == "world":
        records = await client.fetch_psd_world(commodity_code, year, api_key)
    elif country_lower == "all":
        records = await client.fetch_psd_all_countries(commodity_code, year, api_key)
    else:
        country_code = resolve_country_code(country)
        records = await client.fetch_psd_country(commodity_code, country_code, year, api_key)

    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_psd_response(records)

    if attributes:
        df = parser.filter_attributes(df, attributes)

    if pivot:
        df = parser.pivot_attributes(df)

    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = MetaInfo(
            source="usda",
            source_url=source_url,
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["usda_psd"],
            selected_source="usda_psd",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
