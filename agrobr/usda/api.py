"""API pública do módulo USDA — Production, Supply, Distribution (PSD).

Estimativas internacionais de produção, oferta e demanda agrícola.
Fonte: USDA FAS OpenData API v2.

Requer API key gratuita: https://api.data.gov/signup/
Configuração: variável de ambiente AGROBR_USDA_API_KEY
"""

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
    """Busca dados PSD (Production, Supply, Distribution) do USDA.

    Args:
        commodity: Commodity ("soja", "milho", "trigo") ou código USDA.
        country: País ("BR", "brasil", "US"). Default: Brasil.
                 Use "world" para dados mundiais agregados,
                 "all" para todos os países.
        market_year: Marketing year (ex: 2024). None usa o mais recente.
        attributes: Filtrar por atributos (ex: ["Production", "Exports"]).
        pivot: Se True, pivota atributos como colunas.
        api_key: USDA API key (ou usa AGROBR_USDA_API_KEY).
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com dados PSD.

    Raises:
        SourceUnavailableError: Se API USDA indisponível ou key inválida.
        ValueError: Se commodity ou país desconhecido.

    Example:
        >>> df = await usda.psd("soja", country="BR", market_year=2024)
        >>> df.columns.tolist()
        ['commodity_code', 'commodity', 'country_code', 'country',
         'market_year', 'attribute', 'attribute_br', 'value', 'unit']
    """
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

    # Filtrar atributos
    if attributes:
        df = parser.filter_attributes(df, attributes)

    # Pivotar se solicitado
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
