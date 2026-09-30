from __future__ import annotations

import hashlib
import time
import warnings
from typing import Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.models import MetaInfo
from agrobr.utils import time as time_utils
from agrobr.utils.result import build_source_meta, finalize_result

from . import client, parser
from .models import resolve_attributes, resolve_commodity_code, resolve_country_code

logger = _log.get_logger(__name__)


@overload
async def psd(
    commodity: str,
    *,
    country: str = "BR",
    market_year: int | None = None,
    attributes: list[str] | None = None,
    pivot: bool = False,
    api_key: str | None = None,
    as_polars: bool = False,
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
    as_polars: bool = False,
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
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    commodity_code = resolve_commodity_code(commodity)
    pedidos = resolve_attributes(attributes)
    country_lower = country.strip().lower()

    async def buscar(ano: int) -> client.RespostaPSD:
        if country_lower == "world":
            return await client.fetch_psd_world(commodity_code, ano, api_key)
        if country_lower == "all":
            return await client.fetch_psd_all_countries(commodity_code, ano, api_key)
        country_code = resolve_country_code(country)
        return await client.fetch_psd_country(commodity_code, country_code, ano, api_key)

    tentados = [market_year or time_utils.utcnow().year]
    logger.info(
        "usda_psd",
        commodity=commodity,
        commodity_code=commodity_code,
        country=country,
        year=tentados[0],
    )

    t0 = time.monotonic()
    resposta = await buscar(tentados[0])
    df = parser.parse_psd_response(resposta.dados)
    if market_year is None and df.empty:
        tentados.append(tentados[0] - 1)
        resposta = await buscar(tentados[-1])
        df = parser.parse_psd_response(resposta.dados)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()

    if pedidos:
        df = parser.filter_attributes(df, pedidos)

    if pivot:
        df = parser.pivot_attributes(df)

    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "usda",
        resposta.url,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        attempted_sources=["usda_psd"],
        selected_source="usda_psd",
        raw_content_hash=hashlib.sha256(resposta.corpo).hexdigest() if resposta.corpo else None,
        raw_content_size=len(resposta.corpo),
        source_details={
            "market_year": tentados[-1],
            "market_year_tentados": tentados,
            "market_year_padrao": market_year is None,
        },
    )
    if resposta.status == 404:
        aviso = (
            f"usda: a fonte respondeu HTTP 404 para o ano {tentados[-1]}: sem dado publicado para a "
            "combinação (ou a URL da API mudou); o resultado vem vazio"
        )
        meta.validation_warnings.append(aviso)
        warnings.warn(aviso, UserWarning, stacklevel=2)
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
