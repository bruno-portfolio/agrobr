from __future__ import annotations

import hashlib
import time
from datetime import date
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr import contracts
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result

from . import client, parser
from .models import PARSER_VERSION, resolve_contract_codes

logger = structlog.get_logger()


@overload
async def cot(
    commodity: str | None = None,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    combined: bool = False,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cot(
    commodity: str | None = None,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    combined: bool = False,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def cot(
    commodity: str | None = None,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    combined: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Posicionamento semanal de traders (COT Disaggregated) em contratos agro de Chicago/NY.

    Dados do relatório Commitments of Traders do CFTC desde 2006, com as
    categorias managed money (fundos), producer/merchant (hedgers), swap
    dealers e other reportables. `combined=True` inclui opções
    (futures+options); o default cobre apenas futuros.
    """
    codes = resolve_contract_codes(commodity)

    t0 = time.monotonic()
    records, source_url, corpo = await client.fetch_cot(
        codes, start=start, end=end, combined=combined
    )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_cot(records)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "cftc",
        source_url,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
        schema_version=contracts.get_contract("posicionamento_fundos").version,
        attempted_sources=["cftc"],
        selected_source="cftc",
        raw_content_hash=hashlib.sha256(corpo).hexdigest(),
        raw_content_size=len(corpo),
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
