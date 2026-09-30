from __future__ import annotations

import hashlib
import time
from typing import Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result

from . import client, parser

logger = _log.get_logger(__name__)


@overload
async def condicao_lavouras(
    produto: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def condicao_lavouras(
    produto: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def condicao_lavouras(
    produto: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    logger.info("deral_condicao_lavouras", produto=produto)

    t0 = time.monotonic()
    data = await client.fetch_pc_xls()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df, engine = parser.parse_pc_xls_with_engine(data)

    if produto:
        df = parser.filter_by_produto(df, produto)

    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "deral",
        f"{client.BASE_URL}/PC.xls",
        f"httpx+{engine}",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        raw_content_hash=hashlib.sha256(data).hexdigest(),
        raw_content_size=len(data),
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
