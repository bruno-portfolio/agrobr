from __future__ import annotations

import time
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.warnings import warn_once

from . import client, parser
from .models import SAFRAS_URLS

logger = structlog.get_logger()


@overload
async def ensaio_soja(
    safra: str,
    *,
    cultivar: str | None = None,
    empresa: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ensaio_soja(
    safra: str,
    *,
    cultivar: str | None = ...,
    empresa: str | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def ensaio_soja(
    safra: str,
    *,
    cultivar: str | None = None,
    empresa: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    warn_once(
        "rio_verde",
        "Fundação Rio Verde: termos de uso não encontrados. "
        "Classificação: zona_cinza. Veja docs/licenses.md.",
    )
    logger.info("rio_verde_ensaio_soja", safra=safra, cultivar=cultivar, empresa=empresa)

    t0 = time.monotonic()
    raw, source_url = await client.fetch_ensaio_soja(safra)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_ensaio_soja(raw, safra)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if cultivar is not None:
        df = df[df["cultivar"].str.contains(cultivar, case=False, na=False, regex=False)]
    if empresa is not None:
        df = df[df["empresa"].str.contains(empresa, case=False, na=False, regex=False)]

    df = df.reset_index(drop=True)

    meta = build_source_meta(
        "rio_verde",
        source_url,
        "httpx+pdf",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def safras_disponiveis() -> list[str]:
    return list(SAFRAS_URLS.keys())
