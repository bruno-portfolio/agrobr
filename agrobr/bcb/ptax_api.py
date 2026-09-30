from __future__ import annotations

import time
import warnings
from datetime import date, datetime
from typing import Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.contracts import bcb_ptax
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result
from agrobr.utils import time as time_utils

from . import ptax_client, ptax_metadata, ptax_parser, ptax_query

logger = _log.get_logger(__name__)


def _validate_flags(as_polars: bool, return_meta: bool) -> None:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")


@overload
async def ptax(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ptax(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> result.DataFrame: ...


@overload
async def ptax(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def ptax(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[result.DataFrame, MetaInfo]: ...


@overload
async def ptax(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def ptax(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult:
    _validate_flags(as_polars, return_meta)
    query = ptax_query.build_query(
        data=data,
        data_inicial=inicio,
        data_final=fim,
        moeda=moeda,
        boletim=boletim,
        top=top,
        reference_date=time_utils.hoje(),
    )
    logger.info("bcb_ptax_selection", query=query.model_dump(mode="json"))
    started = time.monotonic()
    acquired = await ptax_client.fetch_ptax_acquisition(query)
    fetch_ms = int((time.monotonic() - started) * 1000)
    for warning in acquired.warnings:
        warnings.warn(warning, UserWarning, stacklevel=2)
    started = time.monotonic()
    catalog_frame = ptax_parser.build_currencies_frame(acquired.catalog.records)
    contracts.validate_dataset(catalog_frame, bcb_ptax.BCB_PTAX_MOEDAS_V1)
    frame = ptax_parser.build_quotes_frame(acquired.records)
    contracts.validate_dataset(frame, bcb_ptax.BCB_PTAX_V2)
    parse_ms = int((time.monotonic() - started) * 1000)
    meta = ptax_metadata.build_quote_meta(
        acquired, frame, catalog_frame, fetch_ms=fetch_ms, parse_ms=parse_ms
    )
    return result.finalize_result(
        frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=("moeda", "tipo_boletim"),
    )


@overload
async def ptax_moedas(
    *,
    top: int = 1000,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ptax_moedas(
    *,
    top: int = 1000,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> result.DataFrame: ...


@overload
async def ptax_moedas(
    *,
    top: int = 1000,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def ptax_moedas(
    *,
    top: int = 1000,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[result.DataFrame, MetaInfo]: ...


@overload
async def ptax_moedas(
    *,
    top: int = 1000,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def ptax_moedas(
    *,
    top: int = 1000,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult:
    _validate_flags(as_polars, return_meta)
    query = ptax_query.build_catalog_query(top=top)
    logger.info("bcb_ptax_catalog_selection", query=query.model_dump(mode="json"))
    started = time.monotonic()
    acquired = await ptax_client.fetch_currencies_acquisition(query)
    fetch_ms = int((time.monotonic() - started) * 1000)
    for warning in acquired.warnings:
        warnings.warn(warning, UserWarning, stacklevel=2)
    started = time.monotonic()
    frame = ptax_parser.build_currencies_frame(acquired.records)
    contracts.validate_dataset(frame, bcb_ptax.BCB_PTAX_MOEDAS_V1)
    parse_ms = int((time.monotonic() - started) * 1000)
    meta = ptax_metadata.build_catalog_meta(acquired, frame, fetch_ms=fetch_ms, parse_ms=parse_ms)
    return result.finalize_result(
        frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=("moeda", "nome", "tipo_moeda"),
    )
