from __future__ import annotations

import time
import warnings
from datetime import date, datetime
from typing import Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.contracts import bcb_focus
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result

from . import focus_client, focus_metadata, focus_parser, focus_query

logger = _log.get_logger(__name__)


@overload
async def focus(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def focus(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> result.DataFrame: ...


@overload
async def focus(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def focus(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[result.DataFrame, MetaInfo]: ...


@overload
async def focus(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult: ...


async def focus(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result.DataFrameResult:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    query = focus_query.build_query(
        indicador,
        periodicidade=periodicidade,
        top=top,
        data_inicial=inicio,
        max_registros=max_registros,
    )
    logger.info("bcb_focus_selection", query=query.model_dump(mode="json"))
    started = time.monotonic()
    acquired = await focus_client.fetch_focus_acquisition(query)
    fetch_ms = int((time.monotonic() - started) * 1000)
    for warning in acquired.warnings:
        warnings.warn(warning, UserWarning, stacklevel=2)
    started = time.monotonic()
    frame = focus_parser.build_frame(acquired.records)
    contracts.validate_dataset(frame, bcb_focus.BCB_FOCUS_V2)
    parse_ms = int((time.monotonic() - started) * 1000)
    meta = focus_metadata.build_meta(acquired, frame, fetch_ms=fetch_ms, parse_ms=parse_ms)
    return result.finalize_result(
        frame,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=("indicador", "data_referencia", "periodicidade", "indicador_detalhe"),
    )
