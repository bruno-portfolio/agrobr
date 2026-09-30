from __future__ import annotations

import time
import warnings
from datetime import UTC, datetime
from typing import Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.contracts import bcb_sgs as source_contracts
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result

from . import sgs_client, sgs_metadata, sgs_parser, sgs_query

logger = _log.get_logger(__name__)


@overload
async def sgs(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def sgs(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def sgs(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    selection = sgs_query.build_query(
        codigo,
        data_inicial=data_inicial,
        data_final=data_final,
        ultimos=ultimos,
        reference_date=datetime.now(UTC).date(),
    )
    logger.info("bcb_sgs_request", query=selection.model_dump(mode="json"))
    started = time.monotonic()
    acquired = await sgs_client.fetch_sgs_acquisition(selection)
    fetch_ms = int((time.monotonic() - started) * 1000)
    for message in acquired.warnings:
        warnings.warn(message, UserWarning, stacklevel=2)
    started = time.monotonic()
    frame = sgs_parser.build_frame(acquired.records, selection.codigo, selection.nome_serie)
    contracts.validate_dataset(frame, source_contracts.BCB_SGS_V2)
    parse_ms = int((time.monotonic() - started) * 1000)
    meta = sgs_metadata.build_meta(acquired, frame, fetch_ms=fetch_ms, parse_ms=parse_ms)
    return result.finalize_result(
        frame, meta, as_polars=as_polars, return_meta=return_meta, string_columns=("nome_serie",)
    )
