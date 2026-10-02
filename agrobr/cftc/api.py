from __future__ import annotations

import hashlib
import time
import warnings
from datetime import date, datetime
from typing import Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrameResult, build_source_meta, finalize_result
from agrobr.utils.validation import parse_data

from . import client, parser
from .models import PARSER_VERSION, SCHEMA_VERSION, resolve_contract_codes

logger = _log.get_logger(__name__)


@overload
async def cot(
    produto: str | None = None,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    combined: bool = False,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cot(
    produto: str | None = None,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    combined: bool = False,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def cot(
    produto: str | None = None,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    combined: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def cot(
    produto: str | None = None,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    combined: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    """Posicionamento semanal de traders (COT Disaggregated) em contratos agro de Chicago/NY.

    Dados do relatório Commitments of Traders do CFTC desde 2006, com as
    categorias managed money (fundos), producer/merchant (hedgers), swap
    dealers e other reportables. `combined=True` inclui opções
    (futures+options); o default cobre apenas futuros. As colunas seguem os
    nomes do relatório; o dataset `posicionamento_fundos` as entrega em português.
    """
    codes = resolve_contract_codes(produto)
    inicio_dt, fim_dt = parse_data(inicio, "inicio"), parse_data(fim, "fim")
    if inicio_dt is not None and fim_dt is not None and inicio_dt > fim_dt:
        raise InvalidParameterError(f"inicio ({inicio_dt}) posterior a fim ({fim_dt})")

    t0 = time.monotonic()
    records, source_url, corpo = await client.fetch_cot(
        codes, start=inicio_dt, end=fim_dt, combined=combined
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
        schema_version=SCHEMA_VERSION,
        attempted_sources=["cftc"],
        selected_source="cftc",
        raw_content_hash=hashlib.sha256(corpo).hexdigest(),
        raw_content_size=len(corpo),
    )
    if len(records) >= client.MAX_ROWS:
        aviso = (
            f"A consulta CFTC atingiu o limite de {client.MAX_ROWS} registros; "
            "a completude não foi comprovada. Reduza o período solicitado."
        )
        warnings.warn(aviso, UserWarning, stacklevel=2)
        meta.validation_warnings.append(aviso)
        meta.source_details.update(row_limit=client.MAX_ROWS, completeness="unknown")
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
