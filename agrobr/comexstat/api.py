from __future__ import annotations

import time
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.time import utcnow
from agrobr.utils.validation import validate_uf

from . import client
from .models import resolve_ncm
from .parser import PARSER_VERSION, agregar_mensal, parse_exportacao, parse_importacao

logger = structlog.get_logger()

_PARSE_FN = {"exportacao": parse_exportacao, "importacao": parse_importacao}
_CSV_PREFIX = {"exportacao": "EXP", "importacao": "IMP"}


async def _fetch_comexstat(
    fluxo: str,
    produto: str,
    ano: int | None,
    uf: str | None,
    agregacao: str,
    as_polars: bool,
    return_meta: bool,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    if ano is None:
        ano = utcnow().year - 1
        logger.info("comexstat_default_ano", ano=ano)
    if not isinstance(ano, int) or isinstance(ano, bool):
        raise InvalidParameterError("ano deve ser inteiro")
    if not 1997 <= ano <= utcnow().year:
        raise InvalidParameterError(f"ano deve estar entre 1997 e {utcnow().year}")
    uf = validate_uf(uf)
    if agregacao not in {"mensal", "detalhado"}:
        raise InvalidParameterError("agregacao deve ser 'mensal' ou 'detalhado'")

    ncm = resolve_ncm(produto)

    t0 = time.monotonic()
    logger.info(f"comexstat_{fluxo}_request", produto=produto, ncm=ncm, ano=ano, uf=uf)

    fetch_fn = getattr(client, f"fetch_{fluxo}_csv")
    csv_text: str = await fetch_fn(ano)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = _PARSE_FN[fluxo](csv_text, ncm=ncm, uf=uf)

    if agregacao == "mensal":
        df = agregar_mensal(df)

    parse_ms = int((time.monotonic() - t1) * 1000)

    logger.info(f"comexstat_{fluxo}_ok", produto=produto, ncm=ncm, ano=ano, records=len(df))

    meta = build_source_meta(
        "comexstat",
        f"{client.BULK_CSV_BASE}/{_CSV_PREFIX[fluxo]}_{ano}.csv",
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _fetch_comexstat("exportacao", produto, ano, uf, agregacao, as_polars, return_meta)


@overload
async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _fetch_comexstat("importacao", produto, ano, uf, agregacao, as_polars, return_meta)
