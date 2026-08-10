from __future__ import annotations

import time
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.warnings import warn_once

from . import client, parser
from .models import resolve_produto

logger = structlog.get_logger()


@overload
async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    warn_once(
        "abiove",
        "ABIOVE: termos de uso não encontrados publicamente. "
        "Autorização solicitada em fev/2026. Classificação: zona_cinza. "
        "Veja docs/licenses.md para detalhes.",
    )

    logger.info(
        "abiove_exportacao",
        ano=ano,
        mes=mes,
        produto=produto,
        agregacao=agregacao,
    )

    t0 = time.monotonic()
    excel_bytes, source_url = await client.fetch_exportacao_excel(ano, mes)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_exportacao_excel(excel_bytes, ano=ano)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if mes is not None:
        df = df[df["mes"] == mes].reset_index(drop=True)

    if produto:
        produto_norm = resolve_produto(produto)
        df = df[df["produto"] == produto_norm].reset_index(drop=True)

    if agregacao == "mensal":
        df = parser.agregar_mensal(df)

    meta = build_source_meta(
        "abiove",
        source_url,
        "httpx+openpyxl",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
