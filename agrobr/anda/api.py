from __future__ import annotations

import time
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.warnings import warn_once

from . import client, models, parser

logger = structlog.get_logger()


@overload
async def entregas(
    ano: int,
    *,
    uf: str | None = None,
    produto: str = "total",
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def entregas(
    ano: int,
    *,
    uf: str | None = None,
    produto: str = "total",
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def entregas(
    ano: int,
    *,
    uf: str | None = None,
    produto: str = "total",
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    produto_normalizado = models.resolve_produto(produto)

    warn_once(
        "anda",
        "ANDA: termos de uso não encontrados publicamente. "
        "Autorização solicitada em fev/2026. Classificação: zona_cinza. "
        "Veja docs/licenses.md para detalhes.",
    )

    logger.info(
        "anda_entregas",
        ano=ano,
        uf=uf,
        produto=produto_normalizado,
        agregacao=agregacao,
    )

    t0 = time.monotonic()
    pdf_bytes, ano_real = await client.fetch_entregas_pdf(ano)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_entregas_pdf(pdf_bytes, ano=ano_real)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if uf:
        uf_upper = uf.upper().strip()
        df = df[df["uf"] == uf_upper].reset_index(drop=True)

    if agregacao == "mensal":
        df = parser.agregar_mensal(df)

    meta = build_source_meta(
        "anda",
        client.ESTATISTICAS_URL,
        "httpx+pdfplumber",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
