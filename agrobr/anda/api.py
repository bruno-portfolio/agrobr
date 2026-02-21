from __future__ import annotations

import time
import warnings
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser

logger = structlog.get_logger()

_WARNED = False


@overload
async def entregas(
    ano: int,
    *,
    uf: str | None = None,
    produto: str = "total",
    agregacao: str = "detalhado",
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def entregas(
    ano: int,
    *,
    uf: str | None = None,
    produto: str = "total",
    agregacao: str = "detalhado",
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def entregas(
    ano: int,
    *,
    uf: str | None = None,
    produto: str = "total",
    agregacao: str = "detalhado",
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    global _WARNED  # noqa: PLW0603
    if not _WARNED:
        warnings.warn(
            "ANDA: termos de uso não encontrados publicamente. "
            "Autorização solicitada em fev/2026. Classificação: zona_cinza. "
            "Veja docs/licenses.md para detalhes.",
            UserWarning,
            stacklevel=2,
        )
        _WARNED = True

    logger.info("anda_entregas", ano=ano, uf=uf, produto=produto, agregacao=agregacao)

    t0 = time.monotonic()
    pdf_bytes, ano_real = await client.fetch_entregas_pdf(ano)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_entregas_pdf(pdf_bytes, ano=ano_real, produto=produto)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if uf:
        uf_upper = uf.upper().strip()
        df = df[df["uf"] == uf_upper].reset_index(drop=True)

    if agregacao == "mensal":
        df = parser.agregar_mensal(df)

    if return_meta:
        meta = MetaInfo(
            source="anda",
            source_url=client.ESTATISTICAS_URL,
            source_method="httpx+pdfplumber",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["anda"],
            selected_source="anda",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
