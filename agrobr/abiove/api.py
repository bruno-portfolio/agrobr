from __future__ import annotations

import time
import warnings
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import normalize_produto

logger = structlog.get_logger()

_WARNED = False


@overload
async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    global _WARNED  # noqa: PLW0603
    if not _WARNED:
        warnings.warn(
            "ABIOVE: termos de uso não encontrados publicamente. "
            "Autorização solicitada em fev/2026. Classificação: zona_cinza. "
            "Veja docs/licenses.md para detalhes.",
            UserWarning,
            stacklevel=2,
        )
        _WARNED = True

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
        produto_norm = normalize_produto(produto)
        df = df[df["produto"] == produto_norm].reset_index(drop=True)

    if agregacao == "mensal":
        df = parser.agregar_mensal(df)

    if return_meta:
        meta = MetaInfo(
            source="abiove",
            source_url=source_url,
            source_method="httpx+openpyxl",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["abiove"],
            selected_source="abiove",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
