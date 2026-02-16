"""API pública do módulo ANDA — entregas de fertilizantes."""

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
    """Busca entregas de fertilizantes por UF/mês.

    Dados da ANDA (Associação Nacional para Difusão de Adubos).
    Requer pdfplumber instalado (pip install agrobr[pdf]).

    Args:
        ano: Ano de referência (2010+).
        uf: Filtrar por UF (ex: "MT"). None retorna todas.
        produto: Tipo de fertilizante (default: "total").
        agregacao: "detalhado" (por UF/mês) ou "mensal" (soma por mês).
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com colunas: ano, mes, uf, produto_fertilizante, volume_ton.

    Raises:
        FileNotFoundError: Se PDF do ano não for encontrado.
        ImportError: Se pdfplumber não está instalado.
        ParseError: Se dados não puderem ser extraídos.

    Example:
        >>> df = await anda.entregas(ano=2024, uf="MT")
        >>> df.columns.tolist()
        ['ano', 'mes', 'uf', 'produto_fertilizante', 'volume_ton']
    """
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

    # Filtra por UF
    if uf:
        uf_upper = uf.upper().strip()
        df = df[df["uf"] == uf_upper].reset_index(drop=True)

    # Agrega se solicitado
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
