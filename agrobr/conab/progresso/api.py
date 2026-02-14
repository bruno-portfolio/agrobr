from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import CULTURAS_VALIDAS, normalizar_cultura

logger = structlog.get_logger()


@overload
async def progresso_safra(
    *,
    cultura: str | None = None,
    estado: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def progresso_safra(
    *,
    cultura: str | None = None,
    estado: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def progresso_safra(
    *,
    cultura: str | None = None,
    estado: str | None = None,
    operacao: str | None = None,
    semana_url: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Busca dados de progresso semanal de safra da CONAB.

    Percentuais de plantio e colheita por cultura, estado e semana.

    Args:
        cultura: Filtrar por cultura (ex: "Soja", "Milho 2a", "algodao").
            Se None, todas as culturas.
        estado: Filtrar por UF (ex: "MT", "GO"). Se None, todos.
        operacao: Filtrar por operacao: "Semeadura" ou "Colheita".
            Se None, ambas.
        semana_url: URL de uma semana especifica. Se None, busca a mais recente.
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com colunas: cultura, safra, operacao, estado,
        semana_atual, pct_ano_anterior, pct_semana_anterior,
        pct_semana_atual, pct_media_5_anos.

    Raises:
        SourceUnavailableError: Se dados indisponiveis.
        ParseError: Se nao conseguir parsear o XLSX.
    """
    logger.info(
        "conab_progresso_safra",
        cultura=cultura,
        estado=estado,
        operacao=operacao,
    )

    t0 = time.monotonic()
    if semana_url:
        xlsx_bytes, source_url = await client.fetch_xlsx_semanal(semana_url)
        desc = semana_url
    else:
        xlsx_bytes, source_url, desc = await client.fetch_latest()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_progresso_xlsx(xlsx_bytes)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if cultura is not None:
        cultura_norm = normalizar_cultura(cultura)
        if cultura_norm in CULTURAS_VALIDAS:
            df = df[df["cultura"] == cultura_norm].reset_index(drop=True)
        else:
            df = df[df["cultura"].str.lower().str.contains(cultura.lower())].reset_index(drop=True)

    if estado is not None:
        estado_upper = estado.strip().upper()
        df = df[df["estado"].str.upper() == estado_upper].reset_index(drop=True)

    if operacao is not None:
        op_title = operacao.strip().title()
        df = df[df["operacao"] == op_title].reset_index(drop=True)

    if return_meta:
        meta = MetaInfo(
            source="conab_progresso",
            source_url=source_url,
            source_method="httpx+xlsx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["conab_govbr"],
            selected_source="conab_govbr",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


async def semanas_disponiveis(max_pages: int = 4) -> list[dict[str, str]]:
    """Lista semanas disponiveis no portal CONAB Progresso de Safra.

    Args:
        max_pages: Maximo de paginas a buscar (default 4 = ~80 semanas).

    Returns:
        Lista de dicts com 'descricao' e 'url' para cada semana.
    """
    weeks = await client.list_semanas(max_pages=max_pages)
    return [{"descricao": desc, "url": url} for desc, url in weeks]
