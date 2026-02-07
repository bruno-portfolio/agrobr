"""API pública do módulo ComexStat."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client
from .models import resolve_ncm
from .parser import PARSER_VERSION, agregar_mensal, parse_exportacao

logger = structlog.get_logger()


@overload
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Obtém dados de exportação brasileira.

    Baixa CSV anual da ComexStat e filtra por produto (NCM) e UF.

    Args:
        produto: Nome do produto (soja, milho, cafe, etc).
        ano: Ano de referência (default: ano corrente).
        uf: Filtrar por UF de origem (ex: "MT", "PR").
        agregacao: "mensal" (padrão) ou "detalhado" (registro a registro).
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com colunas: ano, mes, ncm, uf, kg_liquido,
        valor_fob_usd, volume_ton (se mensal).

    Example:
        >>> df = await comexstat.exportacao("soja", ano=2024)
        >>> df, meta = await comexstat.exportacao("soja", ano=2024, return_meta=True)
    """
    if ano is None:
        ano = datetime.now().year

    ncm = resolve_ncm(produto)

    t0 = time.monotonic()

    logger.info(
        "comexstat_exportacao_request",
        produto=produto,
        ncm=ncm,
        ano=ano,
        uf=uf,
    )

    csv_text = await client.fetch_exportacao_csv(ano)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parse_exportacao(csv_text, ncm=ncm, uf=uf)

    if agregacao == "mensal":
        df = agregar_mensal(df)

    parse_ms = int((time.monotonic() - t1) * 1000)

    logger.info(
        "comexstat_exportacao_ok",
        produto=produto,
        ncm=ncm,
        ano=ano,
        records=len(df),
    )

    if return_meta:
        meta = MetaInfo(
            source="comexstat",
            source_url=f"{client.BULK_CSV_BASE}/EXP_{ano}.csv",
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["comexstat"],
            selected_source="comexstat",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
