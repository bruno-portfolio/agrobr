"""API pública do módulo DERAL — condição das lavouras Paraná.

Dados do Departamento de Economia Rural (SEAB/PR).
Condição semanal das lavouras, progresso de plantio e colheita.

Fonte: SEAB/DERAL (agricultura.pr.gov.br/deral), sem autenticação.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser

logger = structlog.get_logger()


@overload
async def condicao_lavouras(
    produto: str | None = None,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def condicao_lavouras(
    produto: str | None = None,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def condicao_lavouras(
    produto: str | None = None,
    *,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Busca condição das lavouras do Paraná (DERAL).

    Dados semanais de condição (boa/média/ruim), progresso de
    plantio e colheita para as culturas monitoradas pelo DERAL.

    Args:
        produto: Filtrar por produto (\"soja\", \"milho\", \"trigo\").
                 None retorna todos os produtos.
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame: produto, data, condicao, pct, plantio_pct, colheita_pct.

    Raises:
        SourceUnavailableError: Se site DERAL indisponível.

    Example:
        >>> df = await deral.condicao_lavouras("soja")
        >>> df.columns.tolist()
        ['produto', 'data', 'condicao', 'pct', 'plantio_pct', 'colheita_pct']
    """
    logger.info("deral_condicao_lavouras", produto=produto)

    t0 = time.monotonic()
    data = await client.fetch_pc_xls()
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_pc_xls(data)

    if produto:
        df = parser.filter_by_produto(df, produto)

    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = MetaInfo(
            source="deral",
            source_url=f"{client.BASE_URL}/PC.xls",
            source_method="httpx+openpyxl",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["deral"],
            selected_source="deral",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
