"""API pública do módulo IMEA — cotações e indicadores Mato Grosso.

Dados do Instituto Mato-Grossense de Economia Agropecuária.
Cotações diárias, indicadores de preço, progresso de safra e
comercialização para as cadeias produtivas de MT.

Fonte: API pública IMEA (api1.imea.com.br), sem autenticação.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import resolve_cadeia_id

logger = structlog.get_logger()


@overload
async def cotacoes(
    cadeia: str = "soja",
    *,
    safra: str | None = None,
    unidade: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def cotacoes(
    cadeia: str = "soja",
    *,
    safra: str | None = None,
    unidade: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def cotacoes(
    cadeia: str = "soja",
    *,
    safra: str | None = None,
    unidade: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Busca cotações e indicadores IMEA para Mato Grosso.

    Dados de preços, progresso de safra, comercialização e custos
    para as cadeias produtivas de Mato Grosso.

    Args:
        cadeia: Cadeia produtiva ("soja", "milho", "algodao", "bovinocultura").
        safra: Filtrar por safra (ex: "24/25"). None retorna todas.
        unidade: Filtrar por unidade (ex: "R$/sc", "R$/t", "%").
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame: cadeia, localidade, valor, variacao, safra,
                   unidade, unidade_descricao, data_publicacao.

    Raises:
        SourceUnavailableError: Se API IMEA indisponível.
        ValueError: Se cadeia desconhecida.

    Example:
        >>> df = await imea.cotacoes("soja")
        >>> df.columns.tolist()
        ['cadeia', 'localidade', 'valor', 'variacao', 'safra',
         'unidade', 'unidade_descricao', 'data_publicacao']
    """
    cadeia_id = resolve_cadeia_id(cadeia)

    logger.info(
        "imea_cotacoes",
        cadeia=cadeia,
        cadeia_id=cadeia_id,
        safra=safra,
        unidade=unidade,
    )

    t0 = time.monotonic()
    records = await client.fetch_cotacoes(cadeia_id)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_cotacoes(records)

    # Filtrar por safra
    if safra:
        df = parser.filter_by_safra(df, safra)

    # Filtrar por unidade
    if unidade:
        df = parser.filter_by_unidade(df, unidade)

    parse_ms = int((time.monotonic() - t1) * 1000)

    if return_meta:
        meta = MetaInfo(
            source="imea",
            source_url=f"{client.BASE_URL}/v2/mobile/cadeias/{cadeia_id}/cotacoes",
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["imea"],
            selected_source="imea",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
