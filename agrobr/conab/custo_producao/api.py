"""API pública do sub-módulo custo de produção CONAB."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client
from .parser import PARSER_VERSION, items_to_dataframe, parse_planilha

logger = structlog.get_logger()


@overload
async def custo_producao(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def custo_producao(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def custo_producao(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Obtém custo de produção detalhado por hectare.

    Busca planilha Excel da CONAB e retorna DataFrame com itens
    discriminados de custo de produção por cultura e UF.

    Args:
        cultura: Nome da cultura (soja, milho, arroz, etc).
        uf: Filtrar por UF (ex: "MT", "PR"). Se None, retorna o primeiro resultado.
        safra: Safra no formato "2024/25" (default: mais recente).
        tecnologia: Nível tecnológico ("alta", "media", "baixa").
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com colunas: cultura, uf, safra, tecnologia,
        categoria, item, unidade, quantidade_ha, preco_unitario,
        valor_ha, participacao_pct.

    Example:
        >>> df = await conab.custo_producao("soja", uf="MT")
        >>> df, meta = await conab.custo_producao("soja", uf="MT", return_meta=True)
    """
    t0 = time.monotonic()

    logger.info(
        "conab_custo_producao_request",
        cultura=cultura,
        uf=uf,
        safra=safra,
        tecnologia=tecnologia,
    )

    xlsx, metadata = await client.fetch_xlsx_for_cultura(
        cultura=cultura,
        uf=uf,
        safra=safra,
    )

    resolved_uf = metadata.get("uf", uf or "BR")
    resolved_safra = metadata.get("safra", safra or "latest")

    t1 = time.monotonic()
    items, custo_total = parse_planilha(
        xlsx=xlsx,
        cultura=cultura,
        uf=resolved_uf,
        safra=resolved_safra,
        tecnologia=tecnologia,
    )
    parse_ms = int((time.monotonic() - t1) * 1000)

    df = items_to_dataframe(items)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    logger.info(
        "conab_custo_producao_ok",
        cultura=cultura,
        uf=resolved_uf,
        safra=resolved_safra,
        items=len(items),
        coe=custo_total.coe_ha if custo_total else None,
    )

    if return_meta:
        meta = MetaInfo(
            source="conab_custo",
            source_url=metadata.get("url", client.CUSTOS_PAGE),
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["conab_custo"],
            selected_source="conab_custo",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


@overload
async def custo_producao_total(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    *,
    return_meta: Literal[False] = False,
) -> dict[str, Any]: ...


@overload
async def custo_producao_total(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    *,
    return_meta: Literal[True],
) -> tuple[dict[str, Any], MetaInfo]: ...


async def custo_producao_total(
    cultura: str,
    uf: str | None = None,
    safra: str | None = None,
    tecnologia: str = "alta",
    return_meta: bool = False,
) -> dict[str, Any] | tuple[dict[str, Any], MetaInfo]:
    """Obtém agregados de custo total (COE, COT, CT) por hectare.

    Retorna o Custo Operacional Efetivo (COE), Custo Operacional Total (COT)
    e Custo Total (CT) por hectare para a cultura/UF especificada.

    O COE é o valor usado como denominador do proxy de crédito no SCI:
        area_estimada = credito_custeio / coe_ha

    Args:
        cultura: Nome da cultura (soja, milho, etc).
        uf: Filtrar por UF (ex: "MT").
        safra: Safra no formato "2024/25".
        tecnologia: Nível tecnológico ("alta", "media", "baixa").
        return_meta: Se True, retorna tupla (dict, MetaInfo).

    Returns:
        Dict com: cultura, uf, safra, tecnologia, coe_ha, cot_ha, ct_ha.

    Example:
        >>> totais = await conab.custo_producao_total("soja", uf="MT")
        >>> print(f"COE/ha: R$ {totais['coe_ha']:.2f}")
    """
    t0 = time.monotonic()

    logger.info(
        "conab_custo_total_request",
        cultura=cultura,
        uf=uf,
        safra=safra,
        tecnologia=tecnologia,
    )

    xlsx, metadata = await client.fetch_xlsx_for_cultura(
        cultura=cultura,
        uf=uf,
        safra=safra,
    )

    resolved_uf = metadata.get("uf", uf or "BR")
    resolved_safra = metadata.get("safra", safra or "latest")

    t1 = time.monotonic()
    _, custo_total = parse_planilha(
        xlsx=xlsx,
        cultura=cultura,
        uf=resolved_uf,
        safra=resolved_safra,
        tecnologia=tecnologia,
    )
    parse_ms = int((time.monotonic() - t1) * 1000)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    if custo_total is None:
        result: dict[str, Any] = {
            "cultura": cultura.lower(),
            "uf": resolved_uf,
            "safra": resolved_safra,
            "tecnologia": tecnologia.lower(),
            "coe_ha": 0.0,
            "cot_ha": None,
            "ct_ha": None,
        }
    else:
        result = custo_total.model_dump()

    logger.info(
        "conab_custo_total_ok",
        cultura=cultura,
        uf=resolved_uf,
        coe=result.get("coe_ha"),
    )

    if return_meta:
        meta = MetaInfo(
            source="conab_custo",
            source_url=metadata.get("url", client.CUSTOS_PAGE),
            source_method="httpx",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=1,
            columns=list(result.keys()),
            parser_version=PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["conab_custo"],
            selected_source="conab_custo",
            fetch_timestamp=datetime.now(UTC),
        )
        return result, meta

    return result
