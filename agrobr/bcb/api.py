"""API pública do módulo BCB/SICOR."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client
from .models import UF_CODES, normalize_safra_sicor, resolve_produto_sicor
from .parser import PARSER_VERSION, agregar_por_uf, parse_credito_rural

logger = structlog.get_logger()


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: str = "municipio",
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: str = "municipio",
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: str = "municipio",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Obtém dados de crédito rural do SICOR/BCB.

    Busca contratos de crédito rural aprovados por município,
    opcionalmente filtrados por produto, safra e UF.

    Args:
        produto: Nome do produto (soja, milho, arroz, etc).
        safra: Safra no formato "2024/25" ou "2023/2024" (default: mais recente).
        finalidade: Finalidade do crédito ("custeio", "investimento", etc).
        uf: Filtrar por UF (ex: "MT", "PR").
        agregacao: "municipio" (padrão) ou "uf" (agrega por UF).
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo).

    Returns:
        DataFrame com colunas: safra, uf, cd_municipio, municipio,
        produto, valor, area_financiada, qtd_contratos.

    Example:
        >>> df = await bcb.credito_rural("soja", safra="2024/25", finalidade="custeio")
        >>> df, meta = await bcb.credito_rural("soja", safra="2024/25", return_meta=True)
    """
    t0 = time.monotonic()

    produto_sicor = resolve_produto_sicor(produto)
    safra_sicor = normalize_safra_sicor(safra) if safra else None
    cd_uf = UF_CODES.get(uf.upper()) if uf else None

    logger.info(
        "bcb_credito_rural_request",
        produto=produto,
        produto_sicor=produto_sicor,
        safra=safra,
        safra_sicor=safra_sicor,
        finalidade=finalidade,
        uf=uf,
    )

    dados, source_used = await client.fetch_credito_rural_with_fallback(
        finalidade=finalidade,
        produto_sicor=produto_sicor,
        safra_sicor=safra_sicor,
        cd_uf=cd_uf,
    )

    fetch_ms = int((time.monotonic() - t0) * 1000)

    attempted_sources = ["bcb_odata"]
    if source_used == "bigquery":
        attempted_sources.append("bcb_bigquery")

    t1 = time.monotonic()
    df = parse_credito_rural(dados, finalidade=finalidade)

    if uf and "uf" in df.columns:
        df = df[df["uf"] == uf.upper()].reset_index(drop=True)

    if agregacao == "uf":
        df = agregar_por_uf(df)

    parse_ms = int((time.monotonic() - t1) * 1000)

    source_method = "httpx" if source_used == "odata" else "bigquery"

    logger.info(
        "bcb_credito_rural_ok",
        produto=produto,
        safra=safra,
        records=len(df),
        source_used=source_used,
    )

    if return_meta:
        meta = MetaInfo(
            source="bcb",
            source_url=f"{client.BASE_URL}/{client.ENDPOINT_MAP.get(finalidade.lower(), 'CusteioMunicipio')}",
            source_method=source_method,
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=attempted_sources,
            selected_source=f"bcb_{source_used}",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df
