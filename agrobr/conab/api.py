"""API pública do módulo CONAB."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr import constants
from agrobr.cache.policies import calculate_expiry
from agrobr.conab import client
from agrobr.conab.parsers.v1 import ConabParserV1
from agrobr.models import MetaInfo

logger = structlog.get_logger()


@overload
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame:
    ...


@overload
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]:
    ...


async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """
    Obtém dados de safra por produto.

    Args:
        produto: Nome do produto (soja, milho, arroz, feijao, algodao, trigo, etc)
        safra: Safra no formato "2024/25" (default: mais recente)
        uf: Filtrar por UF (ex: "MT", "PR")
        levantamento: Número do levantamento (default: mais recente)
        as_polars: Se True, retorna polars.DataFrame
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

    Returns:
        DataFrame com dados de safra por UF ou tupla (DataFrame, MetaInfo)

    Example:
        >>> df = await conab.safras('soja', safra='2025/26')
        >>> df, meta = await conab.safras('milho', uf='MT', return_meta=True)
    """
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="conab",
        source_url="https://www.conab.gov.br/info-agro/safras/graos",
        source_method="httpx",
        fetched_at=datetime.now(),
    )

    logger.info(
        "conab_safras_request",
        produto=produto,
        safra=safra,
        uf=uf,
        levantamento=levantamento,
    )

    parse_start = time.perf_counter()
    xlsx, metadata = await client.fetch_safra_xlsx(safra=safra, levantamento=levantamento)

    meta.raw_content_size = len(xlsx) if isinstance(xlsx, bytes) else 0
    meta.source_url = metadata.get("url", meta.source_url)

    parser = ConabParserV1()
    safra_list = parser.parse_safra_produto(
        xlsx=xlsx,
        produto=produto,
        safra_ref=safra or metadata["safra"],
        levantamento=metadata.get("levantamento"),
    )

    meta.parse_duration_ms = int((time.perf_counter() - parse_start) * 1000)
    meta.parser_version = parser.version

    if uf:
        safra_list = [s for s in safra_list if s.uf == uf.upper()]

    if not safra_list:
        logger.warning(
            "conab_safras_empty",
            produto=produto,
            safra=safra,
            uf=uf,
        )
        df = pd.DataFrame()
        if return_meta:
            meta.records_count = 0
            meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
            return df, meta
        return df

    df = pd.DataFrame([s.model_dump() for s in safra_list])

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = f"conab:safras:{produto}:{safra or 'latest'}"
    meta.cache_expires_at = calculate_expiry(constants.Fonte.CONAB)

    if as_polars:
        try:
            import polars as pl

            result_df = pl.from_pandas(df)
            if return_meta:
                return result_df, meta  # type: ignore[return-value]
            return result_df  # type: ignore[return-value]
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    logger.info(
        "conab_safras_success",
        produto=produto,
        records=len(df),
    )

    if return_meta:
        return df, meta
    return df


async def balanco(
    produto: str | None = None,
    safra: str | None = None,
    as_polars: bool = False,
) -> pd.DataFrame:
    """
    Obtém dados de balanço de oferta e demanda.

    Args:
        produto: Filtrar por produto (soja, milho, etc). None para todos.
        safra: Safra de referência para o levantamento (default: mais recente)
        as_polars: Se True, retorna polars.DataFrame

    Returns:
        DataFrame com balanço de oferta/demanda

    Example:
        >>> df = await conab.balanco('soja')
        >>> df = await conab.balanco()  # Todos os produtos
    """
    logger.info(
        "conab_balanco_request",
        produto=produto,
        safra=safra,
    )

    xlsx, metadata = await client.fetch_safra_xlsx(safra=safra)

    parser = ConabParserV1()
    suprimentos = parser.parse_suprimento(xlsx=xlsx, produto=produto)

    if not suprimentos:
        logger.warning(
            "conab_balanco_empty",
            produto=produto,
        )
        return pd.DataFrame()

    df = pd.DataFrame(suprimentos)

    if as_polars:
        try:
            import polars as pl

            return pl.from_pandas(df)  # type: ignore[no-any-return]
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    logger.info(
        "conab_balanco_success",
        produto=produto,
        records=len(df),
    )

    return df


async def brasil_total(
    safra: str | None = None,
    as_polars: bool = False,
) -> pd.DataFrame:
    """
    Obtém totais do Brasil por produto.

    Args:
        safra: Safra de referência (default: mais recente)
        as_polars: Se True, retorna polars.DataFrame

    Returns:
        DataFrame com totais por produto

    Example:
        >>> df = await conab.brasil_total()
        >>> df = await conab.brasil_total(safra='2025/26')
    """
    logger.info(
        "conab_brasil_total_request",
        safra=safra,
    )

    xlsx, metadata = await client.fetch_safra_xlsx(safra=safra)

    parser = ConabParserV1()
    totais = parser.parse_brasil_total(xlsx=xlsx, safra_ref=safra)

    if not totais:
        logger.warning("conab_brasil_total_empty", safra=safra)
        return pd.DataFrame()

    df = pd.DataFrame(totais)

    if as_polars:
        try:
            import polars as pl

            return pl.from_pandas(df)  # type: ignore[no-any-return]
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    logger.info(
        "conab_brasil_total_success",
        records=len(df),
    )

    return df


async def levantamentos() -> list[dict[str, Any]]:
    """
    Lista levantamentos de safra disponíveis.

    Returns:
        Lista de dicts com informações dos levantamentos

    Example:
        >>> levs = await conab.levantamentos()
        >>> for lev in levs[:5]:
        ...     print(f"{lev['safra']} - {lev['levantamento']}º levantamento")
    """
    return await client.list_levantamentos()


async def produtos() -> list[str]:
    """
    Lista produtos disponíveis.

    Returns:
        Lista de nomes de produtos

    Example:
        >>> prods = await conab.produtos()
        >>> print(prods)
        ['soja', 'milho', 'arroz', 'feijao', ...]
    """
    return list(constants.CONAB_PRODUTOS.keys())


async def ufs() -> list[str]:
    """
    Lista UFs disponíveis.

    Returns:
        Lista de siglas de UF

    Example:
        >>> estados = await conab.ufs()
        >>> print(estados)
        ['AC', 'AL', 'AM', ...]
    """
    return constants.CONAB_UFS.copy()
