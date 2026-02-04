"""API pública do módulo CONAB."""

from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from agrobr import constants
from agrobr.conab import client
from agrobr.conab.parsers.v1 import ConabParserV1

logger = structlog.get_logger()


async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    as_polars: bool = False,
) -> pd.DataFrame:
    """
    Obtém dados de safra por produto.

    Args:
        produto: Nome do produto (soja, milho, arroz, feijao, algodao, trigo, etc)
        safra: Safra no formato "2024/25" (default: mais recente)
        uf: Filtrar por UF (ex: "MT", "PR")
        levantamento: Número do levantamento (default: mais recente)
        as_polars: Se True, retorna polars.DataFrame

    Returns:
        DataFrame com dados de safra por UF

    Example:
        >>> df = await conab.safras('soja', safra='2025/26')
        >>> df = await conab.safras('milho', uf='MT')
    """
    logger.info(
        "conab_safras_request",
        produto=produto,
        safra=safra,
        uf=uf,
        levantamento=levantamento,
    )

    xlsx, metadata = await client.fetch_safra_xlsx(safra=safra, levantamento=levantamento)

    parser = ConabParserV1()
    safra_list = parser.parse_safra_produto(
        xlsx=xlsx,
        produto=produto,
        safra_ref=safra or metadata["safra"],
        levantamento=metadata.get("levantamento"),
    )

    if uf:
        safra_list = [s for s in safra_list if s.uf == uf.upper()]

    if not safra_list:
        logger.warning(
            "conab_safras_empty",
            produto=produto,
            safra=safra,
            uf=uf,
        )
        return pd.DataFrame()

    df = pd.DataFrame([s.model_dump() for s in safra_list])

    if as_polars:
        try:
            import polars as pl
            return pl.from_pandas(df)
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    logger.info(
        "conab_safras_success",
        produto=produto,
        records=len(df),
    )

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
            return pl.from_pandas(df)
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
            return pl.from_pandas(df)
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
