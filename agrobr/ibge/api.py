"""API pública do módulo IBGE - PAM e LSPA."""

from __future__ import annotations

from typing import Literal

import pandas as pd
import structlog

from agrobr.ibge import client

logger = structlog.get_logger()


async def pam(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: bool = False,
) -> pd.DataFrame:
    """
    Obtém dados da Produção Agrícola Municipal (PAM).

    Args:
        produto: Nome do produto (soja, milho, arroz, feijao, trigo, etc)
        ano: Ano ou lista de anos (default: último disponível)
        uf: Filtrar por UF (ex: "MT", "PR"). Requer nivel="uf" ou "municipio"
        nivel: Nível territorial ("brasil", "uf", "municipio")
        variaveis: Lista de variáveis (area_plantada, area_colhida, producao, rendimento)
        as_polars: Se True, retorna polars.DataFrame

    Returns:
        DataFrame com dados da PAM

    Example:
        >>> df = await ibge.pam('soja', ano=2023)
        >>> df = await ibge.pam('milho', ano=[2020, 2021, 2022], uf='MT')
    """
    logger.info(
        "ibge_pam_request",
        produto=produto,
        ano=ano,
        uf=uf,
        nivel=nivel,
    )

    # Mapeia produto para código SIDRA
    produto_lower = produto.lower()
    if produto_lower not in client.PRODUTOS_PAM:
        raise ValueError(
            f"Produto não suportado: {produto}. "
            f"Disponíveis: {list(client.PRODUTOS_PAM.keys())}"
        )

    produto_cod = client.PRODUTOS_PAM[produto_lower]

    # Mapeia variáveis
    if variaveis is None:
        variaveis = ["area_plantada", "area_colhida", "producao", "rendimento"]

    var_codes = []
    for var in variaveis:
        if var in client.VARIAVEIS:
            var_codes.append(client.VARIAVEIS[var])
        else:
            logger.warning(f"Variável desconhecida: {var}")

    # Mapeia nível territorial
    nivel_map = {
        "brasil": "1",
        "uf": "3",
        "municipio": "6",
    }
    territorial_level = nivel_map.get(nivel, "3")

    # Define código territorial
    ibge_code = "all"
    if uf and nivel in ("uf", "municipio"):
        ibge_code = client.uf_to_ibge_code(uf)

    # Define período
    if ano is None:
        period = "last"
    elif isinstance(ano, list):
        period = ",".join(str(a) for a in ano)
    else:
        period = str(ano)

    # Busca dados
    df = await client.fetch_sidra(
        table_code=client.TABELAS["pam_nova"],
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=",".join(var_codes) if var_codes else None,
        period=period,
        classifications={"782": produto_cod},
    )

    # Processa resposta
    df = client.parse_sidra_response(df)

    # Pivota para ter variáveis como colunas
    if "variavel" in df.columns and "valor" in df.columns:
        df_pivot = df.pivot_table(
            index=["localidade", "ano"] if "localidade" in df.columns else ["ano"],
            columns="variavel",
            values="valor",
            aggfunc="first",
        ).reset_index()

        # Renomeia colunas para nomes mais simples
        rename_map = {
            "Área plantada": "area_plantada",
            "Área colhida": "area_colhida",
            "Quantidade produzida": "producao",
            "Rendimento médio da produção": "rendimento",
            "Valor da produção": "valor_producao",
        }
        df_pivot = df_pivot.rename(columns=rename_map)
        df = df_pivot

    df["produto"] = produto_lower
    df["fonte"] = "ibge_pam"

    if as_polars:
        try:
            import polars as pl
            return pl.from_pandas(df)
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    logger.info(
        "ibge_pam_success",
        produto=produto,
        records=len(df),
    )

    return df


async def lspa(
    produto: str,
    ano: int | str | None = None,
    mes: int | str | None = None,
    uf: str | None = None,
    as_polars: bool = False,
) -> pd.DataFrame:
    """
    Obtém dados do Levantamento Sistemático da Produção Agrícola (LSPA).

    O LSPA fornece estimativas mensais de safra para os principais produtos.

    Args:
        produto: Nome do produto (soja, milho_1, milho_2, arroz, feijao_1, etc)
        ano: Ano de referência (default: atual)
        mes: Mês de referência (1-12). Se None, retorna todos os meses do ano.
        uf: Filtrar por UF (ex: "MT", "PR")
        as_polars: Se True, retorna polars.DataFrame

    Returns:
        DataFrame com estimativas LSPA

    Example:
        >>> df = await ibge.lspa('soja', ano=2024)
        >>> df = await ibge.lspa('milho_1', ano=2024, mes=6, uf='PR')
    """
    logger.info(
        "ibge_lspa_request",
        produto=produto,
        ano=ano,
        mes=mes,
        uf=uf,
    )

    # Mapeia produto para código SIDRA
    produto_lower = produto.lower()
    if produto_lower not in client.PRODUTOS_LSPA:
        raise ValueError(
            f"Produto não suportado: {produto}. "
            f"Disponíveis: {list(client.PRODUTOS_LSPA.keys())}"
        )

    produto_cod = client.PRODUTOS_LSPA[produto_lower]

    # Define período
    if ano is None:
        from datetime import date
        ano = date.today().year

    if mes:
        period = f"{ano}{int(mes):02d}"
    else:
        # Todos os meses do ano
        period = ",".join(f"{ano}{m:02d}" for m in range(1, 13))

    # Define nível territorial
    territorial_level = "3" if uf else "1"
    ibge_code = client.uf_to_ibge_code(uf) if uf else "all"

    # Busca dados (não especifica variáveis - retorna todas)
    df = await client.fetch_sidra(
        table_code=client.TABELAS["lspa"],
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        period=period,
        classifications={"48": produto_cod},
    )

    # Processa resposta
    df = client.parse_sidra_response(df)

    # Adiciona período da consulta
    df["ano"] = ano
    if mes:
        df["mes"] = mes

    df["produto"] = produto_lower
    df["fonte"] = "ibge_lspa"

    if as_polars:
        try:
            import polars as pl
            return pl.from_pandas(df)
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    logger.info(
        "ibge_lspa_success",
        produto=produto,
        records=len(df),
    )

    return df


async def produtos_pam() -> list[str]:
    """
    Lista produtos disponíveis na PAM.

    Returns:
        Lista de nomes de produtos

    Example:
        >>> prods = await ibge.produtos_pam()
        >>> print(prods)
        ['soja', 'milho', 'arroz', ...]
    """
    return list(client.PRODUTOS_PAM.keys())


async def produtos_lspa() -> list[str]:
    """
    Lista produtos disponíveis no LSPA.

    Returns:
        Lista de nomes de produtos

    Example:
        >>> prods = await ibge.produtos_lspa()
        >>> print(prods)
        ['soja', 'milho_1', 'milho_2', ...]
    """
    return list(client.PRODUTOS_LSPA.keys())


async def ufs() -> list[str]:
    """
    Lista UFs disponíveis.

    Returns:
        Lista de siglas de UF
    """
    return list(client.get_uf_codes().keys())
