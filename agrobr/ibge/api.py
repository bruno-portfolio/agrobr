"""API pública do módulo IBGE - PAM e LSPA."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr import constants
from agrobr.cache.policies import calculate_expiry
from agrobr.ibge import client
from agrobr.models import MetaInfo

logger = structlog.get_logger()

_LSPA_ALIASES: dict[str, list[str]] = {
    "milho": ["milho_1", "milho_2"],
    "feijao": ["feijao_1", "feijao_2", "feijao_3"],
    "amendoim": ["amendoim_1", "amendoim_2"],
    "batata": ["batata_1", "batata_2"],
}


def _expand_lspa_produto(produto: str) -> list[tuple[str, str]]:
    """Expande nome genérico de produto LSPA para sub-produtos.

    Args:
        produto: Nome do produto (pode ser genérico como "milho" ou específico como "milho_1")

    Returns:
        Lista de tuplas (nome_sub_produto, codigo_sidra)

    Raises:
        ValueError: Se o produto não for suportado
    """
    if produto in client.PRODUTOS_LSPA:
        return [(produto, client.PRODUTOS_LSPA[produto])]

    if produto in _LSPA_ALIASES:
        return [(sub, client.PRODUTOS_LSPA[sub]) for sub in _LSPA_ALIASES[produto]]

    all_valid = sorted(set(list(client.PRODUTOS_LSPA.keys()) + list(_LSPA_ALIASES.keys())))
    raise ValueError(f"Produto não suportado: {produto}. Disponíveis: {all_valid}")


@overload
async def pam(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def pam(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def pam(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """
    Obtém dados da Produção Agrícola Municipal (PAM).

    Args:
        produto: Nome do produto (soja, milho, arroz, feijao, trigo, etc)
        ano: Ano ou lista de anos (default: último disponível)
        uf: Filtrar por UF (ex: "MT", "PR"). Requer nivel="uf" ou "municipio"
        nivel: Nível territorial ("brasil", "uf", "municipio")
        variaveis: Lista de variáveis (area_plantada, area_colhida, producao, rendimento)
        as_polars: Se True, retorna polars.DataFrame
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

    Returns:
        DataFrame com dados da PAM ou tupla (DataFrame, MetaInfo)

    Example:
        >>> df = await ibge.pam('soja', ano=2023)
        >>> df, meta = await ibge.pam('milho', ano=[2020, 2021, 2022], uf='MT', return_meta=True)
    """
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_pam",
        source_url="https://sidra.ibge.gov.br",
        source_method="httpx",
        fetched_at=datetime.now(),
    )
    logger.info(
        "ibge_pam_request",
        produto=produto,
        ano=ano,
        uf=uf,
        nivel=nivel,
    )

    produto_lower = produto.lower()
    if produto_lower not in client.PRODUTOS_PAM:
        raise ValueError(
            f"Produto não suportado: {produto}. Disponíveis: {list(client.PRODUTOS_PAM.keys())}"
        )

    produto_cod = client.PRODUTOS_PAM[produto_lower]

    if variaveis is None:
        variaveis = ["area_plantada", "area_colhida", "producao", "rendimento"]

    var_codes = []
    for var in variaveis:
        if var in client.VARIAVEIS:
            var_codes.append(client.VARIAVEIS[var])
        else:
            logger.warning(f"Variável desconhecida: {var}")

    nivel_map = {
        "brasil": "1",
        "uf": "3",
        "municipio": "6",
    }
    territorial_level = nivel_map.get(nivel, "3")

    ibge_code = "all"
    if uf and nivel in ("uf", "municipio"):
        ibge_code = client.uf_to_ibge_code(uf)

    if ano is None:
        period = "last"
    elif isinstance(ano, list):
        period = ",".join(str(a) for a in ano)
    else:
        period = str(ano)

    df = await client.fetch_sidra(
        table_code=client.TABELAS["pam_nova"],
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=",".join(var_codes) if var_codes else None,
        period=period,
        classifications={"782": produto_cod},
    )

    df = client.parse_sidra_response(df)

    if "variavel" in df.columns and "valor" in df.columns:
        df_pivot = df.pivot_table(
            index=["localidade", "ano"] if "localidade" in df.columns else ["ano"],
            columns="variavel",
            values="valor",
            aggfunc="first",
        ).reset_index()

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

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = f"ibge:pam:{produto}:{ano}"
    meta.cache_expires_at = calculate_expiry(constants.Fonte.IBGE, "pam")

    if as_polars:
        try:
            import polars as pl

            result_df = pl.from_pandas(df)
            if return_meta:
                return result_df, meta  # type: ignore[return-value,no-any-return]
            return result_df  # type: ignore[return-value,no-any-return]
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    logger.info(
        "ibge_pam_success",
        produto=produto,
        records=len(df),
    )

    if return_meta:
        return df, meta
    return df


@overload
async def lspa(
    produto: str,
    ano: int | str | None = None,
    mes: int | str | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def lspa(
    produto: str,
    ano: int | str | None = None,
    mes: int | str | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def lspa(
    produto: str,
    ano: int | str | None = None,
    mes: int | str | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """
    Obtém dados do Levantamento Sistemático da Produção Agrícola (LSPA).

    O LSPA fornece estimativas mensais de safra para os principais produtos.

    Args:
        produto: Nome do produto (soja, milho_1, milho_2, arroz, feijao_1, etc)
        ano: Ano de referência (default: atual)
        mes: Mês de referência (1-12). Se None, retorna todos os meses do ano.
        uf: Filtrar por UF (ex: "MT", "PR")
        as_polars: Se True, retorna polars.DataFrame
        return_meta: Se True, retorna tupla (DataFrame, MetaInfo)

    Returns:
        DataFrame com estimativas LSPA ou tupla (DataFrame, MetaInfo)

    Example:
        >>> df = await ibge.lspa('soja', ano=2024)
        >>> df, meta = await ibge.lspa('milho_1', ano=2024, mes=6, uf='PR', return_meta=True)
    """
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_lspa",
        source_url="https://sidra.ibge.gov.br",
        source_method="httpx",
        fetched_at=datetime.now(),
    )
    logger.info(
        "ibge_lspa_request",
        produto=produto,
        ano=ano,
        mes=mes,
        uf=uf,
    )

    produto_lower = produto.lower()
    sub_produtos = _expand_lspa_produto(produto_lower)

    if ano is None:
        from datetime import date

        ano = date.today().year

    period = f"{ano}{int(mes):02d}" if mes else ",".join(f"{ano}{m:02d}" for m in range(1, 13))

    territorial_level = "3" if uf else "1"
    ibge_code = client.uf_to_ibge_code(uf) if uf else "all"

    frames: list[pd.DataFrame] = []
    for sub_nome, sub_cod in sub_produtos:
        sub_df = await client.fetch_sidra(
            table_code=client.TABELAS["lspa"],
            territorial_level=territorial_level,
            ibge_territorial_code=ibge_code,
            period=period,
            classifications={"48": sub_cod},
        )

        sub_df = client.parse_sidra_response(sub_df)

        sub_df["ano"] = ano
        if mes:
            sub_df["mes"] = mes

        sub_df["produto"] = sub_nome
        sub_df["fonte"] = "ibge_lspa"
        frames.append(sub_df)

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = f"ibge:lspa:{produto}:{ano}:{mes}"
    meta.cache_expires_at = calculate_expiry(constants.Fonte.IBGE, "lspa")

    if as_polars:
        try:
            import polars as pl

            result_df = pl.from_pandas(df)
            if return_meta:
                return result_df, meta  # type: ignore[return-value,no-any-return]
            return result_df  # type: ignore[return-value,no-any-return]
        except ImportError:
            logger.warning("polars_not_installed", fallback="pandas")

    logger.info(
        "ibge_lspa_success",
        produto=produto,
        records=len(df),
    )

    if return_meta:
        return df, meta
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
