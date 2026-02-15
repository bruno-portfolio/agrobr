"""API pública do módulo IBGE - PAM e LSPA."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr import constants
from agrobr.cache.keys import build_cache_key
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
    meta.cache_key = build_cache_key(
        "ibge:pam",
        {"produto": produto, "ano": ano},
        schema_version=meta.schema_version,
    )
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
    meta.cache_key = build_cache_key(
        "ibge:lspa",
        {"produto": produto, "ano": ano, "mes": mes},
        schema_version=meta.schema_version,
    )
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


@overload
async def ppm(
    especie: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ppm(
    especie: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def ppm(
    especie: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_ppm",
        source_url="https://sidra.ibge.gov.br",
        source_method="httpx",
        fetched_at=datetime.now(),
    )
    logger.info(
        "ibge_ppm_request",
        especie=especie,
        ano=ano,
        uf=uf,
        nivel=nivel,
    )

    especie_lower = especie.lower()
    all_valid = sorted(
        list(client.REBANHOS_PPM.keys()) + list(client.PRODUTOS_ORIGEM_ANIMAL.keys())
    )

    is_rebanho = especie_lower in client.REBANHOS_PPM
    is_producao = especie_lower in client.PRODUTOS_ORIGEM_ANIMAL

    if not is_rebanho and not is_producao:
        raise ValueError(f"Espécie/produto não suportado: {especie}. Disponíveis: {all_valid}")

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

    classifications: dict[str, str | list[str]] = {}
    if is_rebanho:
        table_code = client.TABELAS["ppm_rebanho"]
        variable = client.VARIAVEIS_PPM["efetivo"]
        classifications["79"] = client.REBANHOS_PPM[especie_lower]
    else:
        table_code = client.TABELAS["ppm_producao"]
        variable = client.VARIAVEIS_PPM["producao"]
        classifications["80"] = client.PRODUTOS_ORIGEM_ANIMAL[especie_lower]

    df = await client.fetch_sidra(
        table_code=table_code,
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=variable,
        period=period,
        classifications=classifications,
    )

    df = client.parse_sidra_response(df)

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    if "localidade_cod" in df.columns:
        df["localidade_cod"] = pd.to_numeric(df["localidade_cod"], errors="coerce").astype("Int64")

    df["especie"] = especie_lower
    df["unidade"] = client.UNIDADES_PPM.get(especie_lower, "")
    df["fonte"] = "ibge_ppm"

    output_cols = [
        c
        for c in [
            "ano",
            "localidade",
            "localidade_cod",
            "especie",
            "valor",
            "unidade",
            "fonte",
        ]
        if c in df.columns
    ]
    df = df[output_cols].reset_index(drop=True)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:ppm",
        {"especie": especie, "ano": ano},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry(constants.Fonte.IBGE, "ppm")

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
        "ibge_ppm_success",
        especie=especie,
        records=len(df),
    )

    if return_meta:
        return df, meta
    return df


@overload
async def abate(
    especie: str,
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def abate(
    especie: str,
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def abate(
    especie: str,
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_abate",
        source_url="https://sidra.ibge.gov.br",
        source_method="httpx",
        fetched_at=datetime.now(),
    )
    logger.info(
        "ibge_abate_request",
        especie=especie,
        trimestre=trimestre,
        uf=uf,
    )

    especie_lower = especie.lower()
    if especie_lower not in client.ESPECIES_ABATE:
        raise ValueError(f"Espécie não suportada: {especie}. Disponíveis: {client.ESPECIES_ABATE}")

    table_code = client.TABELAS_ABATE[especie_lower]
    var_codes = ",".join(client.VARIAVEIS_ABATE.values())

    territorial_level = "3" if uf is None else "3"
    ibge_code = "all"
    if uf:
        ibge_code = client.uf_to_ibge_code(uf)

    if trimestre is None:
        period = "last"
    elif isinstance(trimestre, list):
        period = ",".join(str(t) for t in trimestre)
    else:
        period = str(trimestre)

    classifications: dict[str, str | list[str]] = {
        "12716": "115236",
        "12529": "118225",
    }
    if especie_lower == "bovino":
        classifications["18"] = "992"

    df = await client.fetch_sidra(
        table_code=table_code,
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=var_codes,
        period=period,
        classifications=classifications,
    )

    col_map = {
        "NC": "nivel_cod",
        "NN": "nivel",
        "MC": "unidade_cod",
        "MN": "unidade",
        "V": "valor",
        "D1C": "localidade_cod",
        "D1N": "localidade",
    }

    trimestre_col = None
    variavel_col = None
    variavel_name_col = ""
    trimestre_name_col = ""
    var_ids = {"284", "285", "1000284", "1000285", "151", "1000151"}
    for dc in ["D2C", "D3C"]:
        if dc not in df.columns or len(df) == 0:
            continue
        sample_str = str(df[dc].iloc[0])
        name_col = dc[:-1] + "N"
        if sample_str in var_ids:
            variavel_col = dc
            variavel_name_col = name_col
        elif len(sample_str) == 6 and sample_str[:4].isdigit():
            trimestre_col = dc
            trimestre_name_col = name_col

    if variavel_col:
        col_map[variavel_col] = "variavel_cod"
        col_map[variavel_name_col] = "variavel_nome"
    if trimestre_col:
        col_map[trimestre_col] = "trimestre_cod"
        col_map[trimestre_name_col] = "trimestre_nome"

    rename_map = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    if "trimestre_cod" in df.columns:
        df["trimestre"] = df["trimestre_cod"].astype(str)

    cabecas = df[df["variavel_cod"].astype(str) == "284"].copy()
    peso = df[df["variavel_cod"].astype(str) == "285"].copy()

    merge_keys = [c for c in ["trimestre", "localidade", "localidade_cod"] if c in cabecas.columns]

    if not cabecas.empty and not peso.empty and merge_keys:
        cabecas = cabecas.rename(columns={"valor": "animais_abatidos"})
        peso = peso.rename(columns={"valor": "peso_carcacas"})
        result = cabecas[merge_keys + ["animais_abatidos"]].merge(
            peso[merge_keys + ["peso_carcacas"]],
            on=merge_keys,
            how="outer",
        )
    elif not cabecas.empty:
        result = cabecas.rename(columns={"valor": "animais_abatidos"})
        result["peso_carcacas"] = pd.NA
    else:
        result = pd.DataFrame()

    if not result.empty:
        if "localidade_cod" in result.columns:
            result["localidade_cod"] = pd.to_numeric(
                result["localidade_cod"], errors="coerce"
            ).astype("Int64")

        result["especie"] = especie_lower
        result["fonte"] = "ibge_abate"

        output_cols = [
            c
            for c in [
                "trimestre",
                "localidade",
                "localidade_cod",
                "especie",
                "animais_abatidos",
                "peso_carcacas",
                "fonte",
            ]
            if c in result.columns
        ]
        result = result[output_cols].reset_index(drop=True)

        for col in ["animais_abatidos", "peso_carcacas"]:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce")

    df = result

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:abate",
        {"especie": especie, "trimestre": trimestre},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry(constants.Fonte.IBGE, "abate")

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
        "ibge_abate_success",
        especie=especie,
        records=len(df),
    )

    if return_meta:
        return df, meta
    return df


async def especies_abate() -> list[str]:
    return list(client.ESPECIES_ABATE)


async def especies_ppm() -> list[str]:
    return sorted(list(client.REBANHOS_PPM.keys()) + list(client.PRODUTOS_ORIGEM_ANIMAL.keys()))


_CLASSIFICACOES_CENSO_AGRO: dict[str, dict[str, str]] = {
    "efetivo_rebanho": {
        "829": "46302",
        "12443": "all",
        "218": "46502",
    },
    "uso_terra": {
        "829": "46302",
        "222": "all",
        "218": "46502",
        "12517": "113601",
        "12567": "41151",
    },
    "lavoura_temporaria": {
        "829": "46302",
        "226": "all",
        "218": "46502",
        "12517": "113601",
    },
    "lavoura_permanente": {
        "829": "46302",
        "227": "all",
        "220": "110085",
    },
}

_CENSO_VAR_NOME: dict[str, str] = {
    "10010": "estabelecimentos",
    "2209": "cabecas",
    "9587": "estabelecimentos",
    "184": "area",
    "10084": "estabelecimentos",
    "10085": "producao",
    "10089": "area_colhida",
    "9504": "estabelecimentos",
    "9506": "producao",
    "10078": "area_colhida",
}

_CENSO_VAR_UNIDADE: dict[str, str] = {
    "10010": "unidades",
    "2209": "cabeças",
    "9587": "unidades",
    "184": "hectares",
    "10084": "unidades",
    "10085": "",
    "10089": "hectares",
    "9504": "unidades",
    "9506": "",
    "10078": "hectares",
}

_CENSO_ALL_VAR_IDS = set(_CENSO_VAR_NOME.keys())

_CENSO_CATEGORIA_COL_INDEX: dict[str, int] = {
    "efetivo_rebanho": 5,
    "uso_terra": 5,
    "lavoura_temporaria": 5,
    "lavoura_permanente": 5,
}


@overload
async def censo_agro(
    tema: str,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def censo_agro(
    tema: str,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def censo_agro(
    tema: str,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_censo_agro",
        source_url="https://sidra.ibge.gov.br",
        source_method="httpx",
        fetched_at=datetime.now(),
    )
    logger.info(
        "ibge_censo_agro_request",
        tema=tema,
        uf=uf,
        nivel=nivel,
    )

    tema_lower = tema.lower()
    if tema_lower not in client.TABELAS_CENSO_AGRO:
        raise ValueError(f"Tema não suportado: {tema}. Disponíveis: {client.TEMAS_CENSO_AGRO}")

    table_code = client.TABELAS_CENSO_AGRO[tema_lower]
    var_map = client.VARIAVEIS_CENSO_AGRO[tema_lower]
    var_codes = ",".join(var_map.values())

    nivel_map = {
        "brasil": "1",
        "uf": "3",
        "municipio": "6",
    }
    territorial_level = nivel_map.get(nivel, "3")

    ibge_code = "all"
    if uf and nivel in ("uf", "municipio"):
        ibge_code = client.uf_to_ibge_code(uf)

    classifications: dict[str, str | list[str]] = dict(_CLASSIFICACOES_CENSO_AGRO[tema_lower])

    df = await client.fetch_sidra(
        table_code=table_code,
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=var_codes,
        period="all",
        classifications=classifications,
    )

    if df.empty:
        df = pd.DataFrame(
            columns=[
                "ano",
                "localidade",
                "localidade_cod",
                "tema",
                "categoria",
                "variavel",
                "valor",
                "unidade",
                "fonte",
            ]
        )
        if return_meta:
            return df, meta
        return df

    col_map: dict[str, str] = {
        "NC": "nivel_cod",
        "NN": "nivel",
        "V": "valor",
        "D1C": "localidade_cod",
        "D1N": "localidade",
    }

    for dc in ["D2C", "D3C"]:
        if dc not in df.columns or len(df) == 0:
            continue
        sample = str(df[dc].iloc[0])
        name_col = dc[:-1] + "N"
        if sample in _CENSO_ALL_VAR_IDS:
            col_map[dc] = "variavel_cod"
            col_map[name_col] = "variavel_nome"
        elif len(sample) == 4 and sample.isdigit():
            col_map[dc] = "ano_cod"
            col_map[name_col] = "ano_nome"

    cat_idx = _CENSO_CATEGORIA_COL_INDEX[tema_lower]
    cat_col_c = f"D{cat_idx}C"
    cat_col_n = f"D{cat_idx}N"
    if cat_col_c in df.columns:
        col_map[cat_col_c] = "categoria_cod"
    if cat_col_n in df.columns:
        col_map[cat_col_n] = "categoria"

    rename_map = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    if "categoria" in df.columns:
        df = df[df["categoria"].str.lower() != "total"]

    if "ano_cod" in df.columns:
        df["ano"] = pd.to_numeric(df["ano_cod"], errors="coerce").astype("Int64")
    else:
        df["ano"] = 2017

    if "variavel_cod" in df.columns:
        df["variavel"] = df["variavel_cod"].map(_CENSO_VAR_NOME).fillna(df.get("variavel_nome", ""))
        unidade_from_map = df["variavel_cod"].map(_CENSO_VAR_UNIDADE)
        mn_col = "MN" if "MN" in df.columns else None
        if mn_col is None:
            for c in df.columns:
                if c == "MN":
                    mn_col = c
                    break
        if mn_col and mn_col in df.columns:
            df["unidade"] = unidade_from_map.where(unidade_from_map != "", df[mn_col])
        else:
            df["unidade"] = unidade_from_map.fillna("")
    else:
        df["variavel"] = ""
        df["unidade"] = ""

    if "localidade_cod" in df.columns:
        df["localidade_cod"] = pd.to_numeric(df["localidade_cod"], errors="coerce").astype("Int64")

    df["tema"] = tema_lower
    df["fonte"] = "ibge_censo_agro"

    output_cols = [
        c
        for c in [
            "ano",
            "localidade",
            "localidade_cod",
            "tema",
            "categoria",
            "variavel",
            "valor",
            "unidade",
            "fonte",
        ]
        if c in df.columns
    ]
    df = df[output_cols].reset_index(drop=True)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:censo_agro",
        {"tema": tema, "uf": uf},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry(constants.Fonte.IBGE, "censo_agro")

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
        "ibge_censo_agro_success",
        tema=tema,
        records=len(df),
    )

    if return_meta:
        return df, meta
    return df


async def temas_censo_agro() -> list[str]:
    return list(client.TEMAS_CENSO_AGRO)


async def ufs() -> list[str]:
    return list(client.get_uf_codes().keys())
