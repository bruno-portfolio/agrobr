from __future__ import annotations

import time
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr.cache.keys import build_cache_key
from agrobr.cache.policies import calculate_expiry
from agrobr.exceptions import InvalidParameterError
from agrobr.ibge import client
from agrobr.ibge._helpers import SIDRA_BASE, resolve_ibge_code, resolve_period
from agrobr.models import MetaInfo
from agrobr.utils.result import finalize_result
from agrobr.utils.time import utcnow

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Silvicultura (PEVS) — tab 291 (produção c194) + tab 5930 (área c734)
# ---------------------------------------------------------------------------


@overload
async def silvicultura(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def silvicultura(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def silvicultura(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_silvicultura",
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_silvicultura"],
        selected_source="ibge_silvicultura",
    )
    logger.info(
        "ibge_silvicultura_request",
        produto=produto,
        ano=ano,
        uf=uf,
        nivel=nivel,
        variavel=variavel,
    )

    produto_lower = produto.lower()
    variavel_lower = variavel.lower()

    if variavel_lower == "area":
        if produto_lower not in client.ESPECIES_SILVICULTURA_AREA:
            raise InvalidParameterError(
                f"Espécie não suportada para área: {produto}. "
                f"Disponíveis: {list(client.ESPECIES_SILVICULTURA_AREA.keys())}"
            )
        table_code = client.TABELAS_PEVS["silvicultura_area"]
        var_code = client.VARIAVEIS_SILVICULTURA_AREA["area_total"]
        classification_key = "734"
        classification_val = client.ESPECIES_SILVICULTURA_AREA[produto_lower]
    elif variavel_lower in ("quantidade_produzida", "valor_producao"):
        if produto_lower not in client.PRODUTOS_SILVICULTURA:
            raise InvalidParameterError(
                f"Produto não suportado: {produto}. "
                f"Disponíveis: {list(client.PRODUTOS_SILVICULTURA.keys())}"
            )
        table_code = client.TABELAS_PEVS["silvicultura_producao"]
        var_code = client.VARIAVEIS_SILVICULTURA[variavel_lower]
        classification_key = "194"
        classification_val = client.PRODUTOS_SILVICULTURA[produto_lower]
    else:
        raise InvalidParameterError(
            f"Variável não suportada: {variavel}. "
            f"Disponíveis: ['quantidade_produzida', 'valor_producao', 'area']"
        )

    territorial_level, ibge_code = resolve_ibge_code(uf, nivel)
    period = resolve_period(ano)

    classifications: dict[str, str | list[str]] = {classification_key: classification_val}

    df = await client.fetch_sidra(
        table_code=table_code,
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=var_code,
        period=period,
        classifications=classifications,
    )

    df = client.parse_sidra_response(
        df,
        rename_columns={
            "MC": "unidade_cod",
            "MN": "unidade_medida",
            "D1C": "localidade_cod",
            "D1N": "localidade",
            "D2C": "ano_cod",
            "D2N": "ano",
            "D3C": "variavel_cod",
            "D3N": "variavel_nome",
            "D4C": "produto_cod",
            "D4N": "produto_raw",
        },
    )

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    if "localidade_cod" in df.columns:
        df["localidade_cod"] = pd.to_numeric(df["localidade_cod"], errors="coerce").astype("Int64")

    df["produto"] = produto_lower
    if variavel_lower == "area":
        df["unidade"] = "Hectares"
    else:
        df["unidade"] = client.UNIDADES_SILVICULTURA.get(produto_lower, "")
    df["fonte"] = "ibge_silvicultura"

    output_cols = [
        c
        for c in ["ano", "localidade", "localidade_cod", "produto", "valor", "unidade", "fonte"]
        if c in df.columns
    ]
    df = df[output_cols].reset_index(drop=True)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:silvicultura",
        {"produto": produto, "ano": ano, "variavel": variavel},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry("ibge_silvicultura")

    logger.info("ibge_silvicultura_success", produto=produto, records=len(df))

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def produtos_silvicultura() -> list[str]:
    return list(client.PRODUTOS_SILVICULTURA.keys())


async def especies_silvicultura_area() -> list[str]:
    return list(client.ESPECIES_SILVICULTURA_AREA.keys())


# ---------------------------------------------------------------------------
# Extração Vegetal (PEVS) — tab 289 (c193)
# ---------------------------------------------------------------------------


@overload
async def extracao_vegetal(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def extracao_vegetal(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def extracao_vegetal(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_extracao_vegetal",
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_extracao_vegetal"],
        selected_source="ibge_extracao_vegetal",
    )
    logger.info(
        "ibge_extracao_vegetal_request",
        produto=produto,
        ano=ano,
        uf=uf,
        nivel=nivel,
        variavel=variavel,
    )

    produto_lower = produto.lower()
    variavel_lower = variavel.lower()

    if produto_lower not in client.PRODUTOS_EXTRACAO_VEGETAL:
        raise InvalidParameterError(
            f"Produto não suportado: {produto}. "
            f"Disponíveis: {list(client.PRODUTOS_EXTRACAO_VEGETAL.keys())}"
        )

    if variavel_lower not in client.VARIAVEIS_EXTRACAO_VEGETAL:
        raise InvalidParameterError(
            f"Variável não suportada: {variavel}. "
            f"Disponíveis: {list(client.VARIAVEIS_EXTRACAO_VEGETAL.keys())}"
        )

    table_code = client.TABELAS_PEVS["extracao_vegetal"]
    var_code = client.VARIAVEIS_EXTRACAO_VEGETAL[variavel_lower]
    produto_cod = client.PRODUTOS_EXTRACAO_VEGETAL[produto_lower]

    territorial_level, ibge_code = resolve_ibge_code(uf, nivel)
    period = resolve_period(ano)

    classifications: dict[str, str | list[str]] = {"193": produto_cod}

    df = await client.fetch_sidra(
        table_code=table_code,
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=var_code,
        period=period,
        classifications=classifications,
    )

    df = client.parse_sidra_response(
        df,
        rename_columns={
            "MC": "unidade_cod",
            "MN": "unidade_medida",
            "D1C": "localidade_cod",
            "D1N": "localidade",
            "D2C": "ano_cod",
            "D2N": "ano",
            "D3C": "variavel_cod",
            "D3N": "variavel_nome",
            "D4C": "produto_cod",
            "D4N": "produto_raw",
        },
    )

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    if "localidade_cod" in df.columns:
        df["localidade_cod"] = pd.to_numeric(df["localidade_cod"], errors="coerce").astype("Int64")

    df["produto"] = produto_lower
    df["unidade"] = client.UNIDADES_EXTRACAO_VEGETAL.get(produto_lower, "")
    df["fonte"] = "ibge_extracao_vegetal"

    output_cols = [
        c
        for c in ["ano", "localidade", "localidade_cod", "produto", "valor", "unidade", "fonte"]
        if c in df.columns
    ]
    df = df[output_cols].reset_index(drop=True)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:extracao_vegetal",
        {"produto": produto, "ano": ano, "variavel": variavel},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry("ibge_extracao_vegetal")

    logger.info("ibge_extracao_vegetal_success", produto=produto, records=len(df))

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def produtos_extracao_vegetal() -> list[str]:
    return list(client.PRODUTOS_EXTRACAO_VEGETAL.keys())


# ---------------------------------------------------------------------------
# Leite Trimestral — tab 1086
# ---------------------------------------------------------------------------


@overload
async def leite_trimestral(
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def leite_trimestral(
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def leite_trimestral(
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_leite_trimestral",
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_leite_trimestral"],
        selected_source="ibge_leite_trimestral",
    )
    logger.info(
        "ibge_leite_trimestral_request",
        trimestre=trimestre,
        uf=uf,
    )

    table_code = client.TABELAS_LEITE["leite_trimestral"]
    var_codes = list(client.VARIAVEIS_LEITE.values())

    territorial_level = "3"
    ibge_code = "all"
    if uf:
        ibge_code = client.uf_to_ibge_code(uf)

    period = resolve_period(trimestre)

    df = await client.fetch_sidra(
        table_code=table_code,
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=var_codes,
        period=period,
    )

    df = client.parse_sidra_response(
        df,
        rename_columns={
            "MC": "unidade_cod",
            "MN": "unidade_medida",
            "D1C": "localidade_cod",
            "D1N": "localidade",
            "D2C": "trimestre_cod",
            "D2N": "trimestre_nome",
            "D3C": "variavel_cod",
            "D3N": "variavel_nome",
        },
    )

    if "trimestre_cod" in df.columns:
        df["trimestre"] = df["trimestre_cod"].astype(str)

    if "localidade_cod" in df.columns:
        df["localidade_cod"] = pd.to_numeric(df["localidade_cod"], errors="coerce").astype("Int64")

    var_name_map = {v: k for k, v in client.VARIAVEIS_LEITE.items()}
    merge_keys = [c for c in ["trimestre", "localidade", "localidade_cod"] if c in df.columns]

    pivot_frames = []
    for var_code, col_name in var_name_map.items():
        subset = df[df["variavel_cod"].astype(str) == var_code].copy()
        if subset.empty:
            continue
        subset = subset.rename(columns={"valor": col_name})
        subset = subset[merge_keys + [col_name]]
        pivot_frames.append(subset)

    if pivot_frames:
        result = pivot_frames[0]
        for pf in pivot_frames[1:]:
            result = result.merge(pf, on=merge_keys, how="outer")
    else:
        result = pd.DataFrame()

    if not result.empty:
        result["fonte"] = "ibge_leite_trimestral"

        for col in ["leite_adquirido", "leite_industrializado", "preco_medio"]:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce")

        output_cols = [
            c
            for c in [
                "trimestre",
                "localidade",
                "localidade_cod",
                "leite_adquirido",
                "leite_industrializado",
                "preco_medio",
                "fonte",
            ]
            if c in result.columns
        ]
        result = result[output_cols].reset_index(drop=True)

    df = result

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:leite_trimestral",
        {"trimestre": trimestre},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry("ibge_leite_trimestral")

    logger.info("ibge_leite_trimestral_success", records=len(df))

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


# ---------------------------------------------------------------------------
# PIB Agro — tab 1846 (corrente) / 6612 (real)
# ---------------------------------------------------------------------------


@overload
async def pib_agro(
    trimestre: str | list[str] | None = None,
    precos: str = "corrente",
    setor: str = "agropecuaria",
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def pib_agro(
    trimestre: str | list[str] | None = None,
    precos: str = "corrente",
    setor: str = "agropecuaria",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def pib_agro(
    trimestre: str | list[str] | None = None,
    precos: str = "corrente",
    setor: str = "agropecuaria",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_pib",
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_pib"],
        selected_source="ibge_pib",
    )
    logger.info(
        "ibge_pib_agro_request",
        trimestre=trimestre,
        precos=precos,
        setor=setor,
    )

    precos_lower = precos.lower()
    setor_lower = setor.lower()

    if precos_lower not in client.VARIAVEIS_PIB:
        raise InvalidParameterError(
            f"Tipo de preços não suportado: {precos}. "
            f"Disponíveis: {list(client.VARIAVEIS_PIB.keys())}"
        )

    if setor_lower not in client.SETORES_PIB:
        raise InvalidParameterError(
            f"Setor não suportado: {setor}. Disponíveis: {list(client.SETORES_PIB.keys())}"
        )

    if precos_lower == "corrente":
        table_code = client.TABELAS_PIB["pib_corrente"]
    else:
        table_code = client.TABELAS_PIB["pib_real"]

    var_code = client.VARIAVEIS_PIB[precos_lower]
    setor_cod = client.SETORES_PIB[setor_lower]

    period = resolve_period(trimestre)

    classifications: dict[str, str | list[str]] = {"11255": setor_cod}

    df = await client.fetch_sidra(
        table_code=table_code,
        territorial_level="1",
        ibge_territorial_code="all",
        variable=var_code,
        period=period,
        classifications=classifications,
    )

    df = client.parse_sidra_response(
        df,
        rename_columns={
            "MC": "unidade_cod",
            "MN": "unidade_medida",
            "D1C": "localidade_cod",
            "D1N": "localidade",
            "D2C": "trimestre_cod",
            "D2N": "trimestre_nome",
            "D3C": "variavel_cod",
            "D3N": "variavel_nome",
            "D4C": "setor_cod",
            "D4N": "setor_raw",
        },
    )

    if "trimestre_cod" in df.columns:
        df["trimestre"] = df["trimestre_cod"].astype(str)

    if "unidade_medida" in df.columns:
        df["unidade"] = df["unidade_medida"]
    else:
        df["unidade"] = "R$ (milhões)" if precos_lower == "corrente" else "R$ de 1995 (milhões)"

    df["setor"] = setor_lower
    df["fonte"] = "ibge_pib"

    output_cols = [
        c for c in ["trimestre", "valor", "unidade", "setor", "fonte"] if c in df.columns
    ]
    df = df[output_cols].reset_index(drop=True)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:pib_agro",
        {"trimestre": trimestre, "precos": precos, "setor": setor},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry("ibge_pib")

    logger.info("ibge_pib_agro_success", records=len(df))

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
