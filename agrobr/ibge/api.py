from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from datetime import date
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

_LSPA_ALIASES: dict[str, list[str]] = {
    "milho": ["milho_1", "milho_2"],
    "feijao": ["feijao_1", "feijao_2", "feijao_3"],
    "amendoim": ["amendoim_1", "amendoim_2"],
    "batata": ["batata_1", "batata_2"],
}

_PAM_COLUMNS = [
    "ano",
    "localidade",
    "produto",
    "area_plantada",
    "area_colhida",
    "producao",
    "rendimento",
    "valor_producao",
    "fonte",
]


def _validate_pam_years(
    ano: int | float | str | Sequence[int | float | str] | None,
) -> int | list[int] | None:
    if ano is None:
        return None
    if isinstance(ano, Sequence) and not isinstance(ano, str):
        values = ano
        is_sequence = True
    else:
        values = [ano]
        is_sequence = False
    current_year = date.today().year
    years: list[int] = []
    for value in values:
        if isinstance(value, bool) or (isinstance(value, float) and not value.is_integer()):
            raise InvalidParameterError("ano deve conter anos inteiros")
        try:
            years.append(int(value))
        except (TypeError, ValueError, OverflowError) as exc:
            raise InvalidParameterError("ano deve conter anos inteiros") from exc
    if any(year < 1974 or year > current_year for year in years):
        raise InvalidParameterError(f"ano deve estar entre 1974 e {current_year}")
    return years if is_sequence else years[0]


def _expand_lspa_produto(produto: str) -> list[tuple[str, str]]:
    if produto in client.PRODUTOS_LSPA:
        return [(produto, client.PRODUTOS_LSPA[produto])]

    if produto in _LSPA_ALIASES:
        return [(sub, client.PRODUTOS_LSPA[sub]) for sub in _LSPA_ALIASES[produto]]

    all_valid = sorted(set(list(client.PRODUTOS_LSPA.keys()) + list(_LSPA_ALIASES.keys())))
    raise InvalidParameterError(f"Produto não suportado: {produto}. Disponíveis: {all_valid}")


@overload
async def pam(
    produto: str,
    ano: int | float | str | Sequence[int | float | str] | None = None,
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
    ano: int | float | str | Sequence[int | float | str] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def pam(
    produto: str,
    ano: int | float | str | Sequence[int | float | str] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    normalized_ano = _validate_pam_years(ano)
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_pam",
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_pam"],
        selected_source="ibge_pam",
    )
    logger.info(
        "ibge_pam_request",
        produto=produto,
        ano=normalized_ano,
        uf=uf,
        nivel=nivel,
    )

    produto_lower = produto.lower()
    if produto_lower not in client.PRODUTOS_PAM:
        raise InvalidParameterError(
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

    territorial_level, ibge_code = resolve_ibge_code(uf, nivel)
    period = resolve_period(normalized_ano)

    df = await client.fetch_sidra(
        table_code=client.TABELAS["pam_nova"],
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=",".join(var_codes) if var_codes else None,
        period=period,
        classifications={"782": produto_cod},
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
            "D3N": "variavel",
            "D4C": "produto_cod",
            "D4N": "produto_raw",
        },
    )

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    if "variavel" in df.columns and "valor" in df.columns:
        df_pivot = df.pivot_table(
            index=["localidade", "ano"] if "localidade" in df.columns else ["ano"],
            columns="variavel",
            values="valor",
            aggfunc="first",
        ).reset_index()

        rename_map = {
            "Área plantada": "area_plantada",
            "Área plantada ou destinada à colheita": "area_plantada",
            "Área colhida": "area_colhida",
            "Quantidade produzida": "producao",
            "Rendimento médio da produção": "rendimento",
            "Valor da produção": "valor_producao",
        }
        df_pivot = df_pivot.rename(columns=rename_map)
        df = df_pivot

    df["produto"] = produto_lower
    df["fonte"] = "ibge_pam"
    if df.empty:
        df = pd.DataFrame(columns=_PAM_COLUMNS)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:pam",
        {"produto": produto, "ano": normalized_ano},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry("ibge_pam")

    logger.info(
        "ibge_pam_success",
        produto=produto,
        records=len(df),
    )

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


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
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_lspa",
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_lspa"],
        selected_source="ibge_lspa",
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

    async def _fetch_sub(sub_nome: str, sub_cod: str) -> pd.DataFrame:
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
        return sub_df

    results = await asyncio.gather(*[_fetch_sub(n, c) for n, c in sub_produtos])
    frames = [df for df in results if not df.empty]

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:lspa",
        {"produto": produto, "ano": ano, "mes": mes},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry("ibge_lspa")

    logger.info(
        "ibge_lspa_success",
        produto=produto,
        records=len(df),
    )

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def produtos_pam() -> list[str]:
    return list(client.PRODUTOS_PAM.keys())


async def produtos_lspa() -> list[str]:
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
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_ppm"],
        selected_source="ibge_ppm",
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
        raise InvalidParameterError(
            f"Espécie/produto não suportado: {especie}. Disponíveis: {all_valid}"
        )

    territorial_level, ibge_code = resolve_ibge_code(uf, nivel)
    period = resolve_period(ano)

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
            "D3N": "variavel",
            "D4C": "especie_cod",
            "D4N": "especie_raw",
        },
    )

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
    meta.cache_expires_at = calculate_expiry("ibge_ppm")

    logger.info(
        "ibge_ppm_success",
        especie=especie,
        records=len(df),
    )

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def _detect_abate_columns(df: pd.DataFrame) -> dict[str, str]:
    col_map = {
        "NC": "nivel_cod",
        "NN": "nivel",
        "MC": "unidade_cod",
        "MN": "unidade",
        "V": "valor",
        "D1C": "localidade_cod",
        "D1N": "localidade",
    }

    var_ids = {"284", "285", "1000284", "1000285", "151", "1000151"}
    for dc in ["D2C", "D3C"]:
        if dc not in df.columns or len(df) == 0:
            continue
        sample_str = str(df[dc].iloc[0])
        name_col = dc[:-1] + "N"
        if sample_str in var_ids:
            col_map[dc] = "variavel_cod"
            col_map[name_col] = "variavel_nome"
        elif len(sample_str) == 6 and sample_str[:4].isdigit():
            col_map[dc] = "trimestre_cod"
            col_map[name_col] = "trimestre_nome"

    return {k: v for k, v in col_map.items() if k in df.columns}


def _merge_cabecas_peso(df: pd.DataFrame, especie_lower: str) -> pd.DataFrame:
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
        return pd.DataFrame()

    if "localidade_cod" in result.columns:
        result["localidade_cod"] = pd.to_numeric(result["localidade_cod"], errors="coerce").astype(
            "Int64"
        )

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

    return result


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
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_abate"],
        selected_source="ibge_abate",
    )
    logger.info(
        "ibge_abate_request",
        especie=especie,
        trimestre=trimestre,
        uf=uf,
    )

    especie_lower = especie.lower()
    if especie_lower not in client.ESPECIES_ABATE:
        raise InvalidParameterError(
            f"Espécie não suportada: {especie}. Disponíveis: {client.ESPECIES_ABATE}"
        )

    table_code = client.TABELAS_ABATE[especie_lower]
    var_codes = ",".join(client.VARIAVEIS_ABATE.values())

    territorial_level = "3"
    ibge_code = "all"
    if uf:
        ibge_code = client.uf_to_ibge_code(uf)

    period = resolve_period(trimestre)

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

    rename_map = _detect_abate_columns(df)
    df = df.rename(columns=rename_map)

    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    if "trimestre_cod" in df.columns:
        df["trimestre"] = df["trimestre_cod"].astype(str)

    df = _merge_cabecas_peso(df, especie_lower)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:abate",
        {"especie": especie, "trimestre": trimestre},
        schema_version=meta.schema_version,
    )
    meta.cache_expires_at = calculate_expiry("ibge_abate")

    logger.info(
        "ibge_abate_success",
        especie=especie,
        records=len(df),
    )

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def especies_abate() -> list[str]:
    return list(client.ESPECIES_ABATE)


async def especies_ppm() -> list[str]:
    return sorted(list(client.REBANHOS_PPM.keys()) + list(client.PRODUTOS_ORIGEM_ANIMAL.keys()))


async def ufs() -> list[str]:
    return list(client.get_uf_codes().keys())
