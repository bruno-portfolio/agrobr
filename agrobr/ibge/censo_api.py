from __future__ import annotations

import time
from typing import Literal, overload

import pandas as pd

from agrobr import _log, constants
from agrobr.cache.keys import build_cache_key
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.ibge import client
from agrobr.ibge._helpers import NIVEL_MAP_HISTORICO, SIDRA_BASE, registrar_canal, resolve_ibge_code
from agrobr.ibge.censo_tables import (
    _CENSO_ALL_VAR_IDS,
    _CENSO_CATEGORIA_COL_INDEX,
    _CENSO_MULTI_TABLE,
    _CENSO_VAR_NOME,
    _CENSO_VAR_UNIDADE,
    _CLASSIFICACOES_CENSO_AGRO,
    _VAR_AS_CATEGORIA,
)
from agrobr.models import MetaInfo
from agrobr.utils import tasks
from agrobr.utils.result import finalize_result
from agrobr.utils.time import utcnow

logger = _log.get_logger(__name__)


def _empty_censo_df() -> pd.DataFrame:
    return pd.DataFrame(
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


def _validate_censo_keys(df: pd.DataFrame, source: str, parser_version: int) -> None:
    keys = ["ano", "localidade_cod", "tema", "categoria", "variavel"]
    if not df.empty and df.duplicated(keys).any():
        raise ParseError(
            source=source,
            parser_version=parser_version,
            reason="Observações censitárias duplicadas por ano, localidade, tema, categoria e variável",
        )


def _parse_censo_raw(
    df: pd.DataFrame,
    tema: str,
    ano_key: str,
    var_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    key = (tema, ano_key)

    vac = _VAR_AS_CATEGORIA.get(key)
    if vac:
        return _process_var_as_categoria(df, tema, ano_key, vac)

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

    cat_idx = _CENSO_CATEGORIA_COL_INDEX.get(key)
    if cat_idx:
        cat_col_c = f"D{cat_idx}C"
        cat_col_n = f"D{cat_idx}N"
        if cat_col_c in col_map:
            for i in range(2, 7):
                alt_c = f"D{i}C"
                if alt_c in df.columns and alt_c not in col_map:
                    cat_col_c = alt_c
                    cat_col_n = f"D{i}N"
                    break
        if cat_col_c in df.columns and cat_col_c not in col_map:
            col_map[cat_col_c] = "categoria_cod"
        if cat_col_n in df.columns and cat_col_n not in col_map:
            col_map[cat_col_n] = "categoria"

    rename_map = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"].replace("-", "0"), errors="coerce").astype(
            "float64"
        )

    if "ano_cod" in df.columns:
        df["ano"] = pd.to_numeric(df["ano_cod"], errors="coerce").astype("Int64")
    else:
        df["ano"] = int(ano_key)

    if "variavel_cod" in df.columns:
        df["variavel"] = df["variavel_cod"].map(_CENSO_VAR_NOME).fillna(df.get("variavel_nome", ""))
        unidade_from_map = df["variavel_cod"].map(_CENSO_VAR_UNIDADE)
        if "MN" in df.columns:
            df["unidade"] = unidade_from_map.where(unidade_from_map != "", df["MN"])
        else:
            df["unidade"] = unidade_from_map.fillna("")
    else:
        effective_var_map = var_map or client.VARIAVEIS_CENSO_AGRO.get(tema, {}).get(ano_key, {})
        if len(effective_var_map) == 1:
            var_name = next(iter(effective_var_map))
            var_code = next(iter(effective_var_map.values()))
            df["variavel"] = var_name
            df["unidade"] = _CENSO_VAR_UNIDADE.get(var_code, "")
        else:
            df["variavel"] = ""
            df["unidade"] = ""

    if "localidade_cod" in df.columns:
        df["localidade_cod"] = pd.to_numeric(df["localidade_cod"], errors="coerce").astype("Int64")

    df["tema"] = tema
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
    return df[output_cols].reset_index(drop=True)


async def _fetch_censo_multi_table(
    tema: str,
    ano_key: str,
    table_specs: list[tuple[str, dict[str, str]]],
    territorial_level: str,
    ibge_code: str,
    meta: MetaInfo | None = None,
) -> pd.DataFrame:
    key = (tema, ano_key)
    classifications: dict[str, str | list[str]] = dict(_CLASSIFICACOES_CENSO_AGRO.get(key, {}))

    async def _fetch_one_table(tc: str, vm: dict[str, str]) -> pd.DataFrame:
        var_codes = ",".join(vm.values())
        df = await client.fetch_sidra(
            table_code=tc,
            territorial_level=territorial_level,
            ibge_territorial_code=ibge_code,
            variable=var_codes,
            period="all",
            classifications=classifications,
        )
        registrar_canal(meta, df)
        if df.empty:
            return pd.DataFrame()
        return _parse_censo_raw(df, tema, ano_key, var_map=vm)

    results = await tasks.gather_or_cancel(*[_fetch_one_table(tc, vm) for tc, vm in table_specs])
    frames = [df for df in results if not df.empty]

    if not frames:
        return _empty_censo_df()
    return pd.concat(frames, ignore_index=True)


async def _fetch_censo_single(
    tema: str,
    ano_key: str,
    territorial_level: str,
    ibge_code: str,
    meta: MetaInfo | None = None,
) -> pd.DataFrame:
    key = (tema, ano_key)

    multi_spec = _CENSO_MULTI_TABLE.get(key)
    if multi_spec:
        return await _fetch_censo_multi_table(
            tema, ano_key, multi_spec, territorial_level, ibge_code, meta
        )

    table_code = client.TABELAS_CENSO_AGRO[tema][ano_key]
    var_map = client.VARIAVEIS_CENSO_AGRO[tema][ano_key]
    var_codes = ",".join(var_map.values())
    classifications: dict[str, str | list[str]] = dict(_CLASSIFICACOES_CENSO_AGRO.get(key, {}))

    df = await client.fetch_sidra(
        table_code=table_code,
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=var_codes,
        period="all",
        classifications=classifications,
    )
    registrar_canal(meta, df)

    if df.empty:
        return _empty_censo_df()

    return _parse_censo_raw(df, tema, ano_key)


def _process_var_as_categoria(
    df: pd.DataFrame,
    tema: str,
    ano_key: str,
    vac: dict[str, tuple[str, str, str]],
) -> pd.DataFrame:
    col_map: dict[str, str] = {
        "NC": "nivel_cod",
        "NN": "nivel",
        "V": "valor",
        "D1C": "localidade_cod",
        "D1N": "localidade",
    }

    var_col = None
    for dc in ["D2C", "D3C"]:
        if dc not in df.columns or len(df) == 0:
            continue
        sample = str(df[dc].iloc[0])
        name_col = dc[:-1] + "N"
        if sample in vac:
            var_col = dc
            col_map[dc] = "variavel_cod"
            col_map[name_col] = "variavel_nome"
        elif len(sample) == 4 and sample.isdigit():
            col_map[dc] = "ano_cod"
            col_map[name_col] = "ano_nome"

    if var_col is None:
        for dc in ["D2C", "D3C"]:
            if dc not in df.columns:
                continue
            if df[dc].iloc[0] in vac:
                var_col = dc
                name_col = dc[:-1] + "N"
                col_map[dc] = "variavel_cod"
                col_map[name_col] = "variavel_nome"
                break

    rename_map = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"].replace("-", "0"), errors="coerce").astype(
            "float64"
        )

    if "ano_cod" in df.columns:
        df["ano"] = pd.to_numeric(df["ano_cod"], errors="coerce").astype("Int64")
    else:
        df["ano"] = int(ano_key)

    if "variavel_cod" in df.columns:
        df["categoria"] = df["variavel_cod"].map(lambda v: vac.get(str(v), ("", "", ""))[0])
        df["variavel"] = df["variavel_cod"].map(lambda v: vac.get(str(v), ("", "", ""))[1])
        df["unidade"] = df["variavel_cod"].map(lambda v: vac.get(str(v), ("", "", ""))[2])
        df = df[df["categoria"] != ""]
    else:
        df["categoria"] = ""
        df["variavel"] = ""
        df["unidade"] = ""

    if "localidade_cod" in df.columns:
        df["localidade_cod"] = pd.to_numeric(df["localidade_cod"], errors="coerce").astype("Int64")

    df["tema"] = tema
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
    return df[output_cols].reset_index(drop=True)


@overload
async def censo_agro(
    tema: str,
    ano: int | str | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def censo_agro(
    tema: str,
    ano: int | str | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def censo_agro(
    tema: str,
    ano: int | str | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_censo_agro",
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_censo_agro"],
        selected_source="ibge_censo_agro",
        parser_version=constants.IBGE_CENSO_PARSER_VERSION,
    )
    logger.info(
        "ibge_censo_agro_request",
        tema=tema,
        ano=ano,
        uf=uf,
        nivel=nivel,
    )

    tema_lower = tema.lower()
    if tema_lower not in client.TABELAS_CENSO_AGRO:
        raise InvalidParameterError(
            f"Tema não suportado: {tema}. Disponíveis: {client.TEMAS_CENSO_AGRO}"
        )

    anos_disponiveis = list(client.TABELAS_CENSO_AGRO[tema_lower].keys())

    if ano is not None:
        ano_str = str(ano)
        if ano_str not in anos_disponiveis:
            raise InvalidParameterError(
                f"Ano {ano} não disponível para tema '{tema_lower}'. Disponíveis: {anos_disponiveis}"
            )
        anos_fetch = [ano_str]
    else:
        anos_fetch = anos_disponiveis

    territorial_level, ibge_code = resolve_ibge_code(uf, nivel)

    results = await tasks.gather_or_cancel(
        *[
            _fetch_censo_single(tema_lower, ak, territorial_level, ibge_code, meta)
            for ak in anos_fetch
        ]
    )
    frames = [df for df in results if not df.empty]

    df = pd.concat(frames, ignore_index=True) if frames else _empty_censo_df()
    _validate_censo_keys(df, "ibge_censo_agro", constants.IBGE_CENSO_PARSER_VERSION)

    if not df.empty and "ano" in df.columns and "localidade" in df.columns:
        df = df.sort_values(["ano", "localidade"]).reset_index(drop=True)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:censo_agro",
        {"tema": tema, "ano": ano, "uf": uf},
        schema_version=meta.schema_version,
    )

    logger.info(
        "ibge_censo_agro_success",
        tema=tema,
        records=len(df),
    )

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def temas_censo_agro() -> list[str]:
    return list(client.TEMAS_CENSO_AGRO)


def _parse_censo_historico_raw(df: pd.DataFrame, tema: str) -> pd.DataFrame:
    periodos_set = {str(p) for p in client.PERIODOS_CENSO_HISTORICO[tema]}
    var_codes_set = set(client.VARIAVEIS_CENSO_HISTORICO[tema].values())
    reverse_vars = {v: k for k, v in client.VARIAVEIS_CENSO_HISTORICO[tema].items()}
    categorias = client.CATEGORIAS_CENSO_HISTORICO.get(tema, {})
    has_classification = bool(client.CLASSIFICACOES_CENSO_HISTORICO[tema])

    ano_dc = var_dc = cat_dc = None
    for dc in ["D2C", "D3C", "D4C"]:
        if dc not in df.columns:
            continue
        sample = str(df[dc].iloc[0])
        if ano_dc is None and len(sample) == 4 and sample.isdigit() and sample in periodos_set:
            ano_dc = dc
        elif var_dc is None and sample in var_codes_set:
            var_dc = dc
        elif cat_dc is None:
            cat_dc = dc

    result = pd.DataFrame()
    result["localidade"] = df["D1N"].values
    result["localidade_cod"] = pd.to_numeric(df["D1C"], errors="coerce").astype("Int64")

    if ano_dc:
        result["ano"] = pd.to_numeric(df[ano_dc], errors="coerce").astype("Int64")

    if var_dc:
        vc = df[var_dc].astype(str)
        result["variavel"] = vc.map(reverse_vars).fillna(vc)
    elif len(var_codes_set) == 1:
        code = next(iter(var_codes_set))
        result["variavel"] = reverse_vars.get(code, code)
        vc = pd.Series(code, index=df.index)
    else:
        result["variavel"] = ""
        vc = pd.Series("", index=df.index)

    if cat_dc:
        cc = df[cat_dc].astype(str)
        cat_name_col = cat_dc[:-1] + "N"
        mapped = cc.map(categorias)
        if cat_name_col in df.columns:
            result["categoria"] = mapped.fillna(df[cat_name_col])
        else:
            result["categoria"] = mapped.fillna("")
    elif not has_classification:
        result["categoria"] = "total"
        cc = pd.Series("", index=df.index)
    else:
        result["categoria"] = ""
        cc = pd.Series("", index=df.index)

    fixed_unit = vc.map(client.UNIDADES_VARIAVEIS_CENSO_HISTORICO)
    if cat_dc:
        cat_unit = cc.map(client.UNIDADES_CATEGORIAS_CENSO_HISTORICO)
    else:
        cat_unit = pd.Series(pd.NA, index=df.index)
    mn_unit = df["MN"] if "MN" in df.columns else pd.Series("", index=df.index)
    result["unidade"] = fixed_unit.fillna(cat_unit).fillna(mn_unit).fillna("")

    result["valor"] = pd.to_numeric(df["V"].replace("-", "0"), errors="coerce").astype("float64")
    result["tema"] = tema
    result["fonte"] = "ibge_censo_agro_historico"

    output_cols = [
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
    return result[output_cols].reset_index(drop=True)


@overload
async def censo_agro_historico(
    tema: str,
    ano: int | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "regiao", "uf"] = "uf",
    as_polars: bool = False,
    *,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def censo_agro_historico(
    tema: str,
    ano: int | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "regiao", "uf"] = "uf",
    as_polars: bool = False,
    *,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def censo_agro_historico(
    tema: str,
    ano: int | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "regiao", "uf"] = "uf",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_censo_agro_historico",
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_censo_agro_historico"],
        selected_source="ibge_censo_agro_historico",
        parser_version=constants.IBGE_CENSO_HISTORICO_PARSER_VERSION,
    )
    logger.info(
        "ibge_censo_agro_historico_request",
        tema=tema,
        ano=ano,
        uf=uf,
        nivel=nivel,
    )

    tema_lower = tema.lower()
    if tema_lower not in client.TABELAS_CENSO_HISTORICO:
        raise InvalidParameterError(
            f"Tema não suportado: {tema}. Disponíveis: {client.TEMAS_CENSO_HISTORICO}"
        )

    niveis_validos = client.NIVEIS_CENSO_HISTORICO[tema_lower]
    if nivel not in niveis_validos:
        raise InvalidParameterError(
            f"Nível '{nivel}' não disponível para tema '{tema_lower}'. "
            f"Disponíveis: {niveis_validos}"
        )

    periodos = client.PERIODOS_CENSO_HISTORICO[tema_lower]
    if ano is None:
        anos = periodos
    elif isinstance(ano, int):
        if ano not in periodos:
            raise InvalidParameterError(
                f"Ano {ano} não disponível para tema '{tema_lower}'. Disponíveis: {periodos}"
            )
        anos = [ano]
    else:
        for a in ano:
            if a not in periodos:
                raise InvalidParameterError(
                    f"Ano {a} não disponível para tema '{tema_lower}'. Disponíveis: {periodos}"
                )
        anos = ano

    territorial_level, ibge_code = resolve_ibge_code(uf, nivel, nivel_map=NIVEL_MAP_HISTORICO)

    period = ",".join(str(a) for a in anos)
    variable = ",".join(client.VARIAVEIS_CENSO_HISTORICO[tema_lower].values())
    classifications: dict[str, str | list[str]] | None = (
        dict(client.CLASSIFICACOES_CENSO_HISTORICO[tema_lower]) or None
    )

    df = await client.fetch_sidra(
        table_code=client.TABELAS_CENSO_HISTORICO[tema_lower],
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=variable,
        period=period,
        classifications=classifications,
    )
    registrar_canal(meta, df)

    df = _empty_censo_df() if df.empty else _parse_censo_historico_raw(df, tema_lower)
    _validate_censo_keys(
        df, "ibge_censo_agro_historico", constants.IBGE_CENSO_HISTORICO_PARSER_VERSION
    )

    if not df.empty and "ano" in df.columns and "localidade" in df.columns:
        df = df.sort_values(["ano", "localidade"]).reset_index(drop=True)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:censo_agro_historico",
        {"tema": tema, "ano": ano, "uf": uf},
        schema_version=meta.schema_version,
    )

    logger.info(
        "ibge_censo_agro_historico_success",
        tema=tema,
        records=len(df),
    )

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def temas_censo_agro_historico() -> list[str]:
    return list(client.TEMAS_CENSO_HISTORICO)
