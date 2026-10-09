from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Literal, overload

import pandas as pd

from agrobr import _log, constants, contracts
from agrobr.cache.keys import build_cache_key
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.ibge import client, lspa_parser, pam_parser
from agrobr.ibge._helpers import (
    SIDRA_BASE,
    _validate_years,
    normalizar_opcao,
    registrar_canal,
    resolve_ibge_code,
    resolve_period,
    resolve_quarter_period,
    tipar_resultado,
)
from agrobr.models import MetaInfo
from agrobr.utils import tasks
from agrobr.utils.result import DataFrame, DataFrameResult, finalize_result
from agrobr.utils.time import hoje, utcnow
from agrobr.utils.warnings import warn_once

logger = _log.get_logger(__name__)

_LSPA_ALIASES: dict[str, list[str]] = {
    "cafe": ["cafe_arabica", "cafe_canephora"],
    "milho": ["milho_1", "milho_2"],
    "feijao": ["feijao_1", "feijao_2", "feijao_3"],
    "amendoim": ["amendoim_1", "amendoim_2"],
    "batata": ["batata_1", "batata_2", "batata_3"],
}

_PPM_ALIASES: dict[str, tuple[str, str]] = {
    "galinhas_poedeiras": (
        "galinhas",
        "a categoria do IBGE é 'Galináceos - galinhas', que inclui poedeiras e matrizeiras",
    ),
}

_PAM_COLUMNS = [
    "ano",
    "localidade",
    "localidade_cod",
    "produto",
    "area_plantada",
    "area_colhida",
    "producao",
    "rendimento",
    "valor_producao",
    "fonte",
    "unidade_producao",
    "unidade_rendimento",
    "unidade_valor_producao",
    "condicao_produto",
]

_ABATE_COLUMNS = [
    "trimestre",
    "localidade",
    "localidade_cod",
    "especie",
    "categoria",
    "animais_abatidos",
    "peso_carcacas",
    "fonte",
]


def _expand_lspa_produto(produto: str, ano: int | str | None = None) -> list[tuple[str, str]]:
    if produto == "cafe" and ano is not None and int(ano) < client.LSPA_CAFE_ESPECIES_ANO_INICIAL:
        return [(produto, client.LSPA_CAFE_TOTAL_COD)]
    if produto in client.PRODUTOS_LSPA:
        return [(produto, client.PRODUTOS_LSPA[produto])]

    if produto in _LSPA_ALIASES:
        return [(sub, client.PRODUTOS_LSPA[sub]) for sub in _LSPA_ALIASES[produto]]

    all_valid = sorted(set(list(client.PRODUTOS_LSPA.keys()) + list(_LSPA_ALIASES.keys())))
    raise InvalidParameterError(f"Produto não suportado: {produto}. Disponíveis: {all_valid}")


def _resolve_lspa_period(ano: int | str, mes: int | str | None) -> str:
    year = _validate_years(ano)
    if not isinstance(year, int):
        raise InvalidParameterError("ano deve ser um único ano inteiro")
    if mes is None:
        return ",".join(f"{year}{month:02d}" for month in range(1, 13))
    if isinstance(mes, bool) or not isinstance(mes, (int, str)):
        raise InvalidParameterError("mes deve ser um inteiro entre 1 e 12")
    try:
        month = int(mes)
    except ValueError as exc:
        raise InvalidParameterError("mes deve ser um inteiro entre 1 e 12") from exc
    if not 1 <= month <= 12:
        raise InvalidParameterError("mes deve ser um inteiro entre 1 e 12")
    return f"{year}{month:02d}"


@overload
async def pam(
    produto: str,
    ano: int | float | str | Sequence[int | float | str] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def pam(
    produto: str,
    ano: int | float | str | Sequence[int | float | str] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> DataFrame: ...


@overload
async def pam(
    produto: str,
    ano: int | float | str | Sequence[int | float | str] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def pam(
    produto: str,
    ano: int | float | str | Sequence[int | float | str] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[DataFrame, MetaInfo]: ...


async def pam(
    produto: str,
    ano: int | float | str | Sequence[int | float | str] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    """Produção agrícola municipal; área plantada não está disponível antes de 1988.

    O dataset ``producao_anual`` representa essa ausência histórica como NA,
    assim como o valor de produção quando não solicitado à fonte.
    Valores mantêm a escala publicada; as colunas ``unidade_*`` identificam
    as unidades por ano. Laranja anterior a 2001 usa mil frutos/frutos por ha;
    moedas anteriores a 1994 não são reais. ``condicao_produto`` distingue
    café em coco (até 2001) de beneficiado (desde 2002), sem conversão implícita.
    """
    normalized_ano = _validate_years(ano)
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_pam",
        parser_version=constants.IBGE_PAM_PARSER_VERSION,
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_pam"],
        selected_source="ibge_pam",
        schema_version=contracts.get_contract("producao_anual").version,
    )
    logger.info(
        "ibge_pam_request",
        produto=produto,
        ano=normalized_ano,
        uf=uf,
        nivel=nivel,
    )

    produto_lower = normalizar_opcao(produto, "Produto", client.PRODUTOS_PAM)

    produto_cod = client.PRODUTOS_PAM[produto_lower]

    if variaveis is None:
        variaveis = ["area_plantada", "area_colhida", "producao", "rendimento"]
    if not isinstance(variaveis, list) or not variaveis:
        raise InvalidParameterError(
            f"variaveis deve ser uma lista não vazia. Disponíveis: {list(client.VARIAVEIS)}"
        )
    var_codes = [
        client.VARIAVEIS[normalizar_opcao(var, "Variável", client.VARIAVEIS)] for var in variaveis
    ]

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
    registrar_canal(meta, df)

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
        df = pam_parser.pivot_observations(df)
    if "localidade_cod" in df.columns:
        df["localidade_cod"] = pd.to_numeric(df["localidade_cod"], errors="coerce").astype("Int64")

    df["produto"] = produto_lower
    df["fonte"] = "ibge_pam"
    if df.empty:
        df = pd.DataFrame(columns=_PAM_COLUMNS)
    else:
        df = pam_parser.add_unit_columns(df, produto_lower)
    df = tipar_resultado(df, "producao_anual", _PAM_COLUMNS)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:pam",
        {
            "produto": produto,
            "ano": normalized_ano,
            "territorial_level": territorial_level,
            "ibge_code": ibge_code,
            "variaveis": variaveis,
        },
        schema_version=meta.schema_version,
    )

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
    *,
    mes: int | str | None = None,
    uf: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def lspa(
    produto: str,
    ano: int | str | None = None,
    *,
    mes: int | str | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> DataFrame: ...


@overload
async def lspa(
    produto: str,
    ano: int | str | None = None,
    *,
    mes: int | str | None = None,
    uf: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def lspa(
    produto: str,
    ano: int | str | None = None,
    *,
    mes: int | str | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[DataFrame, MetaInfo]: ...


async def lspa(
    produto: str,
    ano: int | str | None = None,
    *,
    mes: int | str | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
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

    produto_lower = normalizar_opcao(produto, "Produto", [*client.PRODUTOS_LSPA, *_LSPA_ALIASES])
    if ano is None:
        ano = hoje().year

    period = _resolve_lspa_period(ano, mes)
    sub_produtos = _expand_lspa_produto(produto_lower, ano)
    territorial_level, ibge_code = resolve_ibge_code(uf, "uf" if uf is not None else "brasil")

    async def _fetch_sub(sub_nome: str, sub_cod: str) -> pd.DataFrame:
        sub_df = await client.fetch_sidra(
            table_code=client.TABELAS["lspa"],
            territorial_level=territorial_level,
            ibge_territorial_code=ibge_code,
            period=period,
            classifications={"48": sub_cod},
        )
        registrar_canal(meta, sub_df)
        return lspa_parser.parse_lspa(sub_df, sub_nome)

    results = await tasks.gather_or_cancel(*[_fetch_sub(n, c) for n, c in sub_produtos])
    frames = [df for df in results if not df.empty]

    df = (
        pd.concat(frames, ignore_index=True)
        if frames
        else lspa_parser.parse_lspa(pd.DataFrame(), produto_lower)
    )

    meta.parser_version = lspa_parser.PARSER_VERSION
    meta.dataset = "lspa"
    meta.contract_version = "2.0"
    meta.schema_version = "2.0"

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:lspa",
        {"produto": produto, "ano": ano, "mes": mes, "uf": uf},
        schema_version=meta.schema_version,
    )

    logger.info(
        "ibge_lspa_success",
        produto=produto,
        records=len(df),
    )

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def produtos_pam() -> list[str]:
    return list(client.PRODUTOS_PAM.keys())


async def produtos_lspa() -> list[str]:
    return list(client.PRODUTOS_LSPA) + list(_LSPA_ALIASES)


@overload
async def ppm(
    especie: str,
    ano: int | str | list[int] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ppm(
    especie: str,
    ano: int | str | list[int] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> DataFrame: ...


@overload
async def ppm(
    especie: str,
    ano: int | str | list[int] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def ppm(
    especie: str,
    ano: int | str | list[int] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[DataFrame, MetaInfo]: ...


async def ppm(
    especie: str,
    ano: int | str | list[int] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_ppm",
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_ppm"],
        selected_source="ibge_ppm",
        schema_version=contracts.get_contract("pecuaria_municipal").version,
    )
    logger.info(
        "ibge_ppm_request",
        especie=especie,
        ano=ano,
        uf=uf,
        nivel=nivel,
    )

    all_valid = sorted([*client.REBANHOS_PPM, *client.PRODUTOS_ORIGEM_ANIMAL, *_PPM_ALIASES])
    especie_lower = normalizar_opcao(especie, "Espécie/produto", all_valid)
    if especie_lower in _PPM_ALIASES:
        canonica, motivo = _PPM_ALIASES[especie_lower]
        warn_once(
            f"ibge_ppm_alias:{especie_lower}",
            f"especie='{especie_lower}' está depreciada: {motivo}. Use especie='{canonica}'",
            category=FutureWarning,
        )
        especie_lower = canonica
    is_rebanho = especie_lower in client.REBANHOS_PPM

    territorial_level, ibge_code = resolve_ibge_code(uf, nivel)
    period = resolve_period(_validate_years(ano))

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
    registrar_canal(meta, df)

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
    df = tipar_resultado(df, "pecuaria_municipal")

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:ppm",
        {
            "especie": especie,
            "ano": ano,
            "territorial_level": territorial_level,
            "ibge_code": ibge_code,
        },
        schema_version=meta.schema_version,
    )

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

    codigos_rebanho = set(client.CATEGORIAS_ABATE_BOVINO.values())
    for dc in (f"D{i}C" for i in range(4, 10)):
        if dc in df.columns and df[dc].astype(str).isin(codigos_rebanho).any():
            col_map[dc] = "categoria_cod"
            break

    return {k: v for k, v in col_map.items() if k in df.columns}


def _com_categoria(df: pd.DataFrame, categoria: str) -> pd.DataFrame:
    """A coluna da classificação 18 (tipo de rebanho) vira ``categoria``; sem ela na resposta (suíno,
    frango e o pedido de uma categoria só), toda linha é da categoria pedida."""
    df = df.copy()
    if "categoria_cod" not in df.columns:
        if categoria == "todas" and not df.empty:
            raise _erro_layout_abate("sem a classificação 18 (tipo de rebanho)")
        df["categoria"] = categoria
        return df
    rotulos = {codigo: nome for nome, codigo in client.CATEGORIAS_ABATE_BOVINO.items()}
    df["categoria"] = df["categoria_cod"].astype(str).map(rotulos)
    if df["categoria"].isna().any():
        raise _erro_layout_abate("com tipo de rebanho fora do pedido")
    return df


def _erro_layout_abate(motivo: str) -> ParseError:
    return ParseError(
        source="ibge_abate",
        parser_version=constants.IBGE_ABATE_PARSER_VERSION,
        reason=f"Resposta de abate {motivo}",
    )


def _merge_cabecas_peso(df: pd.DataFrame, especie_lower: str) -> pd.DataFrame:
    if df.empty:
        return tipar_resultado(df, "abate_trimestral", _ABATE_COLUMNS)
    if "variavel_cod" not in df.columns:
        raise _erro_layout_abate("sem coluna de variável reconhecível")
    cabecas = df[df["variavel_cod"].astype(str) == "284"].copy()
    peso = df[df["variavel_cod"].astype(str) == "285"].copy()

    merge_keys = [c for c in ["trimestre", "localidade", "localidade_cod"] if c in cabecas.columns]

    if not merge_keys:
        raise _erro_layout_abate("sem trimestre nem localidade")
    merge_keys.append("categoria")
    if cabecas.empty and peso.empty:
        raise _erro_layout_abate("sem as variáveis 284 e 285")
    if df.duplicated(merge_keys + ["variavel_cod"]).any():
        raise ParseError(
            source="ibge_abate",
            parser_version=constants.IBGE_ABATE_PARSER_VERSION,
            reason="Observação duplicada por trimestre, localidade e variável de abate",
        )
    cabecas = cabecas.rename(columns={"valor": "animais_abatidos"})
    peso = peso.rename(columns={"valor": "peso_carcacas"})
    result = cabecas[merge_keys + ["animais_abatidos"]].merge(
        peso[merge_keys + ["peso_carcacas"]],
        on=merge_keys,
        how="outer",
        validate="one_to_one",
    )

    if "localidade_cod" in result.columns:
        result["localidade_cod"] = pd.to_numeric(result["localidade_cod"], errors="coerce").astype(
            "Int64"
        )

    result["especie"] = especie_lower
    result["fonte"] = "ibge_abate"

    output_cols = [c for c in _ABATE_COLUMNS if c in result.columns]
    result = result[output_cols].reset_index(drop=True)

    for col in ["animais_abatidos", "peso_carcacas"]:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce").astype("float64")
    if result["animais_abatidos"].dropna().mod(1).ne(0).any():
        raise _erro_layout_abate("com quantidade fracionária de animais abatidos")

    return tipar_resultado(result, "abate_trimestral", _ABATE_COLUMNS)


@overload
async def abate(
    especie: str,
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    categoria: str = "total",
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def abate(
    especie: str,
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    categoria: str = "total",
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> DataFrame: ...


@overload
async def abate(
    especie: str,
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    categoria: str = "total",
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def abate(
    especie: str,
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    categoria: str = "total",
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[DataFrame, MetaInfo]: ...


async def abate(
    especie: str,
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    categoria: str = "total",
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    fetch_start = time.perf_counter()
    meta = MetaInfo(
        source="ibge_abate",
        parser_version=constants.IBGE_ABATE_PARSER_VERSION,
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=utcnow(),
        attempted_sources=["ibge_abate"],
        selected_source="ibge_abate",
        schema_version=contracts.get_contract("abate_trimestral").version,
        contract_version=contracts.get_contract("abate_trimestral").version,
    )
    logger.info(
        "ibge_abate_request",
        especie=especie,
        trimestre=trimestre,
        uf=uf,
        categoria=categoria,
    )

    especie_lower = normalizar_opcao(especie, "Espécie", client.ESPECIES_ABATE)
    categoria = normalizar_opcao(
        categoria, "Tipo de rebanho", [*client.CATEGORIAS_ABATE_BOVINO, "todas"]
    )
    if especie_lower != "bovino" and categoria != "total":
        raise InvalidParameterError(
            f"Categoria {categoria!r} só existe no abate bovino; para {especie_lower}, use 'total'"
        )

    table_code = client.TABELAS_ABATE[especie_lower]
    var_codes = ",".join(client.VARIAVEIS_ABATE.values())

    territorial_level, ibge_code = resolve_ibge_code(uf, "uf")

    period = resolve_quarter_period(trimestre)

    classifications: dict[str, str | list[str]] = {
        "12716": "115236",
        "12529": "118225",
    }
    if especie_lower == "bovino":
        codigos = client.CATEGORIAS_ABATE_BOVINO
        classifications["18"] = (
            list(codigos.values()) if categoria == "todas" else codigos[categoria]
        )

    df = await client.fetch_sidra(
        table_code=table_code,
        territorial_level=territorial_level,
        ibge_territorial_code=ibge_code,
        variable=var_codes,
        period=period,
        classifications=classifications,
    )
    registrar_canal(meta, df)

    rename_map = _detect_abate_columns(df)
    df = df.rename(columns=rename_map)

    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"].replace("-", "0"), errors="coerce")

    if "trimestre_cod" in df.columns:
        df["trimestre"] = df["trimestre_cod"].astype(str)

    df = _com_categoria(df, categoria)
    df = _merge_cabecas_peso(df, especie_lower)

    meta.fetch_duration_ms = int((time.perf_counter() - fetch_start) * 1000)
    meta.records_count = len(df)
    meta.columns = df.columns.tolist()
    meta.cache_key = build_cache_key(
        "ibge:abate",
        {
            "especie": especie,
            "trimestre": trimestre,
            "territorial_level": territorial_level,
            "ibge_code": ibge_code,
            "categoria": categoria,
        },
        schema_version=meta.schema_version,
    )

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
